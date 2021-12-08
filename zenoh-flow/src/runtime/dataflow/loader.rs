//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use super::node::{OperatorLoaded, SinkLoaded, SourceLoaded};
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::serde::{Deserialize, Serialize};
use crate::{Configuration, Operator, Sink, Source, ZFError, ZFResult};
use async_std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

use std::path::{Path, PathBuf};
use url::Url;

#[cfg(target_family = "unix")]
static LOAD_FLAGS: std::os::raw::c_int =
    libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

// OPERATOR

pub type OperatorRegisterFn = fn() -> ZFResult<Arc<dyn Operator>>;

pub struct OperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: OperatorRegisterFn,
}

// SOURCE

pub type SourceRegisterFn = fn() -> ZFResult<Arc<dyn Source>>;

pub struct SourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: SourceRegisterFn,
}

// SINK

pub type SinkRegisterFn = fn() -> ZFResult<Arc<dyn Sink>>;

pub struct SinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: SinkRegisterFn,
}

// Extensible support for different implementations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensibleImplementation {
    pub(crate) name: String,
    pub(crate) file_extension: String,
    pub(crate) source_lib: String,
    pub(crate) sink_lib: String,
    pub(crate) operator_lib: String,
    pub(crate) config_lib_key: String,
}

// Loader Config
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoaderConfig {
    pub extensions: Vec<ExtensibleImplementation>,
}

pub struct Loader {
    pub(crate) config: LoaderConfig,
}

impl Loader {
    pub fn new(config: LoaderConfig) -> Self {
        Self { config }
    }

    /// # Safety
    ///
    /// TODO remove all copy-pasted code, make macros/functions instead
    pub fn load_operator(&self, record: OperatorRecord) -> ZFResult<OperatorLoaded> {
        let uri = record.uri.clone().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Operator < {} >.",
                record.id.clone()
            ))
        })?;

        let uri = Url::parse(&uri).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    ZFError::LoadingError(format!(
                        "Missing file extension for dynamically loaded Operator < {} , {:?}>.",
                        record.id.clone(),
                        file_path,
                    ))
                })?;

                match Self::is_lib(&file_extension) {
                    true => {
                        let (lib, op) = unsafe { Self::load_lib_operator(file_path) }?;
                        Ok(OperatorLoaded::try_new(record, Some(Arc::new(lib)), op)?)
                    }
                    _ => Ok(self.load_operator_from_extension(record, file_path)?),
                }
            }
            _ => Err(ZFError::Unimplemented),
        }
    }

    pub fn load_source(&self, record: SourceRecord) -> ZFResult<SourceLoaded> {
        let uri = record.uri.clone().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Source < {} >.",
                record.id.clone()
            ))
        })?;

        let uri = Url::parse(&uri).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    ZFError::LoadingError(format!(
                        "Missing file extension for dynamically loaded Source < {} , {:?}>.",
                        record.id.clone(),
                        file_path,
                    ))
                })?;

                match Self::is_lib(&file_extension) {
                    true => {
                        let (lib, op) = unsafe { Self::load_lib_source(file_path) }?;
                        Ok(SourceLoaded::try_new(record, Some(Arc::new(lib)), op)?)
                    }
                    _ => Ok(self.load_source_from_extension(record, file_path)?),
                }
            }
            _ => Err(ZFError::Unimplemented),
        }
    }

    pub fn load_sink(&self, record: SinkRecord) -> ZFResult<SinkLoaded> {
        let uri = record.uri.clone().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Sink < {} >.",
                record.id.clone()
            ))
        })?;

        let uri = Url::parse(&uri).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    ZFError::LoadingError(format!(
                        "Missing file extension for dynamically loaded Sink < {} , {:?}>.",
                        record.id.clone(),
                        file_path,
                    ))
                })?;

                match Self::is_lib(&file_extension) {
                    true => {
                        let (lib, op) = unsafe { Self::load_lib_sink(file_path) }?;
                        Ok(SinkLoaded::try_new(record, Some(Arc::new(lib)), op)?)
                    }
                    _ => Ok(self.load_sink_from_extension(record, file_path)?),
                }
            }
            _ => Err(ZFError::Unimplemented),
        }
    }

    /// Load the library of the operator.
    ///
    /// # Safety
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it will panic if the symbol `zfoperator_declaration` is not found,
    /// - be sure to *trust* the code you are loading.
    unsafe fn load_lib_operator(path: PathBuf) -> ZFResult<(Library, Arc<dyn Operator>)> {
        log::debug!("Operator Loading {:#?}", path);

        #[cfg(target_family = "unix")]
        let library = Library::open(Some(path), LOAD_FLAGS)?;

        #[cfg(target_family = "windows")]
        let library = Library::new(path)?;

        let decl = library
            .get::<*mut OperatorDeclaration>(b"zfoperator_declaration\0")?
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        Ok((library, (decl.register)()?))
    }

    /// Load the library of a source.
    ///
    /// # Safety
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it will panic if the symbol `zfsource_declaration` is not found,
    /// - be sure to *trust* the code you are loading.
    unsafe fn load_lib_source(path: PathBuf) -> ZFResult<(Library, Arc<dyn Source>)> {
        log::debug!("Source Loading {:#?}", path);

        #[cfg(target_family = "unix")]
        let library = Library::open(Some(path), LOAD_FLAGS)?;

        #[cfg(target_family = "windows")]
        let library = Library::new(path)?;

        let decl = library
            .get::<*mut SourceDeclaration>(b"zfsource_declaration\0")?
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        Ok((library, (decl.register)()?))
    }

    /// Load the library of a sink.
    ///
    /// # Safety
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it will panic if the symbol `zfsink_declaration` is not found,
    /// - be sure to *trust* the code you are loading.
    unsafe fn load_lib_sink(path: PathBuf) -> ZFResult<(Library, Arc<dyn Sink>)> {
        log::debug!("Sink Loading {:#?}", path);

        #[cfg(target_family = "unix")]
        let library = Library::open(Some(path), LOAD_FLAGS)?;

        #[cfg(target_family = "windows")]
        let library = Library::new(path)?;

        let decl = library
            .get::<*mut SinkDeclaration>(b"zfsink_declaration\0")?
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        Ok((library, (decl.register)()?))
    }
    fn make_file_path(uri: Url) -> ZFResult<PathBuf> {
        let mut path = PathBuf::new();
        let file_path = match uri.host_str() {
            Some(h) => format!("{}{}", h, uri.path()),
            None => uri.path().to_string(),
        };
        path.push(file_path);
        let path = std::fs::canonicalize(path)?;
        Ok(path)
    }

    fn is_lib(ext: &str) -> bool {
        if ext == std::env::consts::DLL_EXTENSION {
            return true;
        }
        false
    }

    fn get_file_extension(file: &Path) -> Option<String> {
        if let Some(ext) = file.extension() {
            if let Some(ext) = ext.to_str() {
                return Some(String::from(ext));
            }
        }
        None
    }

    fn load_operator_from_extension(
        &self,
        mut record: OperatorRecord,
        file_path: PathBuf,
    ) -> ZFResult<OperatorLoaded> {
        let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing file extension for dynamically loaded Operator < {} , {:?}>.",
                record.id.clone(),
                file_path,
            ))
        })?;

        match self
            .config
            .extensions
            .iter()
            .find(|e| e.file_extension == file_extension)
        {
            Some(e) => {
                let wrapper_file_path = std::fs::canonicalize(&e.operator_lib)?;
                record.configuration = Some(Self::generate_wrapper_config(
                    record.configuration,
                    e.config_lib_key.clone(),
                    &file_path,
                )?);

                let (lib, op) = unsafe { Self::load_lib_operator(wrapper_file_path) }?;
                Ok(OperatorLoaded::try_new(record, Some(Arc::new(lib)), op)?)
            }
            _ => Err(ZFError::Unimplemented),
        }
    }

    fn load_source_from_extension(
        &self,
        mut record: SourceRecord,
        file_path: PathBuf,
    ) -> ZFResult<SourceLoaded> {
        let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing file extension for dynamically loaded Operator < {} , {:?}>.",
                record.id.clone(),
                file_path,
            ))
        })?;

        match self
            .config
            .extensions
            .iter()
            .find(|e| e.file_extension == file_extension)
        {
            Some(e) => {
                let wrapper_file_path = std::fs::canonicalize(&e.source_lib)?;
                record.configuration = Some(Self::generate_wrapper_config(
                    record.configuration,
                    e.config_lib_key.clone(),
                    &file_path,
                )?);

                let (lib, op) = unsafe { Self::load_lib_source(wrapper_file_path) }?;
                Ok(SourceLoaded::try_new(record, Some(Arc::new(lib)), op)?)
            }
            _ => Err(ZFError::Unimplemented),
        }
    }

    fn load_sink_from_extension(
        &self,
        mut record: SinkRecord,
        file_path: PathBuf,
    ) -> ZFResult<SinkLoaded> {
        let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing file extension for dynamically loaded Operator < {} , {:?}>.",
                record.id.clone(),
                file_path,
            ))
        })?;

        match self
            .config
            .extensions
            .iter()
            .find(|e| e.file_extension == file_extension)
        {
            Some(e) => {
                let wrapper_file_path = std::fs::canonicalize(&e.sink_lib)?;
                record.configuration = Some(Self::generate_wrapper_config(
                    record.configuration,
                    e.config_lib_key.clone(),
                    &file_path,
                )?);

                let (lib, op) = unsafe { Self::load_lib_sink(wrapper_file_path) }?;
                Ok(SinkLoaded::try_new(record, Some(Arc::new(lib)), op)?)
            }
            _ => Err(ZFError::Unimplemented),
        }
    }

    fn generate_wrapper_config(
        configuration: Option<Configuration>,
        config_key: String,
        file_path: &Path,
    ) -> ZFResult<Configuration> {
        let mut new_config: serde_json::map::Map<String, Configuration> =
            serde_json::map::Map::new();
        new_config.insert(
            config_key,
            file_path
                .to_str()
                .ok_or_else(|| {
                    ZFError::LoadingError(format!("Unable parse file path < {:?} >.", file_path,))
                })?
                .into(),
        );

        if let Some(config) = configuration {
            new_config.insert(String::from("configuration"), config);
        }
        Ok(new_config.into())
    }
}
