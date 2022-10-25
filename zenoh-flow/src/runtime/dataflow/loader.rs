//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use super::node::{OperatorFactory, SinkFactory, SourceFactory};
use crate::model::record::{OperatorRecord, SinkRecord, SourceRecord};
use crate::types::Configuration;
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

use std::path::{Path, PathBuf};
use url::Url;

#[cfg(target_family = "unix")]
static LOAD_FLAGS: std::os::raw::c_int =
    libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL;

/// Constant used to check if a node is compatible with the currently
/// running Zenoh Flow daemon.
/// As nodes are dynamically loaded, this is to prevent (possibly cryptic)
///  runtime error due to incompatible API.
pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Constant used to check if a node was compiled with the same version of
/// the Rust compiler than the currently running Zenoh Flow daemon.
/// As Rust is not ABI stable,
/// this is to prevent (possibly cryptic) runtime errors.
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub static EXT_FILE_EXTENSION: &str = "zfext";

/// FactorySymbol groups the symbol we must find in the shared library we load.
pub enum FactorySymbol {
    Source,
    Operator,
    Sink,
}

impl FactorySymbol {
    /// Returns the bytes representation of the symbol.
    ///
    /// They are of the form:
    ///
    /// `b"zf<node_kind>_factory_declaration\0"`
    ///
    /// Where `<node_kind>` is either `operator`, `source`, or `sink`.
    pub fn to_bytes(&self) -> &[u8] {
        match self {
            FactorySymbol::Source => b"zfsource_factory_declaration\0",
            FactorySymbol::Operator => b"zfoperator_factory_declaration\0",
            FactorySymbol::Sink => b"zfsink_factory_declaration\0",
        }
    }
}

/// Declaration expected in the library that will be loaded.
pub struct NodeDeclaration<T: ?Sized> {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: fn() -> Result<Arc<T>>,
}

/// Extensible support for different implementations
/// This represents the configuration for an extension.
///
///
/// Example:
///
/// ```yaml
/// name: python
/// file_extension: py
/// source_lib: ./target/release/libpy_source.so
/// sink_lib: ./target/release/libpy_sink.so
/// operator_lib: ./target/release/libpy_op.so
/// config_lib_key: python-script
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensibleImplementation {
    pub(crate) name: String,
    pub(crate) file_extension: String,
    pub(crate) source_lib: String,
    pub(crate) sink_lib: String,
    pub(crate) operator_lib: String,
    pub(crate) config_lib_key: String,
}

/// Loader configuration files, it includes the extensions.
///
/// Example:
///
/// ```yaml
/// extensions:
///   - name: python
///     file_extension: py
///     source_lib: ./target/release/libpy_source.so
///     sink_lib: ./target/release/libpy_sink.so
///     operator_lib: ./target/release/libpy_op.so
///     config_lib_key: python-script
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoaderConfig {
    extensions: Vec<ExtensibleImplementation>,
}

impl LoaderConfig {
    /// Creates an empty `LoaderConfig`.
    pub fn new() -> Self {
        Self { extensions: vec![] }
    }

    /// Adds the given extension.
    ///
    /// # Errors
    /// It returns an error variant if the extension is already present.
    pub fn try_add_extension(&mut self, ext: ExtensibleImplementation) -> Result<()> {
        if self.extensions.iter().any(|e| e.name == ext.name) {
            return Err(zferror!(ErrorKind::Duplicate).into());
        }
        self.extensions.push(ext);
        Ok(())
    }

    /// Removes the given extension.
    pub fn remove_extension(&mut self, name: &str) -> Option<ExtensibleImplementation> {
        if let Some(index) = self.extensions.iter().position(|e| e.name == name) {
            let ext = self.extensions.remove(index);
            return Some(ext);
        }
        None
    }

    /// Gets the extension that matches the given `file_extension`.
    pub fn get_extension_by_file_extension(
        &self,
        file_extension: &str,
    ) -> Option<&ExtensibleImplementation> {
        if let Some(ext) = self
            .extensions
            .iter()
            .find(|e| e.file_extension == file_extension)
        {
            return Some(ext);
        }
        None
    }

    /// Gets the extension that matches the given `name`.
    pub fn get_extension_by_name(&self, name: &str) -> Option<&ExtensibleImplementation> {
        if let Some(ext) = self.extensions.iter().find(|e| e.name == name) {
            return Some(ext);
        }
        None
    }
}

impl Default for LoaderConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// The dynamic library loader.
/// Before loading it verifies if the versions are compatible
/// and if the symbols are presents.
/// It loads the files in different way depending on the operating system.
/// In particular the scope of the symbols is different between Unix and
/// Windows.
/// In Unix system the symbols are loaded with the flags:
///
/// - `RTLD_NOW` load all the symbols when loading the library.
/// - `RTLD_LOCAL` keep all the symbols local.
///
///
pub struct Loader {
    pub(crate) config: LoaderConfig,
}

impl Loader {
    /// Creates a new `Loader` with the given `config`.
    pub fn new(config: LoaderConfig) -> Self {
        Self { config }
    }

    /// Tries to load a SourceFactory from the information passed within the
    /// [`SourceRecord`](`SourceRecord`).
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different versions of Zenoh-Flow used to build the source
    /// - different versions of the rust compiler used to build the source
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known).
    pub(crate) fn load_source_factory(&self, record: SourceRecord) -> Result<SourceFactory> {
        let uri = record.uri.clone().ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Source < {} >.",
                record.id.clone()
            )
        })?;

        let uri = Url::parse(&uri).map_err(|err| zferror!(ErrorKind::ParsingError, err))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    zferror!(
                        ErrorKind::LoadingError,
                        "Missing file extension for dynamically loaded Source < {} , {:?}>.",
                        record.id.clone(),
                        file_path,
                    )
                })?;

                match Self::is_lib(&file_extension) {
                    true => {
                        let (lib, factory) = unsafe { Self::load_lib_source_factory(file_path) }?;
                        Ok(SourceFactory {
                            record,
                            factory,
                            _library: Some(Arc::new(lib)),
                        })
                    }
                    _ => Ok(self.load_source_factory_from_extension(record, file_path)?),
                }
            }
            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Tries to load a OperatorFactory from the information passed within the
    /// [`OperatorRecord`](`OperatorRecord`).
    ///
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - different versions of Zenoh-Flow used to build the operator
    /// - different versions of the rust compiler used to build the operator
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known).
    pub(crate) fn load_operator_factory(&self, record: OperatorRecord) -> Result<OperatorFactory> {
        let uri = record.uri.clone().ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Operator < {} >.",
                record.id.clone()
            )
        })?;

        let uri = Url::parse(&uri).map_err(|err| zferror!(ErrorKind::ParsingError, err))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    zferror!(
                        ErrorKind::LoadingError,
                        "Missing file extension for dynamically loaded Operator < {} , {:?}>.",
                        record.id.clone(),
                        file_path,
                    )
                })?;

                match Self::is_lib(&file_extension) {
                    true => {
                        let (lib, factory) = unsafe { Self::load_lib_operator_factory(file_path) }?;
                        Ok(OperatorFactory {
                            record,
                            factory,
                            _library: Some(Arc::new(lib)),
                        })
                    }
                    _ => Ok(self.load_operator_factory_from_extension(record, file_path)?),
                }
            }
            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Tries to load a SinkFactory from the information passed within the
    /// [`SinkRecord`](`SinkRecord`).
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different versions of Zenoh-Flow used to build the sink
    /// - different versions of the rust compiler used to build the sink
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known).
    pub(crate) fn load_sink_factory(&self, record: SinkRecord) -> Result<SinkFactory> {
        let uri = record.uri.clone().ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Source < {} >.",
                record.id.clone()
            )
        })?;

        let uri = Url::parse(&uri).map_err(|err| zferror!(ErrorKind::ParsingError, err))?;

        match uri.scheme() {
            "file" => {
                let file_path = Self::make_file_path(uri)?;
                let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
                    zferror!(
                        ErrorKind::LoadingError,
                        "Missing file extension for dynamically loaded Sink < {} , {:?}>.",
                        record.id.clone(),
                        file_path,
                    )
                })?;

                match Self::is_lib(&file_extension) {
                    true => {
                        let (lib, factory) = unsafe { Self::load_lib_sink_factory(file_path) }?;
                        Ok(SinkFactory {
                            record,
                            factory,
                            _library: Some(Arc::new(lib)),
                        })
                    }
                    _ => Ok(self.load_sink_factory_from_extension(record, file_path)?),
                }
            }
            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Load the library of a Source factory.
    ///
    /// # Safety
    ///
    /// - dynamic loading of library, and lookup of symbols.
    ///
    /// # Errors
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it fails if the source factory symbol is not found.
    unsafe fn load_lib_source_factory(
        path: PathBuf,
    ) -> Result<(Library, Arc<dyn crate::traits::SourceFactoryTrait>)> {
        Self::load_lib_node::<dyn crate::traits::SourceFactoryTrait>(path, FactorySymbol::Source)
    }

    /// Load the library of an Operator factory.
    ///
    /// # Safety
    ///
    /// - dynamic loading of library, and lookup of symbols.
    ///
    /// # Errors
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it fails if the operator factory symbol is not found.
    unsafe fn load_lib_operator_factory(
        path: PathBuf,
    ) -> Result<(Library, Arc<dyn crate::traits::OperatorFactoryTrait>)> {
        Self::load_lib_node::<dyn crate::traits::OperatorFactoryTrait>(
            path,
            FactorySymbol::Operator,
        )
    }

    /// Load the library of a Sink factory.
    ///
    /// # Safety
    ///
    /// - dynamic loading of library, and lookup of symbols.
    ///
    /// # Errors
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it fails if the sink factory symbol is not found.
    unsafe fn load_lib_sink_factory(
        path: PathBuf,
    ) -> Result<(Library, Arc<dyn crate::traits::SinkFactoryTrait>)> {
        Self::load_lib_node::<dyn crate::traits::SinkFactoryTrait>(path, FactorySymbol::Sink)
    }

    /// Load the library of a node.
    ///
    /// # Safety
    ///
    /// - dynamic loading of library, and lookup of symbols.
    ///
    /// # Errors
    ///
    /// This function dynamically loads an external library, things can go wrong:
    /// - it fails if the factory symbol is not found,
    unsafe fn load_lib_node<T: ?Sized>(
        path: PathBuf,
        node_symbol: FactorySymbol,
    ) -> Result<(Library, Arc<T>)> {
        log::debug!("Loading {:#?}", path);

        #[cfg(target_family = "unix")]
        let library = Library::open(Some(path), LOAD_FLAGS)?;

        #[cfg(target_family = "windows")]
        let library = Library::new(path)?;

        let decl = library
            .get::<*mut NodeDeclaration<T>>(node_symbol.to_bytes())?
            // .get::<*mut NodeDeclaration<T>>(b"zfoperator_declaration\0")?
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(zferror!(ErrorKind::VersionMismatch).into());
        }

        Ok((library, (decl.register)()?))
    }

    /// Converts the `Url` to a `PathBuf`
    fn make_file_path(uri: Url) -> Result<PathBuf> {
        let mut path = PathBuf::new();
        let file_path = match uri.host_str() {
            Some(h) => format!("{}{}", h, uri.path()),
            None => uri.path().to_string(),
        };
        path.push(file_path);
        let path = std::fs::canonicalize(&path)
            .map_err(|e| zferror!(ErrorKind::IOError, "{}: {}", e, &path.to_string_lossy()))?;
        Ok(path)
    }

    /// Checks if the file is a dynamic library.
    fn is_lib(ext: &str) -> bool {
        if ext == std::env::consts::DLL_EXTENSION {
            return true;
        }
        false
    }

    /// Returns the file extension, if any.
    fn get_file_extension(file: &Path) -> Option<String> {
        if let Some(ext) = file.extension() {
            if let Some(ext) = ext.to_str() {
                return Some(String::from(ext));
            }
        }
        None
    }

    /// Loads a source that is not a dynamic library using one of the extension configured within
    /// the loader.
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - the extension is not known
    /// - different versions of Zenoh-Flow used to build the source
    /// - different versions of the rust compiler used to build the source
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known)
    /// - the source does not match the extension interface.
    fn load_source_factory_from_extension(
        &self,
        mut record: SourceRecord,
        file_path: PathBuf,
    ) -> Result<SourceFactory> {
        let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing file extension for dynamically loaded Source < {} , {:?}>.",
                record.id.clone(),
                file_path,
            )
        })?;

        match self.config.get_extension_by_file_extension(&file_extension) {
            Some(e) => {
                let wrapper_file_path = std::fs::canonicalize(&e.source_lib)?;
                record.configuration = Some(Self::generate_wrapper_config(
                    record.configuration,
                    e.config_lib_key.clone(),
                    &file_path,
                )?);

                let (lib, factory) = unsafe { Self::load_lib_source_factory(wrapper_file_path) }?;
                Ok(SourceFactory {
                    record,
                    factory,
                    _library: Some(Arc::new(lib)),
                })
            }
            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Loads a operator that is not a dynamic library, using one of the extension configured within
    /// the loader.
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - the extension is not known
    /// - different versions of Zenoh-Flow used to build the operator
    /// - different versions of the rust compiler used to build the operator
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known)
    /// - the operator does not match the extension interface.
    fn load_operator_factory_from_extension(
        &self,
        mut record: OperatorRecord,
        file_path: PathBuf,
    ) -> Result<OperatorFactory> {
        let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing file extension for dynamically loaded Operator < {} , {:?}>.",
                record.id.clone(),
                file_path,
            )
        })?;

        match self.config.get_extension_by_file_extension(&file_extension) {
            Some(e) => {
                let wrapper_file_path = std::fs::canonicalize(&e.operator_lib)?;
                record.configuration = Some(Self::generate_wrapper_config(
                    record.configuration,
                    e.config_lib_key.clone(),
                    &file_path,
                )?);

                let (lib, factory) = unsafe { Self::load_lib_operator_factory(wrapper_file_path) }?;
                Ok(OperatorFactory {
                    record,
                    factory,
                    _library: Some(Arc::new(lib)),
                })
            }
            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Loads a sink that is not a dynamic library using one of the extension configured within the
    /// loader.
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - the extension is not known
    /// - different versions of Zenoh-Flow used to build the sink
    /// - different versions of the rust compiler used to build the sink
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is known)
    /// - the sink does not match the extension interface.
    fn load_sink_factory_from_extension(
        &self,
        mut record: SinkRecord,
        file_path: PathBuf,
    ) -> Result<SinkFactory> {
        let file_extension = Self::get_file_extension(&file_path).ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing file extension for dynamically loaded Sink < {} , {:?}>.",
                record.id.clone(),
                file_path,
            )
        })?;

        match self.config.get_extension_by_file_extension(&file_extension) {
            Some(e) => {
                let wrapper_file_path = std::fs::canonicalize(&e.sink_lib)?;
                record.configuration = Some(Self::generate_wrapper_config(
                    record.configuration,
                    e.config_lib_key.clone(),
                    &file_path,
                )?);

                let (lib, factory) = unsafe { Self::load_lib_sink_factory(wrapper_file_path) }?;
                Ok(SinkFactory {
                    record,
                    factory,
                    _library: Some(Arc::new(lib)),
                })
            }
            _ => Err(zferror!(ErrorKind::Unimplemented).into()),
        }
    }

    /// Wraps the configuration in case of an extension.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  unable to parse the file path
    fn generate_wrapper_config(
        configuration: Option<Configuration>,
        config_key: String,
        file_path: &Path,
    ) -> Result<Configuration> {
        let mut new_config: serde_json::map::Map<String, Configuration> =
            serde_json::map::Map::new();
        new_config.insert(
            config_key,
            file_path
                .to_str()
                .ok_or_else(|| {
                    zferror!(
                        ErrorKind::LoadingError,
                        "Unable parse file path < {:?} >.",
                        file_path,
                    )
                })?
                .into(),
        );

        if let Some(config) = configuration {
            new_config.insert(String::from("configuration"), config);
        }
        Ok(new_config.into())
    }
}
