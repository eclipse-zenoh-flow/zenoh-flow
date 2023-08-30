//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

use super::instance::builtin::zenoh::{get_zenoh_sink_declaration, get_zenoh_source_declaration};
use super::node::{
    ConstructorFn, OperatorConstructor, OperatorFn, SinkConstructor, SinkFn, SourceConstructor,
    SourceFn,
};
use crate::model::record::{OperatorRecord, SinkRecord, SourceRecord};
use crate::model::{Middleware, ZFUri};
use crate::types::Configuration;
use crate::utils::parse_uri;
use crate::zfresult::ErrorKind;
use crate::Result;
use crate::{bail, zferror};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

#[cfg(target_family = "unix")]
static LOAD_FLAGS: std::os::raw::c_int =
    libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL;

/// Constant used to check if a node is compatible with the currently running Zenoh Flow daemon.
/// As nodes are dynamically loaded, this is to prevent (possibly cryptic) runtime error due to
/// incompatible API.
pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Constant used to check if a node was compiled with the same version of the Rust compiler than
/// the currently running Zenoh Flow daemon.
/// As Rust is not ABI stable, this is to prevent (possibly cryptic) runtime errors.
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub static EXT_FILE_EXTENSION: &str = "zfext";

/// NodeSymbol groups the symbol we must find in the shared library we load.
pub(crate) enum NodeSymbol {
    Source,
    Operator,
    Sink,
}

impl NodeSymbol {
    /// Returns the bytes representation of the symbol.
    ///
    /// They are of the form:
    ///
    /// `b"_zf_export_<node_kind>\0"`
    ///
    /// Where `<node_kind>` is either `operator`, `source`, or `sink`.
    pub(crate) fn to_bytes(&self) -> &[u8] {
        match self {
            NodeSymbol::Source => b"_zf_export_source\0",
            NodeSymbol::Operator => b"_zf_export_operator\0",
            NodeSymbol::Sink => b"_zf_export_sink\0",
        }
    }
}

/// Declaration expected in the library that will be loaded.
pub struct NodeDeclaration<C> {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub constructor: C,
}

pub type SourceDeclaration = NodeDeclaration<SourceFn>;
pub type OperatorDeclaration = NodeDeclaration<OperatorFn>;
pub type SinkDeclaration = NodeDeclaration<SinkFn>;

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
pub struct Loader {
    pub(crate) config: LoaderConfig,
}

impl Loader {
    /// Creates a new `Loader` with the given `config`.
    pub fn new(config: LoaderConfig) -> Self {
        Self { config }
    }

    /// Loads a node library from a file, using one of the extension configured within the loader.
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different version of Zenoh-Flow used to build the node
    /// - different version of the rust compiler used to build the node
    /// - the library does not contain the symbols
    /// - the extension is not known
    /// - the node does not match the extension interface
    unsafe fn load_node_from_file<T: ConstructorFn>(
        &self,
        node_symbol: NodeSymbol,
        file_path: PathBuf,
        configuration: &mut Option<Configuration>,
    ) -> Result<(Library, T)> {
        let file_extension = crate::utils::get_file_extension(&file_path).ok_or_else(|| {
            zferror!(
                ErrorKind::LoadingError,
                "Missing file extension for < {:?} >",
                file_path,
            )
        })?;

        let library_path = if crate::utils::is_dynamic_library(&file_extension) {
            file_path
        } else {
            match self.config.get_extension_by_file_extension(&file_extension) {
                Some(e) => {
                    Self::wrap_configuration(configuration, e.config_lib_key.clone(), &file_path)?;
                    let lib = match node_symbol {
                        NodeSymbol::Source => &e.source_lib,
                        NodeSymbol::Operator => &e.operator_lib,
                        NodeSymbol::Sink => &e.sink_lib,
                    };
                    std::fs::canonicalize(lib)?
                }
                _ => bail!(ErrorKind::Unimplemented),
            }
        };

        log::trace!("[Loader] loading library {:?}", library_path);

        #[cfg(target_family = "unix")]
        let library = Library::open(Some(library_path.clone()), LOAD_FLAGS)?;

        #[cfg(target_family = "windows")]
        let library = Library::new(library_path)?;

        let decl = library
            .get::<*mut NodeDeclaration<T>>(node_symbol.to_bytes())?
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(zferror!(
                ErrorKind::VersionMismatch,
                "Library {} rustc expected {} rustc found {} - Zenoh-Flow expected {} Zenoh-Flow found {}",
                library_path.display(),
                RUSTC_VERSION,
                decl.rustc_version,
                CORE_VERSION,
                decl.core_version
            )
            .into());
        }

        Ok((library, decl.constructor))
    }

    /// Loads a source from the builtin ones.
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - the buitin middleware is not supported (so far only Zenoh is supported)
    fn load_source_from_builtin(&self, middleware: Middleware) -> Result<SourceFn> {
        match middleware {
            Middleware::Zenoh => {
                let declaration = get_zenoh_source_declaration();
                Ok(declaration.constructor)
            }
        }
    }

    /// Loads a sink from the builtin ones
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - the buitin middleware is not supported (so far only Zenoh is supported)
    fn load_sink_from_builtin(&self, middleware: Middleware) -> Result<SinkFn> {
        match middleware {
            Middleware::Zenoh => {
                let declaration = get_zenoh_sink_declaration();
                Ok(declaration.constructor)
            }
        }
    }

    /// Tries to load a Source from the information passed within the
    /// [`SourceRecord`](`SourceRecord`).
    ///
    /// # Errors
    ///
    /// It can fail because of:
    /// - different version of Zenoh-Flow used to build the source
    /// - different version of the rust compiler used to build the source
    /// - the library does not contain the symbols
    /// - the URI is missing
    /// - the URI scheme is not known (so far only `file://` is supported).
    pub(crate) fn load_source_constructor(
        &self,
        mut record: SourceRecord,
    ) -> Result<SourceConstructor> {
        if let Some(uri) = &record.uri {
            match parse_uri(uri)? {
                ZFUri::File(file_path) => {
                    let (library, constructor) = unsafe {
                        self.load_node_from_file::<SourceFn>(
                            NodeSymbol::Source,
                            file_path,
                            &mut record.configuration,
                        )?
                    };

                    Ok(SourceConstructor::new_dynamic(
                        record,
                        constructor,
                        Arc::new(library),
                    ))
                }
                ZFUri::Builtin(mw) => {
                    let constructor = self.load_source_from_builtin(mw)?;
                    Ok(SourceConstructor::new_static(record, constructor))
                }
            }
        } else {
            bail!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Source < {} >.",
                record.id.clone()
            )
        }
    }

    /// Tries to load an Operator from the information passed within the
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
    pub(crate) fn load_operator_constructor(
        &self,
        mut record: OperatorRecord,
    ) -> Result<OperatorConstructor> {
        if let Some(uri) = &record.uri {
            match parse_uri(uri)? {
                ZFUri::File(file_path) => {
                    let (library, constructor) = unsafe {
                        self.load_node_from_file::<OperatorFn>(
                            NodeSymbol::Operator,
                            file_path,
                            &mut record.configuration,
                        )?
                    };

                    Ok(OperatorConstructor::new_dynamic(
                        record,
                        constructor,
                        Arc::new(library),
                    ))
                }
                ZFUri::Builtin(_mw) => {
                    bail!(
                        ErrorKind::Unimplemented,
                        "Loading builtin operators is not supported < {} >.",
                        record.id.clone()
                    )
                }
            }
        } else {
            bail!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Operator < {} >.",
                record.id.clone()
            )
        }
    }

    /// Tries to load a Sink from the information passed within the
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
    pub(crate) fn load_sink_constructor(&self, mut record: SinkRecord) -> Result<SinkConstructor> {
        if let Some(uri) = &record.uri {
            match parse_uri(uri)? {
                ZFUri::File(file_path) => {
                    let (library, constructor) = unsafe {
                        self.load_node_from_file::<SinkFn>(
                            NodeSymbol::Sink,
                            file_path,
                            &mut record.configuration,
                        )?
                    };

                    Ok(SinkConstructor::new_dynamic(
                        record,
                        constructor,
                        Arc::new(library),
                    ))
                }
                ZFUri::Builtin(mw) => {
                    let constructor = self.load_sink_from_builtin(mw)?;
                    Ok(SinkConstructor::new_static(record, constructor))
                }
            }
        } else {
            bail!(
                ErrorKind::LoadingError,
                "Missing URI for dynamically loaded Sink < {} >.",
                record.id.clone()
            )
        }
    }

    /// Wraps the configuration in case of an extension.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - unable to parse the file path
    fn wrap_configuration(
        configuration: &mut Option<Configuration>,
        config_key: String,
        file_path: &Path,
    ) -> Result<()> {
        let mut new_config: serde_json::map::Map<String, Configuration> =
            serde_json::map::Map::new();
        let config = configuration.take();
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

        if let Some(config) = config {
            new_config.insert(String::from("configuration"), config);
        }

        *configuration = Some(new_config.into());
        Ok(())
    }
}
