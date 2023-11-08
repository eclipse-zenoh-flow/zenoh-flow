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

use std::path::PathBuf;

use anyhow::bail;
use serde::{Deserialize, Serialize};
use zenoh_flow_commons::Result;

use super::NodeSymbol;

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
    pub(crate) source_lib: PathBuf,
    pub(crate) sink_lib: PathBuf,
    pub(crate) operator_lib: PathBuf,
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
    // NOTE This has to be a vector as we are reading it from a file.
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
        if self
            .extensions
            .iter()
            .any(|e| e.file_extension == ext.file_extension)
        {
            bail!(
                "Extension < {} > already has an associated configuration",
                ext.file_extension
            )
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
        self.extensions
            .iter()
            .find(|e| e.file_extension == file_extension)
    }

    pub(crate) fn get_library_path(
        &self,
        file_extension: &str,
        symbol: &NodeSymbol,
    ) -> Option<&PathBuf> {
        self.get_extension_by_file_extension(file_extension)
            .map(|extension| match symbol {
                NodeSymbol::Source => &extension.source_lib,
                NodeSymbol::Operator => &extension.operator_lib,
                NodeSymbol::Sink => &extension.sink_lib,
            })
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
