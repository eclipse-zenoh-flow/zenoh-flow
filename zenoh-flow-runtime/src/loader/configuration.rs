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

use super::NodeSymbol;
use anyhow::bail;
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, ops::Deref, path::PathBuf, sync::Arc};
use zenoh_flow_commons::Result;

// Convenient shortcut.
type Extensions = HashMap<Arc<str>, ExtensibleImplementation>;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct ExtensibleImplementation {
    pub(crate) name: String,
    pub(crate) file_extension: String,
    pub(crate) source_lib: PathBuf,
    pub(crate) sink_lib: PathBuf,
    pub(crate) operator_lib: PathBuf,
    pub(crate) config_lib_key: String,
}

fn deserialize_extensions<'de, D>(deserializer: D) -> std::result::Result<Extensions, D::Error>
where
    D: Deserializer<'de>,
{
    let extensions: Vec<ExtensibleImplementation> =
        serde::de::Deserialize::deserialize(deserializer)?;

    Ok(extensions
        .into_iter()
        .map(|extension| (extension.file_extension.clone().into(), extension))
        .collect::<Extensions>())
}

/// Loader configuration files, it includes the extensions.
///
/// # Example
///
/// ```
/// use zenoh_flow_runtime::LoaderConfig;
///
/// let yaml_extensions = r#"
/// extensions:
///   - name: python
///     file_extension: py
///     source_lib: ./target/release/libpy_source.so
///     sink_lib: ./target/release/libpy_sink.so
///     operator_lib: ./target/release/libpy_op.so
///     config_lib_key: python-script
/// "#;
///
/// assert!(serde_yaml::from_str::<LoaderConfig>(yaml_extensions).is_ok());
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LoaderConfig {
    #[serde(deserialize_with = "deserialize_extensions")]
    extensions: Extensions,
}

impl Deref for LoaderConfig {
    type Target = Extensions;

    fn deref(&self) -> &Self::Target {
        &self.extensions
    }
}

impl LoaderConfig {
    /// Creates an empty `LoaderConfig`.
    pub fn new() -> Self {
        Self {
            extensions: HashMap::default(),
        }
    }

    /// Adds the given extension.
    ///
    /// # Errors
    ///
    /// It returns an error variant if the extension is already present.
    pub fn try_add_extension(&mut self, ext: ExtensibleImplementation) -> Result<()> {
        if self.contains_key(ext.file_extension.as_str()) {
            bail!(
                "Extension < {} > already has an associated configuration",
                ext.file_extension
            )
        }

        self.extensions
            .insert(ext.file_extension.clone().into(), ext);

        Ok(())
    }

    /// TODO@J-Loudet
    pub(crate) fn get_library_path(
        &self,
        file_extension: &str,
        symbol: &NodeSymbol,
    ) -> Option<&PathBuf> {
        self.get(file_extension).map(|extension| match symbol {
            NodeSymbol::Source => &extension.source_lib,
            NodeSymbol::Operator => &extension.operator_lib,
            NodeSymbol::Sink => &extension.sink_lib,
        })
    }
}
