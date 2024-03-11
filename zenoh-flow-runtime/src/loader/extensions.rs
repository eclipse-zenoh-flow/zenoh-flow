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

use super::{validate_library, NodeSymbol};

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use libloading::Library;
use serde::{Deserialize, Deserializer};
use zenoh_flow_commons::Result;
use zenoh_flow_nodes::{OperatorFn, SinkFn, SourceFn};

// Convenient shortcut.
#[derive(Default, Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Extensions(
    #[serde(deserialize_with = "deserialize_extensions")] HashMap<Arc<str>, Extension>,
);

impl Deref for Extensions {
    type Target = HashMap<Arc<str>, Extension>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Extensions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, Deserialize, Hash, PartialEq, Eq)]
pub struct Extension {
    pub(crate) file_extension: Arc<str>,
    pub(crate) libraries: ExtensionLibraries,
}

#[derive(Debug, Clone, Deserialize, Hash, PartialEq, Eq)]
pub(crate) struct ExtensionLibraries {
    pub(crate) source: PathBuf,
    pub(crate) sink: PathBuf,
    pub(crate) operator: PathBuf,
}

impl ExtensionLibraries {
    /// Validates that all the libraries expose the correct symbols and were compiled with the same Rust and Zenoh-Flow
    /// versions.
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol,
    /// - was not compiled with the same Rust version,
    /// - was not using the same Zenoh-Flow version as this Zenoh-Flow [runtime](crate::Runtime).
    pub(crate) fn validate(&self) -> Result<()> {
        unsafe {
            validate_library::<SourceFn>(&Library::new(&self.source)?, &NodeSymbol::Source)
                .with_context(|| format!("{}", self.source.display()))?;
            validate_library::<OperatorFn>(&Library::new(&self.operator)?, &NodeSymbol::Operator)
                .with_context(|| format!("{}", self.operator.display()))?;
            validate_library::<SinkFn>(&Library::new(&self.sink)?, &NodeSymbol::Sink)
                .with_context(|| format!("{}", self.sink.display()))?;
        }

        Ok(())
    }
}

impl Extensions {
    /// Returns the [PathBuf] of the library to load for the provided [NodeSymbol].
    ///
    /// This function is used in a generic context where we don't actually know which type of node we are manipulating.
    pub(crate) fn get_library_path(
        &self,
        file_extension: &str,
        symbol: &NodeSymbol,
    ) -> Option<&PathBuf> {
        self.get(file_extension).map(|extension| match symbol {
            NodeSymbol::Source => &extension.libraries.source,
            NodeSymbol::Operator => &extension.libraries.operator,
            NodeSymbol::Sink => &extension.libraries.sink,
        })
    }

    /// Attempts to add an extension to this Zenoh-Flow [runtime](crate::Runtime).
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol (see these macros: [1], [2], [3]),
    /// - was not compiled with the same Rust version,
    /// - was not using the same Zenoh-Flow version as this Zenoh-Flow [runtime](crate::Runtime).
    ///
    /// [1]: zenoh_flow_nodes::prelude::export_source
    /// [2]: zenoh_flow_nodes::prelude::export_operator
    /// [3]: zenoh_flow_nodes::prelude::export_sink
    pub fn try_add_extension(
        mut self,
        file_extension: impl AsRef<str>,
        source: PathBuf,
        operator: PathBuf,
        sink: PathBuf,
    ) -> Result<Self> {
        let file_ext: Arc<str> = file_extension.as_ref().into();
        let libraries = ExtensionLibraries {
            source,
            sink,
            operator,
        };

        libraries.validate()?;

        self.insert(
            file_ext.clone(),
            Extension {
                file_extension: file_ext,
                libraries,
            },
        );

        Ok(self)
    }
}

/// Attempts to deserialise a set of [Extension] from the provided string.
///
/// # Errors
///
/// This function will return an error if:
/// - the string cannot be deserialised into a vector of [Extension],
/// - any [Extension] does not provide valid libraries.
pub fn deserialize_extensions<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<Arc<str>, Extension>, D::Error>
where
    D: Deserializer<'de>,
{
    let extensions: Vec<Extension> = serde::de::Deserialize::deserialize(deserializer)?;
    let extensions_map = extensions
        .into_iter()
        .map(|extension| (extension.file_extension.clone(), extension))
        .collect::<HashMap<_, _>>();

    #[cfg(not(feature = "test-utils"))]
    {
        for extension in extensions_map.values() {
            extension.libraries.validate().map_err(|e| {
                serde::de::Error::custom(format!(
                    "Failed to validate the libraries for extension < {} >: {:?}",
                    extension.file_extension, e
                ))
            })?;
        }
    }

    Ok(extensions_map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let extensions_yaml = r#"
- file_extension: py
  libraries:
    source: /home/zenoh-flow/extension/libpython_source.so
    operator: /home/zenoh-flow/extension/libpython_operator.so
    sink: /home/zenoh-flow/extension/libpython_sink.so

- file_extension: js
  libraries:
    source: /home/zenoh-flow/extension/libwasm_source.so
    operator: /home/zenoh-flow/extension/libwasm_operator.so
    sink: /home/zenoh-flow/extension/libwasm_sink.so
"#;

        serde_yaml::from_str::<Extensions>(extensions_yaml)
            .expect("Failed to deserialize Extensions from YAML");
    }
}
