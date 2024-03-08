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

/// Additionally supported file extensions for node implementation.
///
/// Zenoh-Flow only supports node implementation in the form of [shared libraries]. To support additional implementation
/// --- for instance [Python scripts] --- a Zenoh-Flow runtime needs to be informed on (i) which shared libraries it
/// should load and (ii) how it should make these shared libraries "load" the node implementation.
///
/// This structure associates file extension(s) to these information.
///
/// [shared libraries]: std::env::consts::DLL_EXTENSION
/// [Python scripts]: https://github.com/eclipse-zenoh/zenoh-flow-python
#[derive(Default, Debug, Clone, Deserialize, PartialEq, Eq)]
#[repr(transparent)]
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

impl From<Extensions> for HashMap<Arc<str>, Extension> {
    fn from(value: Extensions) -> Self {
        value.0
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
    /// Note that if a previous entry was added for the same file extension, the previous entry will be returned.
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol (see these macros: [1], [2], [3]),
    /// - was not compiled with the same Rust version,
    /// - was not using the same version of Zenoh-Flow as this [runtime](crate::Runtime).
    ///
    /// [1]: zenoh_flow_nodes::prelude::export_source
    /// [2]: zenoh_flow_nodes::prelude::export_operator
    /// [3]: zenoh_flow_nodes::prelude::export_sink
    pub(crate) fn try_add_extension(
        &mut self,
        file_extension: impl Into<String>,
        source: impl Into<PathBuf>,
        operator: impl Into<PathBuf>,
        sink: impl Into<PathBuf>,
    ) -> Result<Option<Extension>> {
        let file_ext: Arc<str> = file_extension.into().into();
        let libraries = ExtensionLibraries::new(source.into(), sink.into(), operator.into())?;

        Ok(self.insert(
            file_ext.clone(),
            Extension {
                file_extension: file_ext,
                libraries,
            },
        ))
    }
}

// NOTE: We separate the libraries in its own dedicated structure to have that same textual representation (YAML/JSON).
//       There is no real need to do so.
#[derive(Debug, Clone, Deserialize, Hash, PartialEq, Eq)]
pub struct Extension {
    pub(crate) file_extension: Arc<str>,
    pub(crate) libraries: ExtensionLibraries,
}

impl Extension {
    /// Returns the file extension associated with this extension.
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    /// Returns the [path](PathBuf) of the shared library responsible for loading Source nodes for this file extension.
    pub fn source(&self) -> &PathBuf {
        &self.libraries.source
    }

    /// Returns the [path](PathBuf) of the shared library responsible for loading Operator nodes for this file
    /// extension.
    pub fn operator(&self) -> &PathBuf {
        &self.libraries.operator
    }

    /// Returns the [path](PathBuf) of the shared library responsible for loading Sink nodes for this file extension.
    pub fn sink(&self) -> &PathBuf {
        &self.libraries.sink
    }
}

#[derive(Debug, Clone, Deserialize, Hash, PartialEq, Eq)]
pub(crate) struct ExtensionLibraries {
    pub(crate) source: PathBuf,
    pub(crate) sink: PathBuf,
    pub(crate) operator: PathBuf,
}

impl ExtensionLibraries {
    /// Return a new set of extension libraries after validating them.
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol,
    /// - was not compiled with the same Rust version,
    /// - was not using the same Zenoh-Flow version as this Zenoh-Flow [runtime](crate::Runtime).
    pub(crate) fn new(source: PathBuf, operator: PathBuf, sink: PathBuf) -> Result<Self> {
        let libraries = Self {
            source,
            sink,
            operator,
        };

        libraries.validate()?;

        Ok(libraries)
    }

    /// Validates that all the libraries expose the correct symbols and were compiled with the same Rust and Zenoh-Flow
    /// versions.
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol,
    /// - was not compiled with the same Rust version,
    /// - was not using the same Zenoh-Flow version as this Zenoh-Flow [runtime](crate::Runtime).
    //
    // NOTE: We are separating this method from the `new` method because, when we deserialise this structure, we need to
    // call `validate` after creating it.
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

/// Attempts to deserialise a set of [Extension] from the provided string.
///
/// # Errors
///
/// This function will return an error if:
/// - the string cannot be deserialised into a vector of [Extension],
/// - any [Extension] does not provide valid libraries.
pub(crate) fn deserialize_extensions<'de, D>(
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
