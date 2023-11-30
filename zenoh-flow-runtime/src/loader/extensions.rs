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

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
};

use serde::{Deserialize, Deserializer};

// Convenient shortcut.
#[derive(Default, Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize, Hash)]
pub struct Extension {
    pub(crate) file_extension: Arc<str>,
    pub(crate) libraries: ExtensionLibraries,
}

#[derive(Debug, Clone, Deserialize, Hash)]
pub(crate) struct ExtensionLibraries {
    pub(crate) source: PathBuf,
    pub(crate) sink: PathBuf,
    pub(crate) operator: PathBuf,
}

impl Extensions {
    /// TODO@J-Loudet
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
}

pub fn deserialize_extensions<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<Arc<str>, Extension>, D::Error>
where
    D: Deserializer<'de>,
{
    let extensions: Vec<Extension> = serde::de::Deserialize::deserialize(deserializer)?;

    Ok(extensions
        .into_iter()
        .map(|extension| (extension.file_extension.clone(), extension))
        .collect::<HashMap<_, _>>())
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
