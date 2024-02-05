//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use std::{path::PathBuf, sync::Arc};

use serde::Deserialize;
use zenoh_flow_runtime::Extensions;

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub struct ZenohFlowConfiguration {
    pub name: Arc<str>,
    pub extensions: Option<ExtensionsConfiguration>,
    #[cfg(not(feature = "plugin"))]
    pub zenoh: ZenohConfiguration,
}

#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ExtensionsConfiguration {
    File(PathBuf),
    Extensions(Extensions),
}

#[cfg(not(feature = "plugin"))]
#[derive(Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum ZenohConfiguration {
    File(PathBuf),
    Configuration(zenoh::prelude::Config),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let configuration_extension_path_yaml = r#"
name: My test daemon

extensions: /home/zenoh-flow/extensions/configuration.ext
"#;

        let config = serde_yaml::from_str::<ZenohFlowConfiguration>(
            configuration_extension_path_yaml,
        )
        .expect(
            "Failed to parse ZenohFlowConfiguration with Zenoh as a PathBuf + Extensions as a PathBuf",
        );

        assert_eq!(
            ZenohFlowConfiguration {
                name: "My test daemon".into(),
                extensions: Some(ExtensionsConfiguration::File(PathBuf::from(
                    "/home/zenoh-flow/extensions/configuration.ext"
                ))),
            },
            config
        );

        let configuration_extension_inline_yaml = r#"
name: My test daemon

extensions:
  - file_extension: py
    libraries:
      source: /home/zenoh-flow/extension/libpython_source.so
      operator: /home/zenoh-flow/extension/libpython_operator.so
      sink: /home/zenoh-flow/extension/libpython_sink.so
"#;

        let config =
            serde_yaml::from_str::<ZenohFlowConfiguration>(configuration_extension_inline_yaml)
                .expect(
                    "Failed to parse ZenohFlowConfiguration with Zenoh inline + Extensions inline",
                );

        assert_eq!(
            ZenohFlowConfiguration {
                name: "My test daemon".into(),
                extensions: Some(ExtensionsConfiguration::Extensions(
                    Extensions::default().add_extension(
                        "py",
                        PathBuf::from("/home/zenoh-flow/extension/libpython_source.so"),
                        "/home/zenoh-flow/extension/libpython_operator.so".into(),
                        "/home/zenoh-flow/extension/libpython_sink.so".into()
                    )
                )),
            },
            config
        );
    }
}
