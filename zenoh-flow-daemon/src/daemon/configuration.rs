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

use std::path::PathBuf;

use serde::Deserialize;
use zenoh_flow_runtime::Extensions;

/// The configuration of a Zenoh-Flow Daemon.
#[derive(Deserialize, Debug)]
pub struct ZenohFlowConfiguration {
    /// A human-readable name for this Daemon and its embedded Runtime.
    pub name: String,
    /// Additional supported extensions.
    pub extensions: Option<ExtensionsConfiguration>,
    /// The configuration to connect to Zenoh.
    #[cfg(not(feature = "plugin"))]
    pub zenoh: ZenohConfiguration,
}

/// Enumeration to facilitate defining the [Extensions] supported by the wrapped Zenoh-Flow [Runtime].
///
/// This enumeration allows either having the definition in the same configuration file or in a separate one.
///
/// [Runtime]: zenoh_flow_runtime::Runtime
#[derive(Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ExtensionsConfiguration {
    File(PathBuf),
    Extensions(Extensions),
}

/// Enumeration to facilitate defining the Zenoh configuration.
///
/// This enumeration allows either having the definition in the same configuration file or in a separate one.
#[cfg(not(feature = "plugin"))]
#[derive(Deserialize, Debug)]
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

        let expected_configuration = ZenohFlowConfiguration {
            name: "My test daemon".into(),
            extensions: Some(ExtensionsConfiguration::File(PathBuf::from(
                "/home/zenoh-flow/extensions/configuration.ext",
            ))),
        };

        assert_eq!(expected_configuration.name, config.name);
        assert_eq!(expected_configuration.extensions, config.extensions);

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

        assert_eq!(expected_configuration.name, config.name);
        assert!(config.extensions.is_some());
        let config_extensions = config.extensions.unwrap();
        assert!(matches!(
            config_extensions,
            ExtensionsConfiguration::Extensions(_)
        ));
        if let ExtensionsConfiguration::Extensions(extensions) = config_extensions {
            assert!(extensions.get("py").is_some());
        }
    }
}
