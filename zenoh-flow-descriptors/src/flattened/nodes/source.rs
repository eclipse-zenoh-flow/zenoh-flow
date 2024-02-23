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

use crate::nodes::builtin::zenoh::ZenohSourceDescriptor;
use crate::nodes::source::{CustomSourceDescriptor, SourceDescriptor, SourceVariants};
use crate::uri;

use std::{collections::HashMap, fmt::Display, sync::Arc};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use url::Url;
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, Result, Vars};
use zenoh_keyexpr::OwnedKeyExpr;

/// A `FlattenedSourceDescriptor` is a self-contained description of a Source node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedSourceDescriptor {
    /// The unique (within a data flow) identifier of the Source.
    pub id: NodeId,
    /// A human-readable description of the Source.
    pub description: Option<Arc<str>>,
    /// The type of implementation of the Source, either built-in or a path to a Library.
    #[serde(flatten)]
    pub source: SourceVariant,
    /// The identifiers of the outputs the Source uses.
    pub outputs: Vec<PortId>,
    /// Pairs of `(key, value)` to change the behaviour of the Source without altering its implementation.
    #[serde(default)]
    pub configuration: Configuration,
}

/// ⚠️ This is structure is intended for internal usage.
///
/// The implementation of a Source: either a custom Source with the location of its implementation or a Zenoh built-in
/// node with the list of key expressions to which it should subscribe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceVariant {
    Library(Url),
    Zenoh(HashMap<PortId, OwnedKeyExpr>),
}

/// The Source variant after it has been fetched (if it was remote) but before it has been flattened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum LocalSourceVariants {
    Custom(CustomSourceDescriptor),
    Zenoh(ZenohSourceDescriptor),
}

impl Display for FlattenedSourceDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedSourceDescriptor {
    /// Attempts to flatten a [SourceDescriptor] into a [FlattenedSourceDescriptor].
    ///
    /// If the descriptor needs to be fetched this function will first fetch it, propagate and merge the overwriting
    /// [Vars] and finally construct a [FlattenedSourceDescriptor].
    ///
    /// The configuration of the Source is finally merged with the `overwriting_configuration`.
    ///
    /// # Errors
    ///
    /// The flattening process can fail if we cannot retrieve the remote descriptor.
    pub(crate) fn try_flatten(
        source_desc: SourceDescriptor,
        overwritting_vars: Vars,
        mut overwritting_configuration: Configuration,
    ) -> Result<Self> {
        let descriptor = match source_desc.variant {
            SourceVariants::Remote(remote_desc) => {
                let (mut descriptor, _) = uri::try_load_descriptor::<LocalSourceVariants>(
                    &remote_desc.descriptor,
                    overwritting_vars,
                )
                .context(format!(
                    "[{}] Failed to load source descriptor from < {} >",
                    source_desc.id, &remote_desc.descriptor
                ))?;
                overwritting_configuration = remote_desc
                    .configuration
                    .merge_overwrite(overwritting_configuration);

                if let LocalSourceVariants::Custom(ref mut desc) = descriptor {
                    let description = desc.description.take();
                    desc.description = remote_desc.description.or(description);
                }

                descriptor
            }
            SourceVariants::Zenoh(zenoh_desc) => LocalSourceVariants::Zenoh(zenoh_desc),
            SourceVariants::Custom(custom_desc) => {
                overwritting_configuration = custom_desc
                    .clone()
                    .configuration
                    .merge_overwrite(overwritting_configuration);

                LocalSourceVariants::Custom(custom_desc)
            }
        };

        match descriptor {
            LocalSourceVariants::Custom(custom_source) => Ok(Self {
                id: source_desc.id,
                description: custom_source.description,
                source: SourceVariant::Library(custom_source.library),
                outputs: custom_source.outputs,
                configuration: overwritting_configuration
                    .merge_overwrite(custom_source.configuration),
            }),
            LocalSourceVariants::Zenoh(zenoh_desc) => Ok(Self {
                id: source_desc.id,
                description: zenoh_desc.description,
                outputs: zenoh_desc.subscribers.keys().cloned().collect(),
                source: SourceVariant::Zenoh(zenoh_desc.subscribers),
                configuration: Configuration::default(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_zenoh() {
        let yaml_str = r#"
id: source-0
description: source-0
zenoh:
  kitchen_all: home/kitchen/*
outputs:
  - kitchen_all
configuration: null
runtime: null
"#;
        let flat_source: FlattenedSourceDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");

        assert!(serde_yaml::to_string(&flat_source).is_ok());
    }

    #[test]
    fn test_serialize_no_configuration() {
        let yaml_str = r#"
id: "source-0"
description: "source-0"
library: file:///home/zenoh-flow/nodes/libsource_0.so
outputs:
  - out-0
"#;

        let flat_source: FlattenedSourceDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");
        assert!(serde_yaml::to_string(&flat_source).is_ok());
    }

    #[test]
    fn test_serialize_full() {
        let yaml_str = r#"
id: "source-0"
description: "source-0"
library: file:///home/zenoh-flow/nodes/libsource_0.so
outputs:
  - out-0
runtime: 3dd70f57-feb7-424c-9278-8bc8813c644e
configuration:
  answer: 42
"#;

        let flat_source: FlattenedSourceDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");
        assert!(serde_yaml::to_string(&flat_source).is_ok());
    }
}
