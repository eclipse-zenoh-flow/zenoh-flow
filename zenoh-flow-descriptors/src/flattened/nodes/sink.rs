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

use crate::nodes::builtin::zenoh::ZenohSinkDescriptor;
use crate::nodes::sink::{CustomSinkDescriptor, SinkDescriptor, SinkVariants};
use crate::uri;

use std::{collections::HashMap, fmt::Display, sync::Arc};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use url::Url;
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, Result, Vars};
use zenoh_keyexpr::OwnedKeyExpr;

/// TODO@J-Loudet
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedSinkDescriptor {
    pub id: NodeId,
    pub description: Option<Arc<str>>,
    #[serde(flatten)]
    pub sink: SinkVariant,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkVariant {
    Library(Url),
    Zenoh(HashMap<PortId, OwnedKeyExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum LocalSinkVariants {
    Custom(CustomSinkDescriptor),
    Zenoh(ZenohSinkDescriptor),
}

impl Display for FlattenedSinkDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedSinkDescriptor {
    pub(crate) fn try_flatten(
        sink_desc: SinkDescriptor,
        overwritting_vars: Vars,
        mut overwritting_configuration: Configuration,
    ) -> Result<Self> {
        let descriptor = match sink_desc.variant {
            SinkVariants::Remote(remote_desc) => {
                let (mut descriptor, _) = uri::try_load_descriptor::<LocalSinkVariants>(
                    &remote_desc.descriptor,
                    overwritting_vars,
                )
                .context(format!(
                    "[{}] Failed to load sink descriptor from < {} >",
                    sink_desc.id, &remote_desc.descriptor
                ))?;

                overwritting_configuration = remote_desc
                    .configuration
                    .merge_overwrite(overwritting_configuration);

                if let LocalSinkVariants::Custom(ref mut desc) = descriptor {
                    let description = desc.description.take();
                    desc.description = remote_desc.description.or(description);
                }

                descriptor
            }
            SinkVariants::Zenoh(zenoh_desc) => LocalSinkVariants::Zenoh(zenoh_desc),
            SinkVariants::Custom(custom_desc) => {
                overwritting_configuration = custom_desc
                    .clone()
                    .configuration
                    .merge_overwrite(overwritting_configuration);
                LocalSinkVariants::Custom(custom_desc)
            }
        };

        match descriptor {
            LocalSinkVariants::Custom(custom_sink) => Ok(Self {
                id: sink_desc.id,
                description: custom_sink.description,
                sink: SinkVariant::Library(custom_sink.library),
                inputs: custom_sink.inputs,
                configuration: overwritting_configuration
                    .merge_overwrite(custom_sink.configuration),
            }),
            LocalSinkVariants::Zenoh(zenoh_desc) => Ok(Self {
                id: sink_desc.id,
                description: zenoh_desc.description,
                inputs: zenoh_desc.publishers.keys().cloned().collect(),
                sink: SinkVariant::Zenoh(zenoh_desc.publishers),
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
id: sink-2
description: sink-2
zenoh:
  kitchen_all: home/kitchen/*
inputs:
  - kitchen_all
configuration: null
"#;
        let flat_sink: FlattenedSinkDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");

        assert!(serde_yaml::to_string(&flat_sink).is_ok());
    }

    #[test]
    fn test_serialize_no_configuration() {
        let yaml_str = r#"
id: sink-2
description: sink-2
library: file:///home/zenoh-flow/nodes/libsink_0.so
inputs:
  - in-0
"#;

        let flat_sink: FlattenedSinkDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");
        assert!(serde_yaml::to_string(&flat_sink).is_ok());
    }

    #[test]
    fn test_serialize_full() {
        let yaml_str = r#"
id: "sink-0"
description: "sink-0"
library: file:///home/zenoh-flow/nodes/libsink_0.so
inputs:
  - in-0
configuration:
  answer: 42
"#;

        let flat_sink: FlattenedSinkDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");
        assert!(serde_yaml::to_string(&flat_sink).is_ok());
    }
}
