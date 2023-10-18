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

use std::sync::Arc;

use crate::{flattened::IFlattenable, FlattenedSinkDescriptor};
use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId};

/// Textual representation of a Zenoh-Flow Sink node.
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::SinkDescriptor;
///
/// let sink_yaml = "
/// name: Sink
/// configuration:
///   answer: 42
/// uri: file:///home/zenoh-flow/node/libsink.so
/// inputs:
///   - in-operator
/// ";
/// let sink_yaml = serde_yaml::from_str::<SinkDescriptor>(sink_yaml).unwrap();
///
/// let sink_json = "
/// {
///   \"name\": \"Sink\",
///   \"configuration\": {
///     \"answer\": 42
///   },
///   \"uri\": \"file:///home/zenoh-flow/node/libsink.so\",
///   \"inputs\": [
///     \"in-operator\"
///   ]
/// }";
///
/// let sink_json = serde_json::from_str::<SinkDescriptor>(sink_json).unwrap();
///
/// assert_eq!(sink_yaml, sink_json);
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkDescriptor {
    pub name: Arc<str>,
    #[serde(default)]
    pub configuration: Configuration,
    pub uri: Option<Arc<str>>,
    pub inputs: Vec<PortId>,
}

// TODO@J-Loudet Improve
impl std::fmt::Display for SinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Sink:\n{}", self.name)
    }
}

impl IFlattenable for SinkDescriptor {
    type Flattened = FlattenedSinkDescriptor;

    fn flatten(self, id: NodeId, overwritting_configuration: Configuration) -> Self::Flattened {
        FlattenedSinkDescriptor {
            id,
            name: self.name,
            uri: self.uri,
            inputs: self.inputs,
            configuration: overwritting_configuration.merge_overwrite(self.configuration),
        }
    }
}
