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

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, RuntimeId};

use crate::SinkDescriptor;

/// Textual representation of a Zenoh-Flow Sink node.
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::FlattenedSinkDescriptor;
///
/// let sink_yaml = "
///     id: Sink-2
///     name: Sink
///     configuration:
///       foo: bar
///       answer: 2
///     uri: file:///home/zenoh-flow/node/libsink.so
///     inputs:
///       - in-operator
/// ";
/// let sink_yaml = serde_yaml::from_str::<FlattenedSinkDescriptor>(&sink_yaml).unwrap();
///
/// let sink_json = "
///     {
///       \"id\": \"Sink-2\",
///       \"name\": \"Sink\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 2
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/libsink.so\",
///       \"inputs\": [
///         \"in-operator\"
///       ]
///     }
/// ";
///
/// let sink_json = serde_json::from_str::<FlattenedSinkDescriptor>(&sink_json).unwrap();
///
/// assert_eq!(sink_yaml, sink_json);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedSinkDescriptor {
    pub id: NodeId,
    pub name: String,
    pub uri: Option<String>,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    pub runtime: Option<RuntimeId>,
}

impl Display for FlattenedSinkDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedSinkDescriptor {
    pub fn new(
        sink: SinkDescriptor,
        id: NodeId,
        overwritting_configuration: Configuration,
        runtime: Option<RuntimeId>,
    ) -> Self {
        let SinkDescriptor {
            name,
            configuration,
            uri,
            inputs,
        } = sink;

        Self {
            id,
            name,
            uri,
            inputs,
            configuration: overwritting_configuration.merge_overwrite(configuration),
            runtime,
        }
    }
}
