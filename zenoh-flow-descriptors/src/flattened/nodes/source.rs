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

use crate::SourceDescriptor;

/// Textual representation of a Zenoh-Flow Source node.
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::FlattenedSourceDescriptor;
///
/// let source_yaml = "
///     id: Source-0
///     name: Source
///     configuration:
///       foo: bar
///       answer: 0
///     uri: file:///home/zenoh-flow/node/libsource.so
///     outputs:
///       - out-operator
///     mapping: zenoh-flow-plugin-0
/// ";
/// let source_yaml = serde_yaml::from_str::<FlattenedSourceDescriptor>(&source_yaml).unwrap();
///
/// let source_json = "
///     {
///       \"id\": \"Source-0\",
///       \"name\": \"Source\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 0
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/libsource.so\",
///       \"outputs\": [
///         \"out-operator\"
///       ],
///       \"mapping\": \"zenoh-flow-plugin-0\"
///     }
/// ";
///
/// let source_json = serde_json::from_str::<FlattenedSourceDescriptor>(&source_json).unwrap();
///
/// assert_eq!(source_yaml, source_json);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedSourceDescriptor {
    pub id: NodeId,
    pub name: String,
    pub uri: Option<String>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    pub runtime: Option<RuntimeId>,
}

impl Display for FlattenedSourceDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedSourceDescriptor {
    pub fn new(
        source: SourceDescriptor,
        id: NodeId,
        overwritting_configuration: Configuration,
        runtime: Option<RuntimeId>,
    ) -> Self {
        let SourceDescriptor {
            name,
            uri,
            outputs,
            configuration,
        } = source;

        Self {
            id,
            name,
            uri,
            outputs,
            configuration: overwritting_configuration.merge_overwrite(configuration),
            runtime,
        }
    }
}
