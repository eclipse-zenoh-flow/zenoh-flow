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

use crate::{flattened::IFlattenable, FlattenedSourceDescriptor};
use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, RuntimeId};

/// Textual representation of a Zenoh-Flow Source node.
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::SourceDescriptor;
///
/// let source_yaml = "
/// name: Source
/// configuration:
///   answer: 42
/// uri: file:///home/zenoh-flow/node/libsource.so
/// outputs:
///   - out-operator
/// ";
/// let source_yaml = serde_yaml::from_str::<SourceDescriptor>(&source_yaml).unwrap();
///
/// let source_json = "
/// {
///   \"name\": \"Source\",
///   \"configuration\": {
///     \"answer\": 42
///   },
///   \"uri\": \"file:///home/zenoh-flow/node/libsource.so\",
///   \"outputs\": [
///     \"out-operator\"
///   ]
/// }";
///
/// let source_json = serde_json::from_str::<SourceDescriptor>(&source_json).unwrap();
///
/// assert_eq!(source_yaml, source_json);
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub name: String,
    pub uri: Option<String>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}

/// TODO@J-Loudet Improve display.
impl std::fmt::Display for SourceDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Source:\n{}", self.name)
    }
}

impl IFlattenable for SourceDescriptor {
    type Flattened = FlattenedSourceDescriptor;

    fn flatten(
        self,
        id: NodeId,
        overwriting_configuration: Configuration,
        runtime: Option<RuntimeId>,
    ) -> Self::Flattened {
        let SourceDescriptor {
            name,
            uri,
            outputs,
            configuration,
        } = self;

        FlattenedSourceDescriptor {
            id,
            name,
            uri,
            outputs,
            configuration: overwriting_configuration.merge_overwrite(configuration),
            runtime,
        }
    }
}
