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

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use zenoh_flow_commons::{Configuration, NodeId, PortId};
use zenoh_flow_records::OperatorRecord;

/// TODO@J-Loudet Documentation?
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::FlattenedOperatorDescriptor;
///
/// let yaml = "
///     id: Operator-1
///     name: Operator
///     configuration:
///       foo: bar
///       answer: 1
///     uri: file:///home/zenoh-flow/node/liboperator.so
///     inputs:
///       - in-source
///     outputs:
///       - out-sink
/// ";
/// let operator_yaml = serde_yaml::from_str::<FlattenedOperatorDescriptor>(yaml).unwrap();
///
/// let json = "
///     {
///       \"id\": \"Operator-1\",
///       \"name\": \"Operator\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 1
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/liboperator.so\",
///       \"inputs\": [
///         \"in-source\"
///       ],
///       \"outputs\": [
///         \"out-sink\"
///       ]
///     }
/// ";
///
/// let operator_json = serde_json::from_str::<FlattenedOperatorDescriptor>(json).unwrap();
///
/// assert_eq!(operator_yaml, operator_json);
/// ```

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct FlattenedOperatorDescriptor {
    pub id: NodeId,
    pub name: Arc<str>,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    pub uri: Option<Arc<str>>,
    #[serde(default)]
    pub configuration: Configuration,
}

impl Display for FlattenedOperatorDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedOperatorDescriptor {
    /// Update the identifier of the [FlattenedOperatorDescriptor] prepending the id of the
    /// [CompositeOperatorDescriptor] it belongs to.
    ///
    /// # TODO
    ///
    /// - Prevent the usage of "/" in the id of nodes.
    pub fn composite_id(&mut self, composite_id: &NodeId) -> NodeId {
        self.id = format!("{composite_id}/{}", self.id).into();
        self.id.clone()
    }
}

impl From<FlattenedOperatorDescriptor> for OperatorRecord {
    fn from(value: FlattenedOperatorDescriptor) -> Self {
        let FlattenedOperatorDescriptor {
            id,
            name,
            inputs,
            outputs,
            uri,
            configuration,
        } = value;

        Self {
            id,
            name,
            inputs,
            outputs,
            uri,
            configuration,
        }
    }
}
