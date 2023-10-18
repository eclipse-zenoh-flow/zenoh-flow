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

use crate::{flattened::IFlattenable, FlattenedOperatorDescriptor};
use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId};

/// Textual representation of a Zenoh-Flow Operator node.
///
/// # Example
///
/// ```
/// use serde_yaml;
/// use serde_json;
/// use zenoh_flow_descriptors::OperatorDescriptor;
///
/// let yaml = "
/// name: Operator
/// configuration:
///   answer: 42
/// uri: file:///home/zenoh-flow/node/liboperator.so
/// inputs:
///   - in-source
/// outputs:
///   - out-sink
/// ";
/// let operator_yaml = serde_yaml::from_str::<OperatorDescriptor>(yaml).unwrap();
///
/// let json = "
/// {
///   \"name\": \"Operator\",
///   \"configuration\": {
///     \"answer\": 42
///   },
///   \"uri\": \"file:///home/zenoh-flow/node/liboperator.so\",
///   \"inputs\": [
///     \"in-source\"
///   ],
///   \"outputs\": [
///     \"out-sink\"
///   ]
/// }";
///
/// let operator_json = serde_json::from_str::<OperatorDescriptor>(json).unwrap();
///
/// assert_eq!(operator_yaml, operator_json);
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct OperatorDescriptor {
    pub name: Arc<str>,
    pub uri: Option<Arc<str>>,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}

impl std::fmt::Display for OperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Operator:\n{}", self.name)
    }
}

impl IFlattenable for OperatorDescriptor {
    type Flattened = FlattenedOperatorDescriptor;

    fn flatten(self, id: NodeId, overwritting_configuration: Configuration) -> Self::Flattened {
        FlattenedOperatorDescriptor {
            id,
            name: self.name,
            inputs: self.inputs,
            outputs: self.outputs,
            uri: self.uri,
            configuration: overwritting_configuration.merge_overwrite(self.configuration),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uri;
    use zenoh_flow_commons::{NodeId, Vars};

    #[test]
    fn test_flatten_operator() {
        let id: NodeId = "my-operator-1".into();
        let expected_operator = FlattenedOperatorDescriptor {
            id: id.clone(),
            name: "operator-1".into(),
            inputs: vec!["operator-1-in-1".into(), "operator-1-in-2".into()],
            outputs: vec!["operator-1-out".into()],
            uri: Some("file://operator-1.so".into()),
            configuration: Configuration::default(),
        };

        let (operator_descriptor, _) = uri::try_load_descriptor::<OperatorDescriptor>(
            "file://./tests/descriptors/operator-1.yml",
            Vars::default(),
        )
        .expect("Unexpected error while parsing OperatorDescriptor");

        assert_eq!(
            expected_operator,
            operator_descriptor.flatten(id, Configuration::default())
        )
    }
}
