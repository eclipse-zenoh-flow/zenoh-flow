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

use crate::{InputDescriptor, LinkDescriptor, OperatorDescriptor, OutputDescriptor};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zenoh_flow_commons::{deserialize_id, Configuration, NodeId, PortId};

/// TODO@J-Loudet example?
/// TODO@J-Loudet documentation?
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompositeOutputDescriptor {
    pub id: PortId,
    #[serde(deserialize_with = "deserialize_id")]
    pub node: NodeId,
    pub output: PortId,
}

impl CompositeOutputDescriptor {
    pub fn new(id: impl AsRef<str>, node: impl AsRef<str>, output: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().into(),
            node: node.as_ref().into(),
            output: output.as_ref().into(),
        }
    }
}

impl From<CompositeOutputDescriptor> for OutputDescriptor {
    fn from(value: CompositeOutputDescriptor) -> Self {
        Self {
            node: value.node,
            output: value.output,
        }
    }
}

/// Describes an Input of a Composite Operator.
///
/// # Example
///
/// ```yaml
/// id: UniquePortIdentifier
/// node: Node
/// input: Input
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CompositeInputDescriptor {
    pub id: PortId,
    #[serde(deserialize_with = "deserialize_id")]
    pub node: NodeId,
    pub input: PortId,
}

impl CompositeInputDescriptor {
    pub fn new(id: impl AsRef<str>, node: impl AsRef<str>, input: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().into(),
            node: node.as_ref().into(),
            input: input.as_ref().into(),
        }
    }
}

impl From<CompositeInputDescriptor> for InputDescriptor {
    fn from(value: CompositeInputDescriptor) -> Self {
        Self {
            node: value.node,
            input: value.input,
        }
    }
}

/// A `Composite Operator` Zenoh-Flow node.
///
/// A Composite Operator is a meta-operator: it groups together one or more Operators in a single descriptor. Its main
/// purpose is to simplify the creation of data flow graphs by allowing this form of grouping.
///
/// # `configuration` section caveats
///
/// The `configuration` section of a Composite Operator supersedes the same section in the Operator(s) it references.
/// See the documentation for more details regarding this behavior.
///
/// # Examples
///
/// ```
/// use descriptors::CompositeOperatorDescriptor;
///
/// let yaml = r#"
/// description: CompositeOperator
///
/// configuration:
///   name: foo
///
/// operators:
///   - id: InnerOperator1
///     descriptor: file:///home/zenoh-flow/nodes/operator1.yaml
///
///   - id: InnerOperator2
///     descriptor: file:///home/zenoh-flow/nodes/operator2.yaml
///
/// links:
///   - from:
///       node: InnerOperator1
///       output: out-2
///     to:
///       node: InnerOperator2
///       input: in-1
///
/// inputs:
///   - id: CompositeOperator-in
///     node: InnerOperator1
///     input: in-1
///
/// outputs:
///   - id: CompositeOperator-out
///     node: InnerOperator2
///     output: out-1
/// "#;
///
/// let composite_operator_yaml = serde_yaml::from_str::<CompositeOperatorDescriptor>(&yaml).unwrap();
///
/// let json = r#"
/// {
///   "description": "CompositeOperator",
///
///   "configuration": {
///     "name": "foo"
///   },
///
///   "operators": [
///     {
///       "id": "InnerOperator1",
///       "descriptor": "file:///home/zenoh-flow/nodes/operator1.yaml"
///     },
///     {
///       "id": "InnerOperator2",
///       "descriptor": "file:///home/zenoh-flow/nodes/operator2.yaml"
///     }
///   ],
///
///   "links": [
///     {
///       "from": {
///         "node": "InnerOperator1",
///         "output": "out-2"
///       },
///       "to": {
///         "node": "InnerOperator2",
///         "input": "in-1"
///       }
///     }
///   ],
///
///   "inputs": [
///     {
///       "id": "CompositeOperator-in",
///       "node": "InnerOperator1",
///       "input": "in-1"
///     }
///   ],
///
///   "outputs": [
///     {
///       "id": "CompositeOperator-out",
///       "node": "InnerOperator2",
///       "output": "out-1"
///     }
///   ]
/// }"#;
///
/// let composite_operator_json = serde_json::from_str::<CompositeOperatorDescriptor>(&json).unwrap();
/// assert_eq!(composite_operator_yaml, composite_operator_json);
/// assert_eq!(composite_operator_yaml.configuration.get("name").unwrap().as_str(), Some("foo"));
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CompositeOperatorDescriptor {
    pub description: Arc<str>,
    pub inputs: Vec<CompositeInputDescriptor>,
    pub outputs: Vec<CompositeOutputDescriptor>,
    pub operators: Vec<OperatorDescriptor>,
    pub links: Vec<LinkDescriptor>,
    #[serde(default)]
    pub configuration: Configuration,
}

impl std::fmt::Display for CompositeOperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Composite Operator: {}", self.description)
    }
}
