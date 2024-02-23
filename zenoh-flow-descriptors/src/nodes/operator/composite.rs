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

use crate::nodes::operator::OperatorDescriptor;
use crate::{InputDescriptor, LinkDescriptor, OutputDescriptor};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

/// A `CompositeOutputDescriptor` exposes the [Output](OutputDescriptor) of a node.
///
/// # Example (YAML)
///
/// ```yaml
/// id: my-composite-output
/// node: my-operator
/// output: out
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CompositeOutputDescriptor {
    pub id: PortId,
    pub node: NodeId,
    pub output: PortId,
}

impl From<CompositeOutputDescriptor> for OutputDescriptor {
    fn from(value: CompositeOutputDescriptor) -> Self {
        Self {
            node: value.node,
            output: value.output,
        }
    }
}

/// A `CompositeInputDescriptor` exposes the [Input](InputDescriptor) of a node.
///
/// # Example (YAML)
///
/// ```yaml
/// id: my-composite-input
/// node: my-operator
/// input: in
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct CompositeInputDescriptor {
    pub id: PortId,
    pub node: NodeId,
    pub input: PortId,
}

impl From<CompositeInputDescriptor> for InputDescriptor {
    fn from(value: CompositeInputDescriptor) -> Self {
        Self {
            node: value.node,
            input: value.input,
        }
    }
}

/// A `CompositeOperatorDescriptor` groups together one or more Operators in a single descriptor.
///
/// Its main purpose is to simplify the creation of data flow graphs by allowing this form of grouping.
///
/// # Examples
///
/// ```yaml
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
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompositeOperatorDescriptor {
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
