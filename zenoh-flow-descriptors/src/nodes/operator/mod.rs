//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

pub(crate) mod composite;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

use super::RemoteNodeDescriptor;

/// An `OperatorDescriptor` uniquely identifies and configures an Operator.
///
/// Zenoh-Flow supports several ways of declaring a Operator:
/// - by importing a "remote" descriptor (e.g. located in another descriptor file),
/// - with an inline declaration.
///
/// It is not possible to define an `Operator` inside your code base. This structure was made to be parsed from a
/// configuration file. You should instead use a [FlattenedOperatorDescriptor](crate::FlattenedOperatorDescriptor).
///
/// # Remote descriptor: composite or custom
///
/// Specifying a remote descriptor allows including a [CompositeOperatorDescriptor](crate::CompositeOperatorDescriptor):
/// the composition of several Operators.
///
/// Manually describing a composite operator is not supported as it defeats its primary purpose: simplifying the
/// creation of a data flow.
///
/// # ⚠️ Caveat: `NodeId` and `PortId`
///
/// Zenoh-Flow identifiers cannot contain certain special characters as it could prevent creating valid Zenoh
/// key-expressions. The list can be found [here](zenoh_flow_commons::deserialize_id).
///
///
/// # Examples
///
/// ## Remote descriptor
///
/// ⚠️ For now only the `file://` schema is supported. We are planning to support other protocols in future releases of
/// Zenoh-Flow.
///
/// ```yaml
/// id: my-operator-1
/// descriptor: file:///home/zenoh-flow/my-operator.yaml
/// configuration:
///   answer: 1
/// ```
///
/// With the file at `/home/zenoh-flow/my-operator.yaml` containing:
/// ```yaml
/// description: This is my Operator
/// library: file:///home/zenoh-flow/libmy_operator.so
/// inputs:
///   - in-1
/// outputs:
///   - out-1
/// ```
///
/// ## Inline declaration: custom operator
///
/// ```yaml
/// id: my-operator-1
/// description: This is my Operator
/// library: file:///home/zenoh-flow/libmy_operator.so
/// inputs:
///   - in-1
/// outputs:
///   - out-1
/// configuration:
///   answer: 1
/// ```
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct OperatorDescriptor {
    pub id: NodeId,
    #[serde(flatten)]
    pub variant: OperatorVariants,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum OperatorVariants {
    Remote(RemoteNodeDescriptor),
    Custom(CustomOperatorDescriptor),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CustomOperatorDescriptor {
    pub description: Option<Arc<str>>,
    pub library: Url,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}
