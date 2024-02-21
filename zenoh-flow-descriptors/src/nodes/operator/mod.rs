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

pub(crate) mod composite;

use super::RemoteNodeDescriptor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use url::Url;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

/// A `OperatorDescriptor` uniquely identifies a Operator.
///
/// Zenoh-Flow supports several ways of declaring a Operator:
/// - by importing a "remote" descriptor,
/// - with an inline declaration of a [`CustomOperatorDescriptor`].
///
/// # Remote descriptor: composite or custom
///
/// Specifying a remote descriptor allows including a [CompositeOperatorDescriptor]: the composition of several
/// [CustomOperatorDescriptor]s.
///
/// Manually describing a composite operator is not supported as it defeats its primary purpose: simplifying the
/// creation of a data flow.
///
/// # Caveat: `NodeId`
///
/// Zenoh-Flow nodes cannot contain the "slash" character '/'. Including such character in the id will result in a hard
/// error when parsing.
///
/// # Examples
///
/// ## Remote descriptor
///
/// ```
/// use zenoh_flow_descriptors::OperatorDescriptor;
///
/// let operator_desc_uri = r#"
/// id: my-operator-1
/// descriptor: file:///home/zenoh-flow/my-operator.yaml
/// configuration:
///   answer: 1
/// "#;
///
/// assert!(serde_yaml::from_str::<OperatorDescriptor>(operator_desc_uri).is_ok());
/// ```
///
/// ## Inline declaration: custom operator
///
/// ```
/// use zenoh_flow_descriptors::OperatorDescriptor;
///
/// let operator_desc_custom = r#"
/// id: my-operator-1
/// description: This is my Operator
/// library: file:///home/zenoh-flow/libmy_operator.so
/// inputs:
///   - in-1
/// outputs:
///   - out-1
/// configuration:
///   answer: 1
/// "#;
///
/// assert!(serde_yaml::from_str::<OperatorDescriptor>(operator_desc_custom).is_ok());
/// ```
///
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct OperatorDescriptor {
    pub(crate) id: NodeId,
    #[serde(flatten)]
    pub(crate) variant: OperatorVariants,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum OperatorVariants {
    Remote(RemoteNodeDescriptor),
    Custom(CustomOperatorDescriptor),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CustomOperatorDescriptor {
    pub description: Arc<str>,
    pub library: Url,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}
