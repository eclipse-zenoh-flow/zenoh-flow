//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use serde::{Deserialize, Serialize};
use url::Url;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

use super::RemoteNodeDescriptor;
use crate::nodes::builtin::zenoh::ZenohSinkDescriptor;

/// A `SinkDescriptor` uniquely identifies a Sink.
///
/// Zenoh-Flow supports several ways of declaring a Sink:
/// - by importing a "remote" descriptor (e.g. located in another descriptor file),
/// - with an inline declaration,
/// - with an inline declaration of a Zenoh built-in, listing on which key expressions to publish.
///
/// # ⚠️ Caveat: `NodeId` and `PortId`
///
/// Zenoh-Flow identifiers cannot contain certain special characters as it could prevent creating valid Zenoh
/// key-expressions. The list can be found [here](zenoh_flow_commons::deserialize_id).
///
/// # Examples
/// ## Remote descriptor
///
/// ```yaml
/// id: my-sink-0
/// descriptor: file:///home/zenoh-flow/my-sink.yaml
/// configuration:
///   answer: 0
/// ```
///
/// ## Inline declaration
/// ### Custom sink
///
/// ```yaml
/// id: my-sink-0
/// description: This is my Sink
/// library: file:///home/zenoh-flow/libmy_sink.so
/// inputs:
///   - out-0
///   - out-1
/// configuration:
///   answer: 42
/// ```
///
/// ### Zenoh built-in Sink
///
/// ```yaml
/// id: my-sink-0
/// description: My zenoh sink
/// zenoh-publishers:
///   key_0: key/expr/0
///   key_1: key/expr/1
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SinkDescriptor {
    pub id: NodeId,
    #[serde(flatten)]
    pub variant: SinkVariants,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum SinkVariants {
    Zenoh(ZenohSinkDescriptor),
    Remote(RemoteNodeDescriptor),
    Custom(CustomSinkDescriptor),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CustomSinkDescriptor {
    pub description: Option<Arc<str>>,
    pub library: Url,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}
