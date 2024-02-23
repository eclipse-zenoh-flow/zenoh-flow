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

use super::RemoteNodeDescriptor;
use crate::nodes::builtin::zenoh::ZenohSourceDescriptor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use url::Url;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

/// A `SourceDescriptor` uniquely identifies a Source.
///
/// Zenoh-Flow supports several ways of declaring a Source:
/// - by importing a "remote" descriptor (e.g. located in another descriptor file),
/// - with an inline declaration,
/// - with an inline declaration of a Zenoh built-in.
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
/// id: my-source-0
/// descriptor: file:///home/zenoh-flow/my-source.yaml
/// configuration:
///   answer: 0
/// ```
///
/// ## Inline declaration
/// ### Custom source
///
/// ```yaml
/// id: my-source-0
/// description: This is my Source
/// library: file:///home/zenoh-flow/libmy_source.so
/// outputs:
///   - out-0
///   - out-1
/// configuration:
///   answer: 42
/// ```
///
/// ### Zenoh built-in Source
///
/// ```yaml
/// id: my-source-0
/// description: My zenoh source
/// zenoh-subscribers:
///   ke-0: key/expr/0
///   ke-1: key/expr/1
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceDescriptor {
    pub id: NodeId,
    #[serde(flatten)]
    pub variant: SourceVariants,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub(crate) enum SourceVariants {
    Zenoh(ZenohSourceDescriptor),
    Remote(RemoteNodeDescriptor),
    Custom(CustomSourceDescriptor),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CustomSourceDescriptor {
    pub description: Option<Arc<str>>,
    pub library: Url,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}
