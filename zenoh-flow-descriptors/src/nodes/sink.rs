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

use crate::ZenohSinkDescriptor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

use super::RemoteNodeDescriptor;

/// A `SinkDescriptor` uniquely identifies a Sink.
///
/// Zenoh-Flow supports several ways of declaring a Sink:
/// - by importing a "remote" descriptor,
/// - with an inline declaration of a [`CustomSinkDescriptor`],
/// - with an inline declaration of a Zenoh built-in, listing on which key expressions to publish.
///
/// # Caveat: `NodeId`
///
/// Zenoh-Flow nodes cannot contain the "slash" character '/'. Including such character in the id will result in a hard
/// error when parsing.
///
/// # Examples
/// ## Remote descriptor
///
/// ```
/// use zenoh_flow_descriptors::SinkDescriptor;
///
/// let sink_desc_uri = r#"
/// id: my-sink-0
/// descriptor: file:///home/zenoh-flow/my-sink.yaml
/// configuration:
///   answer: 0
/// "#;
///
/// assert!(serde_yaml::from_str::<SinkDescriptor>(sink_desc_uri).is_ok());
/// ```
///
/// ## Inline declaration
/// ### Custom sink
///
/// ```
/// use zenoh_flow_descriptors::SinkDescriptor;
///
/// let sink_desc_custom = r#"
/// id: my-sink-0
/// description: This is my Sink
/// library: file:///home/zenoh-flow/libmy_sink.so
/// inputs:
///   - out-0
///   - out-1
/// configuration:
///   answer: 42
/// "#;
///
/// assert!(serde_yaml::from_str::<SinkDescriptor>(sink_desc_custom).is_ok());
/// ```
///
/// ### Zenoh built-in Sink
///
/// ```
/// use zenoh_flow_descriptors::SinkDescriptor;
///
/// let sink_desc_zenoh = r#"
/// id: my-sink-0
/// description: My zenoh sink
/// zenoh-publishers:
///   - key/expr/0
///   - key/expr/1
/// "#;
///
/// assert!(serde_yaml::from_str::<SinkDescriptor>(sink_desc_zenoh).is_ok());
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkDescriptor {
    pub id: NodeId,
    #[serde(flatten)]
    pub(crate) variant: SinkVariants,
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
    pub description: Arc<str>,
    pub library: Arc<str>,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}
