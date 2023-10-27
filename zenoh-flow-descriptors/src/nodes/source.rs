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

use crate::ZenohSourceDescriptor;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use zenoh_flow_commons::{Configuration, NodeId, PortId};

use super::RemoteNodeDescriptor;

/// A `SourceDescriptor` uniquely identifies a Source.
///
/// Zenoh-Flow supports several ways of declaring a Source:
/// - by importing a "remote" descriptor,
/// - with an inline declaration of a [`CustomSourceDescriptor`],
/// - with an inline declaration of a Zenoh built-in.
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
/// use zenoh_flow_descriptors::SourceDescriptor;
///
/// let source_desc_uri = r#"
/// id: my-source-0
/// descriptor: file:///home/zenoh-flow/my-source.yaml
/// configuration:
///   answer: 0
/// "#;
///
/// assert!(serde_yaml::from_str::<SourceDescriptor>(source_desc_uri).is_ok());
/// ```
///
/// ## Inline declaration
/// ### Custom source
///
/// ```
/// use zenoh_flow_descriptors::SourceDescriptor;
///
/// let source_desc_custom = r#"
/// id: my-source-0
/// description: This is my Source
/// library: file:///home/zenoh-flow/libmy_source.so
/// outputs:
///   - out-0
///   - out-1
/// configuration:
///   answer: 42
/// "#;
///
/// assert!(serde_yaml::from_str::<SourceDescriptor>(source_desc_custom).is_ok());
/// ```
///
/// ### Zenoh built-in Source
///
/// ```
/// use zenoh_flow_descriptors::SourceDescriptor;
///
/// let source_desc_zenoh = r#"
/// id: my-source-0
/// description: My zenoh source
/// zenoh-subscribers:
///   - key/expr/0
///   - key/expr/1
/// "#;
///
/// assert!(serde_yaml::from_str::<SourceDescriptor>(source_desc_zenoh).is_ok());
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub id: NodeId,
    #[serde(flatten)]
    pub(crate) variant: SourceVariants,
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
    pub description: Arc<str>,
    pub library: Arc<str>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}
