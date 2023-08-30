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

pub(crate) mod message;
pub use message::*;
pub(crate) mod context;
pub use context::*;
pub(crate) mod configuration;
pub use configuration::Configuration;

use std::sync::Arc;

/// A NodeId identifies a node inside a Zenoh Flow graph
pub type NodeId = Arc<str>;
/// A PortId identifies a port within an node.
pub type PortId = Arc<str>;
/// A RuntimeId identifies a runtime within the Zenoh Flow infrastructure.
pub type RuntimeId = Arc<str>;
/// A FlowId identifies a Zenoh Flow graph within Zenoh Flow
pub type FlowId = Arc<str>;
