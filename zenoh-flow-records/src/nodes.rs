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

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, NodeId, PortId};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceRecord {
    pub id: NodeId,
    pub description: Arc<str>,
    pub outputs: Vec<PortId>,
    pub uri: Option<Arc<str>>,
    pub configuration: Configuration,
}

/// TODO@J-Loudet
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct OperatorRecord {
    pub id: NodeId,
    pub description: Arc<str>,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    pub uri: Option<Arc<str>>,
    pub configuration: Configuration,
}

/// TODO@J-Loudet
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkRecord {
    pub id: NodeId,
    pub description: Arc<str>,
    pub inputs: Vec<PortId>,
    pub uri: Option<Arc<str>>,
    pub configuration: Configuration,
}
