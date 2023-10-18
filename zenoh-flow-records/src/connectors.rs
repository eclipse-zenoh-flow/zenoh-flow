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

use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc};
use zenoh_flow_commons::{NodeId, PortId, SharedMemoryParameters};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ZenohSender {
    pub id: NodeId,
    pub resource: Arc<str>,
    pub input: PortId,
    pub shared_memory: SharedMemoryParameters,
}

impl Display for ZenohSender {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ZenohReceiver {
    pub id: NodeId,
    pub resource: Arc<str>,
    pub output: PortId,
    pub shared_memory: SharedMemoryParameters,
}

impl Display for ZenohReceiver {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
