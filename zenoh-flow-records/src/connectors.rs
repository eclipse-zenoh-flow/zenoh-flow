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
use std::fmt::Display;
use zenoh_flow_commons::NodeId;
use zenoh_keyexpr::OwnedKeyExpr;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SenderRecord {
    pub(crate) id: NodeId,
    pub(crate) resource: OwnedKeyExpr,
}

impl Display for SenderRecord {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl SenderRecord {
    pub fn id(&self) -> NodeId {
        self.id.clone()
    }

    pub fn resource(&self) -> &OwnedKeyExpr {
        &self.resource
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ReceiverRecord {
    pub(crate) id: NodeId,
    pub(crate) resource: OwnedKeyExpr,
}

impl Display for ReceiverRecord {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ReceiverRecord {
    pub fn id(&self) -> NodeId {
        self.id.clone()
    }

    pub fn resource(&self) -> &OwnedKeyExpr {
        &self.resource
    }
}
