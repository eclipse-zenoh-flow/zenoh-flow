//
// Copyright (c) 2022 ZettaScale Technology
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

use super::link::PortRecord;
use crate::types::{NodeId, RuntimeId};
use serde::{Deserialize, Serialize};

/// The type of the connector.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, Clone)]
pub enum ZFConnectorKind {
    Sender,
    Receiver,
}

impl std::fmt::Display for ZFConnectorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Receiver => write!(f, "Receiver"),
            Self::Sender => write!(f, "Sender"),
        }
    }
}

/// The internal representation of a connector within a [`DataFlowRecord`](`DataFlowRecord`).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFConnectorRecord {
    pub kind: ZFConnectorKind,
    pub id: NodeId,
    pub resource: String,
    pub link_id: PortRecord,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for ZFConnectorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} - {} - Kind: Connector{}",
            self.id, self.resource, self.kind
        )
    }
}
