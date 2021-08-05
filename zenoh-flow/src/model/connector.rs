//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::model::link::ZFPortDescriptor;
use crate::serde::{Deserialize, Serialize};
use crate::ZFRuntimeID;

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFConnectorRecord {
    pub kind: ZFConnectorKind,
    pub id: String,
    pub resource: String,
    pub link_id: ZFPortDescriptor,
    pub runtime: ZFRuntimeID,
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
