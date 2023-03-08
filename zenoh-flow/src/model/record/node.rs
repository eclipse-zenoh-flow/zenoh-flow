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

use crate::model::record::PortRecord;
use crate::types::{Configuration, NodeId, RuntimeId};
use serde::{Deserialize, Serialize};

/// A `SinkRecord` is an instance of a [`SinkDescriptor`](`crate::model::descriptor::SinkDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkRecord {
    pub id: NodeId,
    pub uid: u32,
    pub inputs: Vec<PortRecord>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for SinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

/// A `SourceRecord` is an instance of a [`SourceDescriptor`](`crate::model::descriptor::SourceDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceRecord {
    pub id: NodeId,
    pub uid: u32,
    pub outputs: Vec<PortRecord>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for SourceRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

/// An `OperatorRecord` is an instance of an [`OperatorDescriptor`](`crate::model::descriptor::OperatorDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperatorRecord {
    pub id: NodeId,
    pub uid: u32,
    pub inputs: Vec<PortRecord>,
    pub outputs: Vec<PortRecord>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for OperatorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}
