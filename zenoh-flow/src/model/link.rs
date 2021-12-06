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

use crate::model::{InputDescriptor, OutputDescriptor};
use crate::{PortId, PortType};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkDescriptor {
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub size: Option<usize>,
    pub queueing_policy: Option<String>,
    pub priority: Option<usize>,
}

impl std::fmt::Display for LinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} => {}", self.from, self.to)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PortDescriptor {
    #[serde(alias = "id")]
    pub port_id: PortId,
    #[serde(alias = "type")]
    pub port_type: PortType,
}

impl std::fmt::Display for PortDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.port_id, self.port_type)
    }
}
