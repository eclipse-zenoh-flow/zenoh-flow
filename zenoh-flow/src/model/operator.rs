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

use crate::types::{ZFLinkId, ZFOperatorId, ZFOperatorName, ZFRuntimeID};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSinkDescriptor {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub input: ZFLinkId,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<ZFRuntimeID>,
}

impl std::fmt::Display for ZFSinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} - Kind: Sink", self.id, self.name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSourceDescriptor {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub output: ZFLinkId,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<ZFRuntimeID>,
}

impl std::fmt::Display for ZFSourceDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} - Kind: Source", self.id, self.name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorDescriptor {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub inputs: Vec<ZFLinkId>,
    pub outputs: Vec<ZFLinkId>,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<ZFRuntimeID>,
}

impl std::fmt::Display for ZFOperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} - Kind: Operator", self.id, self.name)
    }
}
