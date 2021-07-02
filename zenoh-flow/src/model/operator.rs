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

use crate::types::{ZFLinkId, ZFOperatorId, ZFOperatorName};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSinkDescription {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub input: ZFLinkId,
    pub lib: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<String>,
}

impl std::fmt::Display for ZFSinkDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} - Kind: Sink", self.id, self.name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSourceDescription {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub output: ZFLinkId,
    pub lib: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<String>,
}

impl std::fmt::Display for ZFSourceDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} - Kind: Source", self.id, self.name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorDescription {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub inputs: Vec<ZFLinkId>,
    pub outputs: Vec<ZFLinkId>,
    pub lib: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<String>,
}

impl std::fmt::Display for ZFOperatorDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} - Kind: Operator", self.id, self.name)
    }
}
