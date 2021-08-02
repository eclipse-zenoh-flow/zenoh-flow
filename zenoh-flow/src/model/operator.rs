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
use crate::model::period::ZFPeriodDescriptor;
use crate::types::{ZFOperatorId, ZFRuntimeID};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Descriptors

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSinkDescriptor {
    pub id: ZFOperatorId,
    pub input: ZFPortDescriptor,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<ZFRuntimeID>, // to be removed
}

impl std::fmt::Display for ZFSinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSourceDescriptor {
    pub id: ZFOperatorId,
    pub output: ZFPortDescriptor,
    pub period: Option<ZFPeriodDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<ZFRuntimeID>, // to be removed
}

impl std::fmt::Display for ZFSourceDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorDescriptor {
    pub id: ZFOperatorId,
    pub inputs: Vec<ZFPortDescriptor>,
    pub outputs: Vec<ZFPortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: Option<ZFRuntimeID>, // to be removed
}

impl std::fmt::Display for ZFOperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

// Records

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSinkRecord {
    pub id: ZFOperatorId,
    pub input: ZFPortDescriptor,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: ZFRuntimeID,
}

impl std::fmt::Display for ZFSinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

impl ZFSinkRecord {
    pub fn get_input_type(&self, id: &str) -> Option<String> {
        if self.input.port_id == *id {
            Some(self.input.port_type.clone())
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSourceRecord {
    pub id: ZFOperatorId,
    pub output: ZFPortDescriptor,
    pub period: Option<ZFPeriodDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: ZFRuntimeID,
}

impl std::fmt::Display for ZFSourceRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

impl ZFSourceRecord {
    pub fn get_output_type(&self, id: &str) -> Option<String> {
        if self.output.port_id == *id {
            Some(self.output.port_type.clone())
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorRecord {
    pub id: ZFOperatorId,
    pub inputs: Vec<ZFPortDescriptor>,
    pub outputs: Vec<ZFPortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<HashMap<String, String>>,
    pub runtime: ZFRuntimeID,
}

impl std::fmt::Display for ZFOperatorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

impl ZFOperatorRecord {
    pub fn get_output_type(&self, id: &str) -> Option<String> {
        self.outputs
            .iter()
            .find(|&lid| *lid.port_id == *id)
            .map(|lid| lid.port_type.clone())
    }

    pub fn get_input_type(&self, id: &str) -> Option<String> {
        self.inputs
            .iter()
            .find(|&lid| *lid.port_id == *id)
            .map(|lid| lid.port_type.clone())
    }
}
