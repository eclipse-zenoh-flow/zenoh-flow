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

use crate::model::link::PortDescriptor;
use crate::types::{Configuration, NodeId, RuntimeId};
use crate::{DurationDescriptor, PortType};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Descriptors

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkDescriptor {
    pub id: NodeId,
    pub input: PortDescriptor,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: Option<RuntimeId>, // to be removed
}

impl std::fmt::Display for SinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceDescriptor {
    pub id: NodeId,
    pub output: PortDescriptor,
    pub period: Option<DurationDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: Option<RuntimeId>, // to be removed
}

impl std::fmt::Display for SourceDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperatorDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub deadline: Option<DurationDescriptor>,
    pub runtime: Option<RuntimeId>, // to be removed
}

impl std::fmt::Display for OperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

// Records

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkRecord {
    pub id: NodeId,
    pub input: PortDescriptor,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for SinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

impl SinkRecord {
    pub fn get_input_type(&self, id: &str) -> Option<PortType> {
        if self.input.port_id.as_ref() == id {
            Some(self.input.port_type.clone())
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceRecord {
    pub id: NodeId,
    pub output: PortDescriptor,
    pub period: Option<DurationDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for SourceRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

impl SourceRecord {
    pub fn get_output_type(&self, id: &str) -> Option<PortType> {
        if self.output.port_id.as_ref() == id {
            Some(self.output.port_type.clone())
        } else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperatorRecord {
    pub(crate) id: NodeId,
    pub(crate) inputs: Vec<PortDescriptor>,
    pub(crate) outputs: Vec<PortDescriptor>,
    pub(crate) uri: Option<String>,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) deadline: Option<Duration>,
    pub(crate) runtime: RuntimeId,
}

impl std::fmt::Display for OperatorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

impl OperatorRecord {
    pub fn get_output_type(&self, id: &str) -> Option<PortType> {
        self.outputs
            .iter()
            .find(|&lid| *lid.port_id == *id)
            .map(|lid| lid.port_type.clone())
    }

    pub fn get_input_type(&self, id: &str) -> Option<PortType> {
        self.inputs
            .iter()
            .find(|&lid| *lid.port_id == *id)
            .map(|lid| lid.port_type.clone())
    }
}
