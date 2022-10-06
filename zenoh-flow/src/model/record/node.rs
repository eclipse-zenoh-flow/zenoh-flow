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
use crate::types::{Configuration, NodeId, PortType, RuntimeId};
use serde::{Deserialize, Serialize};

/// A `SinkRecord` is an instance of a [`SinkDescriptor`](`SinkDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkRecord {
    pub id: NodeId,
    pub uid: usize,
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

impl SinkRecord {
    /// Returns the `PortType` for the input.
    pub fn get_input_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.inputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }
}

/// A `SourceRecord` is an instance of a [`SourceDescriptor`](`SourceDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceRecord {
    pub id: NodeId,
    pub uid: usize,
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

impl SourceRecord {
    /// Returns the `PortType` for the output.
    pub fn get_output_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.outputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }
}

/// An `OperatorRecord` is an instance of an [`OperatorDescriptor`](`OperatorDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperatorRecord {
    pub id: NodeId,
    pub uid: usize,
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

impl OperatorRecord {
    /// Returns the `PortType` for the given output.
    pub fn get_output_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.outputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }

    /// Return the `PortType` for the given input.
    pub fn get_input_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.inputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }
}
