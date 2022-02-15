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
use crate::model::loops::LoopDescriptor;
use crate::types::{Configuration, NodeId, RuntimeId};
use crate::{DurationDescriptor, PortType};
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Descriptors

/// Describes a sink.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libgeneric_sink.so
/// configuration:
///   file: /tmp/generic-sink.txt
/// input:
///   id: Data
///   type: usize
/// ```
///
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

/// Describes a source.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libcounter_source.so
/// configuration:
///   start: 10
/// output:
///   id: Counter
///   type: usize
/// ```
///
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

/// Describes an operator.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libmy_op.so
/// configuration:
///   by: 10
/// inputs:
///     - id: Number
///       type: usize
/// outputs:
///   - id: Multiplied
///     type: usize
/// ```
///
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

/// A `SinkRecord` is an instance of a [`SinkDescriptor`](`SinkDescriptor`)
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
    /// Returns the `PortType` for the input.
    pub fn get_input_type(&self, id: &str) -> Option<PortType> {
        if self.input.port_id.as_ref() == id {
            Some(self.input.port_type.clone())
        } else {
            None
        }
    }
}

/// A `SourceRecord` is an instance of a [`SourceDescriptor`](`SourceDescriptor`)
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
    /// Returns the `PortType` for the output.
    pub fn get_output_type(&self, id: &str) -> Option<PortType> {
        if self.output.port_id.as_ref() == id {
            Some(self.output.port_type.clone())
        } else {
            None
        }
    }
}

/// An `OperatorRecord` is an instance of an [`OperatorDescriptor`](`OperatorDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperatorRecord {
    pub(crate) id: NodeId,
    pub(crate) inputs: Vec<PortDescriptor>,
    pub(crate) outputs: Vec<PortDescriptor>,
    pub(crate) uri: Option<String>,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) deadline: Option<Duration>,
    pub(crate) runtime: RuntimeId,
    // Ciclo is the italian word for "loop" — we cannot use "loop" as it’s a reserved keyword.
    pub(crate) ciclo: Option<LoopDescriptor>,
}

impl std::fmt::Display for OperatorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

impl OperatorRecord {
    /// Returns the `PortType` for the given output.
    pub fn get_output_type(&self, id: &str) -> Option<PortType> {
        self.outputs
            .iter()
            .find(|&lid| *lid.port_id == *id)
            .map(|lid| lid.port_type.clone())
    }

    /// Return the `PortType` for the given input.
    pub fn get_input_type(&self, id: &str) -> Option<PortType> {
        self.inputs
            .iter()
            .find(|&lid| *lid.port_id == *id)
            .map(|lid| lid.port_type.clone())
    }
}
