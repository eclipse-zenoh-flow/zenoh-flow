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

use crate::model::descriptor::{
    CompositeInputDescriptor, CompositeOutputDescriptor, LinkDescriptor, LoadedNode, PortDescriptor,
};
use crate::types::{Configuration, NodeId};
use crate::zfresult::{ErrorKind, ZFResult as Result};
use crate::{bail, zferror};
use serde::{Deserialize, Serialize};

use super::NodeDescriptor;

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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for SinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

impl LoadedNode for SinkDescriptor {
    fn from_parameters(
        id: NodeId,
        configuration: Option<Configuration>,
        uri: Option<String>,
        inputs: Option<Vec<PortDescriptor>>,
        _outputs: Option<Vec<PortDescriptor>>,
        _operators: Option<Vec<NodeDescriptor>>,
        _links: Option<Vec<LinkDescriptor>>,
        _composite_inputs: Option<Vec<CompositeInputDescriptor>>,
        _compisite_outpus: Option<Vec<CompositeOutputDescriptor>>,
    ) -> Result<Self> {
        match inputs {
            Some(inputs) =>{
                Ok(Self{
                    id,
                    configuration,
                    uri,
                    inputs,
                })
            },
            _ => bail!(ErrorKind::InvalidData, "Creating a SinkDescriptor requires: id, configuration, uri, inputs. Maybe some parameters are set as None?")
        }
    }

    fn get_id(&self) -> &NodeId {
        &self.id
    }

    fn set_id(&mut self, id: NodeId) {
        self.id = id
    }

    fn get_configuration(&self) -> &Option<Configuration> {
        &self.configuration
    }

    fn set_configuration(&mut self, configuration: Option<Configuration>) {
        self.configuration = configuration
    }

    fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SinkDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<SinkDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}
