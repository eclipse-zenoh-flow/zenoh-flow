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

use crate::model::connector::{ZFConnectorKind, ZFConnectorRecord};
use crate::model::operator::{ZFOperatorRecord, ZFSinkRecord, ZFSourceRecord};
use crate::{ZFError, ZFOperatorId, ZFResult, ZFRuntimeID};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNodeKind {
    Operator,
    Source,
    Sink,
    Connector,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNode {
    Operator(ZFOperatorRecord),
    Source(ZFSourceRecord),
    Sink(ZFSinkRecord),
    Connector(ZFConnectorRecord),
}

impl std::fmt::Display for DataFlowNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataFlowNode::Operator(inner) => write!(f, "{}", inner),
            DataFlowNode::Source(inner) => write!(f, "{}", inner),
            DataFlowNode::Sink(inner) => write!(f, "{}", inner),
            DataFlowNode::Connector(inner) => write!(f, "{}", inner),
        }
    }
}

impl DataFlowNode {
    pub fn has_input(&self, id: String) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.inputs.iter().find(|&lid| *lid.port_id == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Source(_) => false,
            DataFlowNode::Sink(sink) => sink.input.port_id == id,
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => false,
                ZFConnectorKind::Sender => zc.link_id.port_id == id,
            },
        }
    }

    pub fn get_input_type(&self, id: String) -> ZFResult<&str> {
        match self {
            DataFlowNode::Operator(op) => match op.inputs.iter().find(|&lid| *lid.port_id == id) {
                Some(lid) => Ok(&lid.port_type),
                None => Err(ZFError::PortNotFound((self.get_id(), id))),
            },
            DataFlowNode::Source(_) => Err(ZFError::PortNotFound((self.get_id(), id))),
            DataFlowNode::Sink(sink) => {
                if sink.input.port_id == id {
                    Ok(&sink.input.port_type)
                } else {
                    Err(ZFError::PortNotFound((self.get_id(), id)))
                }
            }
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => Err(ZFError::PortNotFound((self.get_id(), id))),
                ZFConnectorKind::Sender => {
                    if zc.link_id.port_id == id {
                        Ok(&zc.link_id.port_type)
                    } else {
                        Err(ZFError::PortNotFound((self.get_id(), id)))
                    }
                }
            },
        }
    }

    pub fn has_output(&self, id: String) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.outputs.iter().find(|&lid| *lid.port_id == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Sink(_) => false,
            DataFlowNode::Source(source) => source.output.port_id == id,
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => zc.link_id.port_id == id,
                ZFConnectorKind::Sender => false,
            },
        }
    }

    pub fn get_output_type(&self, id: String) -> ZFResult<&str> {
        match self {
            DataFlowNode::Operator(op) => match op.outputs.iter().find(|&lid| *lid.port_id == id) {
                Some(lid) => Ok(&lid.port_type),
                None => Err(ZFError::PortNotFound((self.get_id(), id))),
            },
            DataFlowNode::Sink(_) => Err(ZFError::PortNotFound((self.get_id(), id))),
            DataFlowNode::Source(source) => {
                if source.output.port_id == id {
                    Ok(&source.output.port_type)
                } else {
                    Err(ZFError::PortNotFound((self.get_id(), id)))
                }
            }
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => {
                    if zc.link_id.port_id == id {
                        Ok(&zc.link_id.port_type)
                    } else {
                        Err(ZFError::PortNotFound((self.get_id(), id)))
                    }
                }
                ZFConnectorKind::Sender => Err(ZFError::PortNotFound((self.get_id(), id))),
            },
        }
    }

    pub fn get_id(&self) -> ZFOperatorId {
        match self {
            DataFlowNode::Operator(op) => op.id.clone(),
            DataFlowNode::Sink(s) => s.id.clone(),
            DataFlowNode::Source(s) => s.id.clone(),
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => zc.id.clone(),
                ZFConnectorKind::Sender => zc.id.clone(),
            },
        }
    }

    pub fn get_runtime(&self) -> ZFRuntimeID {
        match self {
            DataFlowNode::Operator(op) => op.runtime.clone(),
            DataFlowNode::Sink(s) => s.runtime.clone(),
            DataFlowNode::Source(s) => s.runtime.clone(),
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => zc.runtime.clone(),
                ZFConnectorKind::Sender => zc.runtime.clone(),
            },
        }
    }
}
