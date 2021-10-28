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

use std::collections::HashMap;
use std::convert::TryFrom;

use crate::model::connector::{ZFConnectorKind, ZFConnectorRecord};
use crate::model::link::PortDescriptor;
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::model::period::PeriodDescriptor;
use crate::runtime::dataflow::loader::{load_operator, load_sink, load_source};
use crate::{NodeId, Operator, PortId, RuntimeId, Sink, Source, State, ZFError, ZFResult};
use async_std::sync::{Arc, RwLock};
use libloading::Library;
use serde::{Deserialize, Serialize};

pub struct SourceLoaded {
    pub(crate) id: NodeId,
    pub(crate) output: PortDescriptor,
    pub(crate) period: Option<PeriodDescriptor>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) library: Option<Arc<Library>>,
}

impl TryFrom<SourceRecord> for SourceLoaded {
    type Error = ZFError;

    fn try_from(value: SourceRecord) -> Result<Self, Self::Error> {
        let uri = value.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Source < {} >.",
                value.id.clone()
            ))
        })?;
        let (library, source) = load_source(uri)?;
        let state = source.initialize(&value.configuration)?;

        Ok(Self {
            id: value.id,
            output: value.output,
            period: value.period,
            state: Arc::new(RwLock::new(state)),
            source,
            library: Some(Arc::new(library)),
        })
    }
}

pub struct OperatorLoaded {
    pub(crate) id: NodeId,
    pub(crate) inputs: HashMap<PortId, String>,
    pub(crate) outputs: HashMap<PortId, String>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) library: Option<Arc<Library>>,
}

impl TryFrom<OperatorRecord> for OperatorLoaded {
    type Error = ZFError;

    fn try_from(value: OperatorRecord) -> Result<Self, Self::Error> {
        let uri = value.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Operator < {} >.",
                value.id.clone()
            ))
        })?;
        let (library, operator) = load_operator(uri)?;
        let state = operator.initialize(&value.configuration)?;

        let inputs: HashMap<PortId, String> = value
            .inputs
            .into_iter()
            .map(|desc| (desc.port_id.into(), desc.port_type))
            .collect();

        let outputs: HashMap<PortId, String> = value
            .outputs
            .into_iter()
            .map(|desc| (desc.port_id.into(), desc.port_type))
            .collect();

        Ok(Self {
            id: value.id,
            inputs,
            outputs,
            state: Arc::new(RwLock::new(state)),
            operator,
            library: Some(Arc::new(library)),
        })
    }
}

pub struct SinkLoaded {
    pub(crate) id: NodeId,
    pub(crate) input: PortDescriptor,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) library: Option<Arc<Library>>,
}

impl TryFrom<SinkRecord> for SinkLoaded {
    type Error = ZFError;

    fn try_from(value: SinkRecord) -> Result<Self, Self::Error> {
        let uri = value.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Sink < {} >.",
                value.id.clone()
            ))
        })?;
        let (library, sink) = load_sink(uri)?;
        let state = sink.initialize(&value.configuration)?;

        Ok(Self {
            id: value.id,
            input: value.input,
            state: Arc::new(RwLock::new(state)),
            sink,
            library: Some(Arc::new(library)),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNodeKind {
    Operator,
    Source,
    Sink,
    Connector,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNode {
    Operator(OperatorRecord),
    Source(SourceRecord),
    Sink(SinkRecord),
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

    pub fn get_id(&self) -> NodeId {
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

    pub fn get_runtime(&self) -> RuntimeId {
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
