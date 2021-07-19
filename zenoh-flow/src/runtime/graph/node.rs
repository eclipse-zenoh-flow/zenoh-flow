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
use crate::{ZFLinkId, ZFOperatorId, ZFRuntimeID};
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
    pub fn has_input(&self, id: ZFLinkId) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.inputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Source(_) => false,
            DataFlowNode::Sink(sink) => sink.input == id,
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => false,
                ZFConnectorKind::Sender => zc.link_id == id,
            },
        }
    }

    pub fn has_output(&self, id: ZFLinkId) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.outputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Sink(_) => false,
            DataFlowNode::Source(source) => source.output == id,
            DataFlowNode::Connector(zc) => match zc.kind {
                ZFConnectorKind::Receiver => zc.link_id == id,
                ZFConnectorKind::Sender => false,
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
