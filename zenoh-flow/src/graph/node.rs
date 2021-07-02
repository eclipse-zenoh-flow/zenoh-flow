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

use crate::{
    ZFLinkId, ZFOperatorDescription, ZFOperatorId, ZFOperatorName, ZFSinkDescription,
    ZFSourceDescription, ZFZenohConnectorDescription,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNode {
    Operator(ZFOperatorDescription),
    Source(ZFSourceDescription),
    Sink(ZFSinkDescription),
    Connector(ZFZenohConnectorDescription),
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
            DataFlowNode::Connector(zc) => match zc {
                ZFZenohConnectorDescription::Receiver(_) => false,
                ZFZenohConnectorDescription::Sender(tx) => tx.input == id,
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
            DataFlowNode::Connector(zc) => match zc {
                ZFZenohConnectorDescription::Receiver(rx) => rx.output == id,
                ZFZenohConnectorDescription::Sender(_) => false,
            },
        }
    }

    pub fn get_id(&self) -> ZFOperatorId {
        match self {
            DataFlowNode::Operator(op) => op.id.clone(),
            DataFlowNode::Sink(s) => s.id.clone(),
            DataFlowNode::Source(s) => s.id.clone(),
            DataFlowNode::Connector(zc) => zc.get_id(),
        }
    }

    pub fn get_name(&self) -> ZFOperatorName {
        match self {
            DataFlowNode::Operator(op) => op.name.clone(),
            DataFlowNode::Sink(s) => s.name.clone(),
            DataFlowNode::Source(s) => s.name.clone(),
            DataFlowNode::Connector(zc) => zc.get_name(),
        }
    }

    pub fn get_runtime(&self) -> Option<String> {
        match self {
            DataFlowNode::Operator(op) => op.runtime.clone(),
            DataFlowNode::Sink(s) => s.runtime.clone(),
            DataFlowNode::Source(s) => s.runtime.clone(),
            DataFlowNode::Connector(zc) => zc.get_runtime(),
        }
    }
}
