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

use crate::runtime::graph::node::DataFlowNode;
use crate::model::link::ZFLinkDescriptor;
use crate::model::operator::{ZFOperatorDescriptor, ZFSinkDescriptor, ZFSourceDescriptor};
use crate::types::ZFOperatorId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<ZFOperatorDescriptor>,
    pub sources: Vec<ZFSourceDescriptor>,
    pub sinks: Vec<ZFSinkDescriptor>,
    pub links: Vec<ZFLinkDescriptor>,
}

impl DataFlowDescriptor {
    pub fn from_yaml(data: String) -> Self {
        serde_yaml::from_str::<DataFlowDescriptor>(&data).unwrap()
    }

    pub fn find_node(&self, id: ZFOperatorId) -> Option<DataFlowNode> {
        match self.get_operator(id.clone()) {
            Some(o) => Some(DataFlowNode::Operator(o)),
            None => match self.get_source(id.clone()) {
                Some(s) => Some(DataFlowNode::Source(s)),
                None => match self.get_sink(id) {
                    Some(s) => Some(DataFlowNode::Sink(s)),
                    None => None,
                },
            },
        }
    }

    fn get_operator(&self, id: ZFOperatorId) -> Option<ZFOperatorDescriptor> {
        match self.operators.iter().find(|&o| o.id == id) {
            Some(o) => Some(o.clone()),
            None => None,
        }
    }

    fn get_source(&self, id: ZFOperatorId) -> Option<ZFSourceDescriptor> {
        match self.sources.iter().find(|&o| o.id == id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }

    fn get_sink(&self, id: ZFOperatorId) -> Option<ZFSinkDescriptor> {
        match self.sinks.iter().find(|&o| o.id == id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }
}
