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

use crate::model::connector::{ZFConnectorRecord, ZFConnectorKind};
use crate::model::operator::{
    ZFOperatorDescriptor, ZFOperatorRecord, ZFSinkDescriptor, ZFSinkRecord, ZFSourceDescriptor,
    ZFSourceRecord,
};
use crate::model::link::{ZFFromEndpoint, ZFLinkDescriptor, ZFToEndpoint};
use crate::runtime::graph::node::DataFlowNode;
use crate::serde::{Deserialize, Serialize};
use crate::types::{ZFError, ZFOperatorId, ZFResult, ZFRuntimeID};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<ZFOperatorDescriptor>,
    pub sources: Vec<ZFSourceDescriptor>,
    pub sinks: Vec<ZFSinkDescriptor>,
    pub links: Vec<ZFLinkDescriptor>,
    pub mapping: Option<Vec<Mapping>>,
}

impl DataFlowDescriptor {
    pub fn from_yaml(data: String) -> ZFResult<Self> {
        Ok(serde_yaml::from_str::<DataFlowDescriptor>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?)
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

    fn get_mapping(&self, id: ZFOperatorId) -> Option<ZFRuntimeID> {
        match &self.mapping {
            Some(mapping) => match mapping.iter().find(|&o| o.id == id) {
                Some(m) => Some(m.runtime.clone()),
                None => None,
            },
            None => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mapping {
    pub id: ZFOperatorId,
    pub runtime: ZFRuntimeID,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowRecord {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: Vec<ZFOperatorRecord>,
    pub sinks: Vec<ZFSinkRecord>,
    pub sources: Vec<ZFSourceRecord>,
    pub connectors: Vec<ZFConnectorRecord>,
    pub links: Vec<ZFLinkDescriptor>,
}

impl DataFlowRecord {
    pub fn from_yaml(data: String) -> ZFResult<Self> {
        Ok(serde_yaml::from_str::<DataFlowRecord>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?)
    }

    pub fn from_json(data: String) -> ZFResult<Self> {
        Ok(serde_json::from_str::<DataFlowRecord>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?)
    }

    pub fn find_component_runtime(&self, id: &ZFOperatorId) -> Option<ZFRuntimeID> {
        match self.get_operator(id) {
            Some(o) => Some(o.runtime.clone()),
            None => match self.get_source(id) {
                Some(s) => Some(s.runtime.clone()),
                None => match self.get_sink(id) {
                    Some(s) => Some(s.runtime.clone()),
                    None => None,
                },
            },
        }
    }

    fn get_operator(&self, id: &ZFOperatorId) -> Option<ZFOperatorRecord> {
        match self.operators.iter().find(|&o| o.id == *id) {
            Some(o) => Some(o.clone()),
            None => None,
        }
    }

    fn get_source(&self, id: &ZFOperatorId) -> Option<ZFSourceRecord> {
        match self.sources.iter().find(|&o| o.id == *id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }

    fn get_sink(&self, id: &ZFOperatorId) -> Option<ZFSinkRecord> {
        match self.sinks.iter().find(|&o| o.id == *id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }

    fn get_connector(&self, id: &String) -> Option<ZFConnectorRecord> {
        match self.connectors.iter().find(|&o| o.id == *id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }
}

impl DataFlowRecord {
    fn from_dataflow_descriptor(d: DataFlowDescriptor) -> ZFResult<Self> {
        let mut dfr = DataFlowRecord {
            uuid: Uuid::nil(), // all 0s uuid, placeholder
            flow: d.flow.clone(),
            operators: Vec::new(),
            sinks: Vec::new(),
            sources: Vec::new(),
            connectors: Vec::new(),
            links: Vec::new(),
        };

        for o in &d.operators {
            match d.get_mapping(o.id.clone()) {
                Some(m) => {
                    let or = ZFOperatorRecord {
                        id: o.id.clone(),
                        name: o.name.clone(),
                        inputs: o.inputs.clone(),
                        outputs: o.outputs.clone(),
                        uri: o.uri.clone(),
                        configuration: o.configuration.clone(),
                        runtime: m,
                    };
                    dfr.operators.push(or)
                }
                None => return Err(ZFError::Uncompleted),
            }
        }

        for s in &d.sources {
            match d.get_mapping(s.id.clone()) {
                Some(m) => {
                    let sr = ZFSourceRecord {
                        id: s.id.clone(),
                        name: s.name.clone(),
                        output: s.output.clone(),
                        uri: s.uri.clone(),
                        configuration: s.configuration.clone(),
                        runtime: m,
                    };
                    dfr.sources.push(sr)
                }
                None => return Err(ZFError::Uncompleted),
            }
        }

        for s in &d.sinks {
            match d.get_mapping(s.id.clone()) {
                Some(m) => {
                    let sr = ZFSinkRecord {
                        id: s.id.clone(),
                        name: s.name.clone(),
                        input: s.input.clone(),
                        uri: s.uri.clone(),
                        configuration: s.configuration.clone(),
                        runtime: m,
                    };
                    dfr.sinks.push(sr)
                }
                None => return Err(ZFError::Uncompleted),
            }
        }

        for l in &d.links {
            if l.from.output != l.to.input {
                return Err(ZFError::PortIdNotMatching((l.from.output.clone(), l.to.input.clone())))
            }

            let from_runtime = match dfr.find_component_runtime(&l.from.name) {
                Some(rt) => rt,
                None => return Err(ZFError::Uncompleted),
            };

            let to_runtime = match dfr.find_component_runtime(&l.to.name) {
                Some(rt) => rt,
                None => return Err(ZFError::Uncompleted),
            };

            if from_runtime == to_runtime {
                // link between components on the same runtime
                dfr.links.push(l.clone())
            } else {
                // link between component on different runtime
                // here we have to create the connectors information
                // and add the new links


                //creating zenoh resource name
                let z_resource_name = format!("/zf/data/{}/{}/{}/{}",
                &dfr.flow, &dfr.uuid,&l.from.name, &l.from.output);

                //creating sender
                let sender_id = format!("sender-{}-{}-{}-{}", &dfr.flow, &dfr.uuid,&l.from.name, &l.from.output);
                let sender = ZFConnectorRecord {
                    kind : ZFConnectorKind::Sender,
                    id : sender_id.clone(),
                    resource : z_resource_name.clone(),
                    link_id: l.from.output.clone(),
                    runtime: from_runtime
                };


                //creating link between component and sender
                let link_sender = ZFLinkDescriptor {
                    from: l.from.clone(),
                    to: ZFToEndpoint {
                        name: sender_id,
                        input: sender.link_id.clone(),
                    },
                    size: None,
                    queueing_policy: None,
                    priority: None,
                };

                // storing info in the data flow record
                dfr.connectors.push(sender);
                dfr.links.push(link_sender);

                // creating receiver

                let receiver_id = format!("sender-{}-{}-{}-{}", &dfr.flow, &dfr.uuid,&l.to.name, &l.to.input);
                let receiver = ZFConnectorRecord {
                    kind : ZFConnectorKind::Receiver,
                    id : receiver_id.clone(),
                    resource : z_resource_name.clone(),
                    link_id: l.to.input.clone(),
                    runtime: to_runtime
                };

                //creating link between receiver and component
                let link_receiver = ZFLinkDescriptor {
                    from: ZFFromEndpoint {
                        name: receiver_id,
                        output: receiver.link_id.clone(),
                    },
                    to: l.to.clone(),
                    size: None,
                    queueing_policy: None,
                    priority: None,
                };

                // storing info in the data flow record
                dfr.connectors.push(receiver);
                dfr.links.push(link_receiver);

            }

        }
        Ok(dfr)
    }
}
