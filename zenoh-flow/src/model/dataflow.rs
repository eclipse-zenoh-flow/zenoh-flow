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
use crate::model::link::{
    ZFLinkDescriptor, ZFLinkFromDescriptor, ZFLinkToDescriptor, ZFPortDescriptor,
};
use crate::model::operator::{
    ZFOperatorDescriptor, ZFOperatorRecord, ZFSinkDescriptor, ZFSinkRecord, ZFSourceDescriptor,
    ZFSourceRecord,
};
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
        serde_yaml::from_str::<DataFlowDescriptor>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    pub fn from_json(data: String) -> ZFResult<Self> {
        serde_json::from_str::<DataFlowDescriptor>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    fn _get_operator(&self, id: ZFOperatorId) -> Option<ZFOperatorDescriptor> {
        self.operators.iter().find(|&o| o.id == id).cloned()
    }

    fn _get_source(&self, id: ZFOperatorId) -> Option<ZFSourceDescriptor> {
        self.sources.iter().find(|&o| o.id == id).cloned()
    }

    fn _get_sink(&self, id: ZFOperatorId) -> Option<ZFSinkDescriptor> {
        self.sinks.iter().find(|&o| o.id == id).cloned()
    }

    pub fn get_mapping(&self, id: &str) -> Option<ZFRuntimeID> {
        match &self.mapping {
            Some(mapping) => mapping
                .iter()
                .find(|&o| o.id == *id)
                .map(|m| m.runtime.clone()),
            None => None,
        }
    }

    pub fn add_mapping(&mut self, mapping: Mapping) {
        match self.mapping.as_mut() {
            Some(m) => m.push(mapping),
            None => self.mapping = Some(vec![mapping]),
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
        serde_yaml::from_str::<DataFlowRecord>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    pub fn from_json(data: String) -> ZFResult<Self> {
        serde_json::from_str::<DataFlowRecord>(&data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn find_component_runtime(&self, id: &str) -> Option<ZFRuntimeID> {
        match self.get_operator(id) {
            Some(o) => Some(o.runtime),
            None => match self.get_source(id) {
                Some(s) => Some(s.runtime),
                None => match self.get_sink(id) {
                    Some(s) => Some(s.runtime),
                    None => None,
                },
            },
        }
    }

    pub fn find_component_output_type(&self, id: &str, output: &str) -> Option<String> {
        log::trace!("find_component_output_type({:?},{:?})", id, output);
        match self.get_operator(id) {
            Some(o) => o.get_output_type(output),
            None => match self.get_source(id) {
                Some(s) => s.get_output_type(output),
                None => None,
            },
        }
    }

    pub fn find_component_input_type(&self, id: &str, input: &str) -> Option<String> {
        log::trace!("find_component_input_type({:?},{:?})", id, input);
        match self.get_operator(id) {
            Some(o) => o.get_input_type(input),
            None => match self.get_sink(id) {
                Some(s) => s.get_input_type(input),
                None => None,
            },
        }
    }

    pub fn find_node(&self, id: &str) -> Option<DataFlowNode> {
        match self.get_operator(id) {
            Some(o) => Some(DataFlowNode::Operator(o)),
            None => match self.get_source(id) {
                Some(s) => Some(DataFlowNode::Source(s)),
                None => match self.get_sink(id) {
                    Some(s) => Some(DataFlowNode::Sink(s)),
                    None => self.get_connector(id).map(DataFlowNode::Connector),
                },
            },
        }
    }

    fn get_operator(&self, id: &str) -> Option<ZFOperatorRecord> {
        self.operators.iter().find(|&o| o.id == *id).cloned()
    }

    fn get_source(&self, id: &str) -> Option<ZFSourceRecord> {
        self.sources.iter().find(|&o| o.id == *id).cloned()
    }

    fn get_sink(&self, id: &str) -> Option<ZFSinkRecord> {
        self.sinks.iter().find(|&o| o.id == *id).cloned()
    }

    fn get_connector(&self, id: &str) -> Option<ZFConnectorRecord> {
        self.connectors.iter().find(|&o| o.id == *id).cloned()
    }

    fn add_links(&mut self, links: &[ZFLinkDescriptor]) -> ZFResult<()> {
        for l in links {
            // if l.from.output != l.to.input {
            //     return Err(ZFError::PortIdNotMatching((
            //         l.from.output.clone(),
            //         l.to.input.clone(),
            //     )));
            // }

            let from_runtime = match self.find_component_runtime(&l.from.component_id) {
                Some(rt) => rt,
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Unable to find runtime for {}",
                        &l.from.component_id
                    )))
                }
            };

            let to_runtime = match self.find_component_runtime(&l.to.component_id) {
                Some(rt) => rt,
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Unable to find runtime for {}",
                        &l.to.component_id
                    )))
                }
            };

            let from_type =
                match self.find_component_output_type(&l.from.component_id, &l.from.output_id) {
                    Some(t) => t,
                    None => {
                        return Err(ZFError::PortNotFound((
                            l.from.component_id.clone(),
                            l.from.output_id.clone(),
                        )))
                    }
                };

            let to_type = match self.find_component_input_type(&l.to.component_id, &l.to.input_id) {
                Some(t) => t,
                None => {
                    return Err(ZFError::PortNotFound((
                        l.to.component_id.clone(),
                        l.to.input_id.clone(),
                    )))
                }
            };

            if from_type != to_type {
                return Err(ZFError::PortTypeNotMatching((from_type, to_type)));
            }

            if from_runtime == to_runtime {
                // link between components on the same runtime
                self.links.push(l.clone())
            } else {
                // link between component on different runtime
                // here we have to create the connectors information
                // and add the new links

                // creating zenoh resource name
                let z_resource_name = format!(
                    "/zf/data/{}/{}/{}/{}",
                    &self.flow, &self.uuid, &l.from.component_id, &l.from.output_id
                );

                // We only create a sender if none was created for the same resource. The rationale
                // is to avoid creating multiple publisher for the same resource in case an operator
                // acts as a multiplexor.
                if !self
                    .connectors
                    .iter()
                    .any(|c| c.kind == ZFConnectorKind::Sender && c.resource == z_resource_name)
                {
                    // creating sender
                    let sender_id = format!(
                        "sender-{}-{}-{}-{}",
                        &self.flow, &self.uuid, &l.from.component_id, &l.from.output_id
                    );
                    let sender = ZFConnectorRecord {
                        kind: ZFConnectorKind::Sender,
                        id: sender_id.clone(),
                        resource: z_resource_name.clone(),
                        link_id: ZFPortDescriptor {
                            port_id: l.from.output_id.clone(),
                            port_type: from_type,
                        },

                        runtime: from_runtime,
                    };

                    // creating link between component and sender
                    let link_sender = ZFLinkDescriptor {
                        from: l.from.clone(),
                        to: ZFLinkToDescriptor {
                            component_id: sender_id,
                            input_id: l.from.output_id.clone(),
                        },
                        size: None,
                        queueing_policy: None,
                        priority: None,
                    };

                    // storing info in the dataflow record
                    self.connectors.push(sender);
                    self.links.push(link_sender);
                }

                // creating receiver
                let receiver_id = format!(
                    "receiver-{}-{}-{}-{}",
                    &self.flow, &self.uuid, &l.to.component_id, &l.to.input_id
                );
                let receiver = ZFConnectorRecord {
                    kind: ZFConnectorKind::Receiver,
                    id: receiver_id.clone(),
                    resource: z_resource_name.clone(),
                    link_id: ZFPortDescriptor {
                        port_id: l.to.input_id.clone(),
                        port_type: to_type,
                    },

                    runtime: to_runtime,
                };

                //creating link between receiver and component
                let link_receiver = ZFLinkDescriptor {
                    from: ZFLinkFromDescriptor {
                        component_id: receiver_id,
                        output_id: l.to.input_id.clone(),
                    },
                    to: l.to.clone(),
                    size: None,
                    queueing_policy: None,
                    priority: None,
                };

                // storing info in the data flow record
                self.connectors.push(receiver);
                self.links.push(link_receiver);
            }
        }

        Ok(())
    }

    pub fn from_dataflow_descriptor(d: DataFlowDescriptor) -> ZFResult<Self> {
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
            match d.get_mapping(&o.id) {
                Some(m) => {
                    let or = ZFOperatorRecord {
                        id: o.id.clone(),
                        // name: o.name.clone(),
                        inputs: o.inputs.clone(),
                        outputs: o.outputs.clone(),
                        uri: o.uri.clone(),
                        configuration: o.configuration.clone(),
                        runtime: m,
                    };
                    dfr.operators.push(or)
                }
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Missing mapping for {}",
                        o.id.clone()
                    )))
                }
            }
        }

        for s in &d.sources {
            match d.get_mapping(&s.id) {
                Some(m) => {
                    let sr = ZFSourceRecord {
                        id: s.id.clone(),
                        period: s.period.clone(),
                        output: s.output.clone(),
                        uri: s.uri.clone(),
                        configuration: s.configuration.clone(),
                        runtime: m,
                    };
                    dfr.sources.push(sr)
                }
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Missing mapping for {}",
                        s.id.clone()
                    )))
                }
            }
        }

        for s in &d.sinks {
            match d.get_mapping(&s.id) {
                Some(m) => {
                    let sr = ZFSinkRecord {
                        id: s.id.clone(),
                        // name: s.name.clone(),
                        input: s.input.clone(),
                        uri: s.uri.clone(),
                        configuration: s.configuration.clone(),
                        runtime: m,
                    };
                    dfr.sinks.push(sr)
                }
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Missing mapping for {}",
                        s.id.clone()
                    )))
                }
            }
        }

        dfr.add_links(&d.links)?;
        Ok(dfr)
    }
}
