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
use crate::model::dataflow::descriptor::DataFlowDescriptor;
use crate::model::deadline::E2EDeadlineRecord;
use crate::model::link::{LinkDescriptor, PortDescriptor};
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::serde::{Deserialize, Serialize};
use crate::types::{RuntimeId, ZFError, ZFResult};
use crate::PortType;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowRecord {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: Vec<OperatorRecord>,
    pub sinks: Vec<SinkRecord>,
    pub sources: Vec<SourceRecord>,
    pub connectors: Vec<ZFConnectorRecord>,
    pub links: Vec<LinkDescriptor>,
    pub end_to_end_deadlines: Option<Vec<E2EDeadlineRecord>>,
}

impl DataFlowRecord {
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        serde_yaml::from_str::<DataFlowRecord>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    pub fn from_json(data: &str) -> ZFResult<Self> {
        serde_json::from_str::<DataFlowRecord>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn find_node_runtime(&self, id: &str) -> Option<RuntimeId> {
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

    pub fn find_node_output_type(&self, id: &str, output: &str) -> Option<PortType> {
        log::trace!("find_node_output_type({:?},{:?})", id, output);
        match self.get_operator(id) {
            Some(o) => o.get_output_type(output),
            None => match self.get_source(id) {
                Some(s) => s.get_output_type(output),
                None => None,
            },
        }
    }

    pub fn find_node_input_type(&self, id: &str, input: &str) -> Option<PortType> {
        log::trace!("find_node_input_type({:?},{:?})", id, input);
        match self.get_operator(id) {
            Some(o) => o.get_input_type(input),
            None => match self.get_sink(id) {
                Some(s) => s.get_input_type(input),
                None => None,
            },
        }
    }

    fn get_operator(&self, id: &str) -> Option<OperatorRecord> {
        self.operators
            .iter()
            .find(|&o| o.id.as_ref() == id)
            .cloned()
    }

    fn get_source(&self, id: &str) -> Option<SourceRecord> {
        self.sources.iter().find(|&o| o.id.as_ref() == id).cloned()
    }

    fn get_sink(&self, id: &str) -> Option<SinkRecord> {
        self.sinks.iter().find(|&o| o.id.as_ref() == id).cloned()
    }

    fn add_links(&mut self, links: &[LinkDescriptor]) -> ZFResult<()> {
        for l in links {
            let from_runtime = match self.find_node_runtime(&l.from.node) {
                Some(rt) => rt,
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Unable to find runtime for {}",
                        &l.from.node
                    )))
                }
            };

            let to_runtime = match self.find_node_runtime(&l.to.node) {
                Some(rt) => rt,
                None => {
                    return Err(ZFError::Uncompleted(format!(
                        "Unable to find runtime for {}",
                        &l.to.node
                    )))
                }
            };

            let from_type = match self.find_node_output_type(&l.from.node, &l.from.output) {
                Some(t) => t,
                None => {
                    return Err(ZFError::PortNotFound((
                        l.from.node.clone(),
                        l.from.output.clone(),
                    )))
                }
            };

            let to_type = match self.find_node_input_type(&l.to.node, &l.to.input) {
                Some(t) => t,
                None => {
                    return Err(ZFError::PortNotFound((
                        l.to.node.clone(),
                        l.to.input.clone(),
                    )))
                }
            };

            if from_type != to_type {
                return Err(ZFError::PortTypeNotMatching((from_type, to_type)));
            }

            if from_runtime == to_runtime {
                // link between nodes on the same runtime
                self.links.push(l.clone())
            } else {
                // link between node on different runtime
                // here we have to create the connectors information
                // and add the new links

                // creating zenoh resource name
                let z_resource_name = format!(
                    "/zf/data/{}/{}/{}/{}",
                    &self.flow, &self.uuid, &l.from.node, &l.from.output
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
                        &self.flow, &self.uuid, &l.from.node, &l.from.output
                    );
                    let sender = ZFConnectorRecord {
                        kind: ZFConnectorKind::Sender,
                        id: sender_id.clone().into(),
                        resource: z_resource_name.clone(),
                        link_id: PortDescriptor {
                            port_id: l.from.output.clone(),
                            port_type: from_type,
                        },

                        runtime: from_runtime,
                    };

                    // creating link between node and sender
                    let link_sender = LinkDescriptor {
                        from: l.from.clone(),
                        to: InputDescriptor {
                            node: sender_id.into(),
                            input: l.from.output.clone(),
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
                    &self.flow, &self.uuid, &l.to.node, &l.to.input
                );
                let receiver = ZFConnectorRecord {
                    kind: ZFConnectorKind::Receiver,
                    id: receiver_id.clone().into(),
                    resource: z_resource_name.clone(),
                    link_id: PortDescriptor {
                        port_id: l.to.input.clone(),
                        port_type: to_type,
                    },

                    runtime: to_runtime,
                };

                // Creating link between receiver and node
                let link_receiver = LinkDescriptor {
                    from: OutputDescriptor {
                        node: receiver_id.into(),
                        output: l.to.input.clone(),
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
}

impl TryFrom<(DataFlowDescriptor, Uuid)> for DataFlowRecord {
    type Error = ZFError;

    fn try_from(d: (DataFlowDescriptor, Uuid)) -> Result<Self, Self::Error> {
        let (d, id) = d;

        let deadlines = d
            .deadlines
            .clone()
            .map(|deadlines_desc| deadlines_desc.into_iter().map(|desc| desc.into()).collect());

        let mut dfr = DataFlowRecord {
            uuid: id,
            flow: d.flow.clone(),
            operators: Vec::new(),
            sinks: Vec::new(),
            sources: Vec::new(),
            connectors: Vec::new(),
            links: Vec::new(),
            end_to_end_deadlines: deadlines,
        };

        for o in &d.operators {
            match d.get_mapping(&o.id) {
                Some(m) => {
                    let or = OperatorRecord {
                        id: o.id.clone(),
                        inputs: o.inputs.clone(),
                        outputs: o.outputs.clone(),
                        uri: o.uri.clone(),
                        configuration: o.configuration.clone(),
                        runtime: m,
                        deadline: o.deadline.as_ref().map(|period| period.to_duration()),
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
                    let sr = SourceRecord {
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
                    let sr = SinkRecord {
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

impl Hash for DataFlowRecord {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
        self.flow.hash(state);
    }
}

impl PartialEq for DataFlowRecord {
    fn eq(&self, other: &DataFlowRecord) -> bool {
        self.uuid == other.uuid && self.flow == other.flow
    }
}

impl Eq for DataFlowRecord {}
