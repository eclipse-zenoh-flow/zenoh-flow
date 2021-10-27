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
#![allow(clippy::type_complexity)]

use crate::model::connector::{ZFConnectorKind, ZFConnectorRecord};
use crate::model::link::{LinkDescriptor, LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use crate::model::node::{
    OperatorDescriptor, OperatorRecord, SinkDescriptor, SinkRecord, SourceDescriptor, SourceRecord,
};
use crate::serde::{Deserialize, Serialize};
use crate::types::{NodeId, RuntimeId, ZFError, ZFResult};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<OperatorDescriptor>,
    pub sources: Vec<SourceDescriptor>,
    pub sinks: Vec<SinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<Vec<Mapping>>,
}

impl DataFlowDescriptor {
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<DataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<DataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn get_mapping(&self, id: &str) -> Option<RuntimeId> {
        match &self.mapping {
            Some(mapping) => mapping
                .iter()
                .find(|&o| o.id.as_ref() == id)
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

    pub fn get_runtimes(&self) -> Vec<RuntimeId> {
        let mut runtimes = HashSet::new();

        match &self.mapping {
            Some(mapping) => {
                for node_mapping in mapping.iter() {
                    runtimes.insert(node_mapping.runtime.clone());
                }
            }
            None => (),
        }
        runtimes.into_iter().collect()
    }

    fn validate(&self) -> ZFResult<()> {
        // The clippy::type_complexity raises because of this HashMap.
        let mut nodes: HashMap<NodeId, (HashMap<String, String>, HashMap<String, String>)> =
            HashMap::new();

        // Checks for duplicated operators
        for operator in &self.operators {
            if nodes.contains_key(&operator.id) {
                return Err(ZFError::DuplicatedNodeId(operator.id.clone()));
            }

            // Checks for duplicated ports
            let mut inputs: HashMap<String, String> = HashMap::new();
            let mut outputs: HashMap<String, String> = HashMap::new();
            for input in &operator.inputs {
                if inputs.contains_key(&input.port_id) {
                    return Err(ZFError::DuplicatedInputPort((
                        operator.id.clone(),
                        input.port_id.clone().into(),
                    )));
                }
                inputs.insert(input.port_id.clone(), input.port_type.clone());
            }

            for output in &operator.outputs {
                if outputs.contains_key(&output.port_id) {
                    return Err(ZFError::DuplicatedOutputPort((
                        operator.id.clone(),
                        output.port_id.clone().into(),
                    )));
                }
                outputs.insert(output.port_id.clone(), output.port_type.clone());
            }
            nodes.insert(operator.id.clone(), (inputs, outputs));
        }

        // Checks for duplicated sinks.
        for sink in &self.sinks {
            if nodes.contains_key(&sink.id) {
                return Err(ZFError::DuplicatedNodeId(sink.id.clone()));
            }
            let mut inputs = HashMap::new();
            inputs.insert(sink.input.port_id.clone(), sink.input.port_type.clone());
            nodes.insert(sink.id.clone(), (inputs, HashMap::new()));
        }

        // Checks for duplicated sources.
        for source in &self.sources {
            if nodes.contains_key(&source.id) {
                return Err(ZFError::DuplicatedNodeId(source.id.clone()));
            }
            let mut outputs = HashMap::new();
            outputs.insert(
                source.output.port_id.clone(),
                source.output.port_type.clone(),
            );
            nodes.insert(source.id.clone(), (HashMap::new(), outputs));
        }

        let mut inputs: HashMap<String, String> = HashMap::new();
        let mut outputs: HashMap<String, String> = HashMap::new();

        // Checks links
        for l in &self.links {
            // Checks if ports are already connected
            let from_str = format!("{}.{}", l.from.node, l.from.output);
            let to_str = format!("{}.{}", l.to.node, l.to.input);
            match (inputs.get(&from_str), outputs.get(&to_str)) {
                (None, None) => (),
                (Some(to), _) => {
                    return Err(ZFError::DuplicatedConnection((from_str, to.to_string())))
                }
                (None, Some(from)) => {
                    return Err(ZFError::DuplicatedConnection((to_str, from.to_string())))
                }
            };

            inputs.insert(from_str.clone(), to_str.clone());
            outputs.insert(to_str.clone(), from_str.clone());

            // Checks if the output port exists.
            let out_port_type = match nodes.get(&l.from.node) {
                Some((_, node_outputs)) => match node_outputs.get(&l.from.output) {
                    Some(out_type) => Ok(out_type),
                    None => Err(ZFError::PortNotFound((
                        l.from.node.clone(),
                        l.from.output.clone(),
                    ))),
                },
                None => Err(ZFError::OperatorNotFound(l.from.node.clone())),
            }?;

            // Checks if the input port exists.
            let in_port_type = match nodes.get(&l.to.node) {
                Some((node_inputs, _)) => match node_inputs.get(&l.to.input) {
                    Some(in_type) => Ok(in_type),
                    None => Err(ZFError::PortNotFound((
                        l.to.node.clone(),
                        l.to.input.clone(),
                    ))),
                },
                None => Err(ZFError::OperatorNotFound(l.to.node.clone())),
            }?;

            // Checks if the output type and input type matches.
            if in_port_type != out_port_type {
                return Err(ZFError::PortTypeNotMatching((
                    out_port_type.clone(),
                    in_port_type.clone(),
                )));
            }
        }

        // Checks for non connected ports
        for (node_id, (node_inputs, node_outputs)) in &nodes {
            for node_input in node_inputs.keys() {
                let to_str = format!("{}.{}", node_id, node_input);
                if !outputs.contains_key(&to_str) {
                    return Err(ZFError::PortNotConnected((
                        node_id.clone(),
                        node_input.clone().into(),
                    )));
                }
            }
            for node_output in node_outputs.keys() {
                let from_str = format!("{}.{}", node_id, node_output);
                if !inputs.contains_key(&from_str) {
                    return Err(ZFError::PortNotConnected((
                        node_id.clone(),
                        node_output.clone().into(),
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Hash for DataFlowDescriptor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.flow.hash(state);
    }
}

impl PartialEq for DataFlowDescriptor {
    fn eq(&self, other: &DataFlowDescriptor) -> bool {
        self.flow == other.flow
    }
}

impl Eq for DataFlowDescriptor {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mapping {
    pub id: NodeId,
    pub runtime: RuntimeId,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowRecord {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: Vec<OperatorRecord>,
    pub sinks: Vec<SinkRecord>,
    pub sources: Vec<SourceRecord>,
    pub connectors: Vec<ZFConnectorRecord>,
    pub links: Vec<LinkDescriptor>,
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

    pub fn find_node_output_type(&self, id: &str, output: &str) -> Option<String> {
        log::trace!("find_node_output_type({:?},{:?})", id, output);
        match self.get_operator(id) {
            Some(o) => o.get_output_type(output),
            None => match self.get_source(id) {
                Some(s) => s.get_output_type(output),
                None => None,
            },
        }
    }

    pub fn find_node_input_type(&self, id: &str, input: &str) -> Option<String> {
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
                        to: LinkToDescriptor {
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
                    from: LinkFromDescriptor {
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
        let mut dfr = DataFlowRecord {
            uuid: id,
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
                    let or = OperatorRecord {
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
