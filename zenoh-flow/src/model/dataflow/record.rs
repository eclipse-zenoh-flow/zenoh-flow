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

use crate::error::ZFError;
use crate::model::connector::{ZFConnectorKind, ZFConnectorRecord};
use crate::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use crate::model::link::{LinkDescriptor, LinkRecord, PortRecord};
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::serde::{Deserialize, Serialize};
use crate::types::{
    merge_configurations, NodeId, PortId, PortType, RuntimeId, ZFResult, PORT_TYPE_ANY,
};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// A `DataFlowRecord` is an instance of a [`FlattenDataFlowDescriptor`](`FlattenDataFlowDescriptor`).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowRecord {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: HashMap<NodeId, OperatorRecord>,
    pub sinks: HashMap<NodeId, SinkRecord>,
    pub sources: HashMap<NodeId, SourceRecord>,
    pub connectors: Vec<ZFConnectorRecord>,
    pub links: Vec<LinkRecord>,
    pub counter: u32,
}

impl DataFlowRecord {
    /// Creates a new `DataFlowRecord` record from its YAML format.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        serde_yaml::from_str::<DataFlowRecord>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    /// Creates a new `DataFlowRecord` from its JSON format.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        serde_json::from_str::<DataFlowRecord>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }

    /// Returns the JSON representation of the `DataFlowRecord`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `DataFlowRecord`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the runtime mapping for the given node.
    pub fn find_node_runtime(&self, id: &str) -> Option<RuntimeId> {
        match self.operators.get(id) {
            Some(o) => Some(o.runtime.clone()),
            None => match self.sources.get(id) {
                Some(s) => Some(s.runtime.clone()),
                None => self.sinks.get(id).map(|s| s.runtime.clone()),
            },
        }
    }

    /// Returns the output type for the given node and port.
    pub fn find_node_output_type(&self, id: &str, output: &str) -> Option<&PortType> {
        log::trace!("find_node_output_type({:?},{:?})", id, output);
        match self.operators.get(id) {
            Some(o) => o.get_output_type(output),
            None => match self.sources.get(id) {
                Some(s) => s.get_output_type(output),
                None => None,
            },
        }
    }

    /// Returns the input type for the given node and port.
    pub fn find_node_input_type(&self, id: &str, input: &str) -> Option<&PortType> {
        log::trace!("find_node_input_type({:?},{:?})", id, input);
        match self.operators.get(id) {
            Some(o) => o.get_input_type(input),
            None => match self.sinks.get(id) {
                Some(s) => s.get_input_type(input),
                None => None,
            },
        }
    }

    /// Finds the uid of the given node.
    fn find_node_uid_by_id(&self, id: &NodeId) -> Option<u32> {
        if let Some(o) = self.operators.get(id) {
            return Some(o.uid);
        }
        if let Some(s) = self.sources.get(id) {
            return Some(s.uid);
        }
        if let Some(s) = self.sinks.get(id) {
            return Some(s.uid);
        }
        None
    }

    /// Find the port uid for the given couple of node, port.
    fn find_port_id_in_node(&self, node_id: &NodeId, port_id: &PortId) -> Option<u32> {
        let (inputs, outputs) = if let Some(op) = self.operators.get(node_id) {
            (Some(&op.inputs), Some(&op.outputs))
        } else if let Some(source) = self.sources.get(node_id) {
            (None, Some(&source.outputs))
        } else if let Some(sink) = self.sinks.get(node_id) {
            (Some(&sink.inputs), None)
        } else {
            (None, None)
        };

        if let Some(outputs) = outputs {
            if let Some(port_record) = outputs.iter().find(|&output| output.port_id == *port_id) {
                return Some(port_record.uid);
            }
        }

        if let Some(inputs) = inputs {
            if let Some(port_record) = inputs.iter().find(|&input| input.port_id == *port_id) {
                return Some(port_record.uid);
            }
        }

        None
    }

    /// Adds the links.
    ///
    /// If the nodes are mapped to different machines it adds the couple of
    /// connectors in between and creates the unique key expression
    /// for the data to flow in Zenoh.
    ///
    ///  # Errors
    /// A variant error is returned if validation fails.
    fn add_links(
        &mut self,
        links: &[LinkDescriptor],
        nodes_to_remove: &HashSet<NodeId>,
    ) -> ZFResult<()> {
        for l in links.iter().filter(|&l| {
            !nodes_to_remove.contains(&l.from.node) && !nodes_to_remove.contains(&l.to.node)
        }) {
            log::debug!("Adding link: {:?}…", l);
            let from_runtime = match self.find_node_runtime(&l.from.node) {
                Some(rt) => rt,
                None => {
                    log::error!("Could not find runtime for: {:?}", &l.from.node);
                    return Err(ZFError::Uncompleted(format!(
                        "Unable to find runtime for {}",
                        &l.from.node
                    )));
                }
            };

            let to_runtime = match self.find_node_runtime(&l.to.node) {
                Some(rt) => rt,
                None => {
                    log::error!("Could not find runtime for: {:?}", &l.to.node);
                    return Err(ZFError::Uncompleted(format!(
                        "Unable to find runtime for {}",
                        &l.to.node
                    )));
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
                Some(t) => t.clone(),
                None => {
                    return Err(ZFError::PortNotFound((
                        l.to.node.clone(),
                        l.to.input.clone(),
                    )))
                }
            };

            if from_type != &to_type
                && from_type.as_ref() != PORT_TYPE_ANY
                && to_type.as_ref() != PORT_TYPE_ANY
            {
                return Err(ZFError::PortTypeNotMatching((from_type.clone(), to_type)));
            }

            if from_runtime == to_runtime {
                log::debug!("Adding link: {:?}… OK", l);
                // link between nodes on the same runtime
                self.links.push((l.clone(), self.counter).into());
                self.counter += 1;
            } else {
                // link between node on different runtime
                // here we have to create the connectors information
                // and add the new links

                let from_uid = self
                    .find_node_uid_by_id(&l.from.node)
                    .ok_or(ZFError::NotFound)?;
                let from_port_uid = self
                    .find_port_id_in_node(&l.from.node, &l.from.output)
                    .ok_or(ZFError::NotFound)?;

                // creating zenoh resource name
                let z_resource_name = format!(
                    "/zf/data/{}/{}/{}/{}",
                    &self.flow, &self.uuid, &from_uid, &from_port_uid
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
                        link_id: PortRecord {
                            uid: self.counter,
                            port_id: l.from.output.clone(),
                            port_type: from_type.clone(),
                        },

                        runtime: from_runtime,
                    };
                    self.counter += 1;

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
                    self.links.push((link_sender, self.counter).into());
                    self.counter += 1;
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
                    link_id: PortRecord {
                        uid: self.counter,
                        port_id: l.to.input.clone(),
                        port_type: to_type.clone(),
                    },

                    runtime: to_runtime,
                };
                self.counter += 1;

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
                self.links.push((link_receiver, self.counter).into());
                self.counter += 1;
            }
        }

        Ok(())
    }
}

impl TryFrom<(FlattenDataFlowDescriptor, Uuid)> for DataFlowRecord {
    type Error = ZFError;

    fn try_from(d: (FlattenDataFlowDescriptor, Uuid)) -> Result<Self, Self::Error> {
        let (dataflow, id) = d;

        let FlattenDataFlowDescriptor {
            flow,
            operators,
            sources,
            sinks,
            links,
            mapping,
            global_configuration,
            flags,
        } = dataflow;

        let mapping = mapping.map_or(HashMap::new(), |m| m);

        let nodes_to_remove = if let Some(flags) = flags {
            super::flag::get_nodes_to_remove(&flags)
        } else {
            HashSet::new()
        };

        let mut dfr = DataFlowRecord {
            uuid: id,
            flow,
            operators: HashMap::with_capacity(operators.len()),
            sinks: HashMap::with_capacity(sinks.len()),
            sources: HashMap::with_capacity(sources.len()),
            connectors: Vec::new(),
            links: Vec::new(),
            counter: 0u32,
        };

        for o in operators
            .into_iter()
            .filter(|o| !nodes_to_remove.contains(&o.id))
        {
            // Converting inputs
            let mut inputs: Vec<PortRecord> = vec![];
            for i in o.inputs {
                inputs.push((i, dfr.counter).into());
                dfr.counter += 1;
            }

            // Converting outputs
            let mut outputs: Vec<PortRecord> = vec![];
            for o in o.outputs {
                outputs.push((o, dfr.counter).into());
                dfr.counter += 1;
            }

            let or = OperatorRecord {
                id: o.id.clone(),
                uid: dfr.counter,
                inputs,
                outputs,
                uri: o.uri,
                configuration: merge_configurations(global_configuration.clone(), o.configuration),
                runtime: mapping
                    .get(&o.id)
                    .ok_or(ZFError::MissingConfiguration)
                    .cloned()?,
            };
            dfr.operators.insert(o.id, or);
            dfr.counter += 1;
        }

        for s in sources
            .into_iter()
            .filter(|s| !nodes_to_remove.contains(&s.id))
        {
            let mut outputs: Vec<PortRecord> = vec![];
            for o in s.outputs {
                outputs.push((o, dfr.counter).into());
                dfr.counter += 1;
            }

            let sr = SourceRecord {
                id: s.id.clone(),
                uid: dfr.counter,
                outputs,
                uri: s.uri,
                configuration: merge_configurations(global_configuration.clone(), s.configuration),
                runtime: mapping
                    .get(&s.id)
                    .ok_or(ZFError::MissingConfiguration)
                    .cloned()?,
            };
            dfr.sources.insert(s.id, sr);
            dfr.counter += 1;
        }

        for s in sinks
            .into_iter()
            .filter(|s| !nodes_to_remove.contains(&s.id))
        {
            let mut inputs: Vec<PortRecord> = Vec::with_capacity(s.inputs.len());
            for i in s.inputs {
                inputs.push((i, dfr.counter).into());
                dfr.counter += 1;
            }

            let sr = SinkRecord {
                id: s.id.clone(),
                uid: dfr.counter,
                inputs,
                uri: s.uri,
                configuration: merge_configurations(global_configuration.clone(), s.configuration),
                runtime: mapping
                    .get(&s.id)
                    .ok_or(ZFError::MissingConfiguration)
                    .cloned()?,
            };
            dfr.sinks.insert(s.id, sr);
            dfr.counter += 1;
        }

        dfr.add_links(&links, &nodes_to_remove)?;

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
