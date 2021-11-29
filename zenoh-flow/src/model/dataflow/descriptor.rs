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

use crate::model::dataflow::validator::DataflowValidator;
use crate::model::deadline::E2EDeadlineDescriptor;
use crate::model::link::LinkDescriptor;
use crate::model::node::{OperatorDescriptor, SinkDescriptor, SourceDescriptor};
use crate::serde::{Deserialize, Serialize};
use crate::types::{NodeId, RuntimeId, ZFError, ZFResult};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mapping {
    pub id: NodeId,
    pub runtime: RuntimeId,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<OperatorDescriptor>,
    pub sources: Vec<SourceDescriptor>,
    pub sinks: Vec<SinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<Vec<Mapping>>,
    pub deadlines: Option<Vec<E2EDeadlineDescriptor>>,
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

    // This method checks that the dataflow graph is correct.
    //
    // In particular it verifies that:
    // - each node has a unique id,
    // - each port (input and output) is connected,
    // - an input port is connected only once (i.e. it receives data from a single output port),
    // - connected ports are declared with the same type.
    fn validate(&self) -> ZFResult<()> {
        let validator = DataflowValidator::try_from(self)?;
        validator.validate_ports()?;
        if let Some(deadlines) = &self.deadlines {
            deadlines.iter().try_for_each(|deadline| {
                validator.validate_deadline(&deadline.from, &deadline.to)
            })?
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
