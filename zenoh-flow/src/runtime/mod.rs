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

use crate::serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    model::dataflow::{DataFlowDescriptor, Mapping},
    ZFResult, ZFRuntimeID,
};

pub mod connectors;
pub mod graph;
pub mod loader;
pub mod message;
pub mod resources;
pub mod runner;

pub async fn map_to_infrastructure(
    mut descriptor: DataFlowDescriptor,
    runtime: &ZFRuntimeID,
) -> ZFResult<DataFlowDescriptor> {
    log::debug!("[Dataflow mapping] Begin mapping for: {}", descriptor.flow);

    // Initial "stupid" mapping, if an operator is not mapped, we map to the local runtime.
    // function is async because it could involve other nodes.

    let mut mappings = Vec::new();

    for o in &descriptor.operators {
        match descriptor.get_mapping(&o.id) {
            Some(_) => (),
            None => {
                let mapping = Mapping {
                    id: o.id.clone(),
                    runtime: (*runtime).clone(),
                };
                mappings.push(mapping);
            }
        }
    }

    for o in &descriptor.sources {
        match descriptor.get_mapping(&o.id) {
            Some(_) => (),
            None => {
                let mapping = Mapping {
                    id: o.id.clone(),
                    runtime: (*runtime).clone(),
                };
                mappings.push(mapping);
            }
        }
    }

    for o in &descriptor.sinks {
        match descriptor.get_mapping(&o.id) {
            Some(_) => (),
            None => {
                let mapping = Mapping {
                    id: o.id.clone(),
                    runtime: (*runtime).clone(),
                };
                mappings.push(mapping);
            }
        }
    }

    for m in mappings {
        descriptor.add_mapping(m)
    }

    Ok(descriptor)
}

// Runtime related types, maybe can be moved.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeStatusEnum {
    Ready,
    NotReady,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeInfo {
    pub id: Uuid,
    pub name: String,
    pub tags: Vec<String>,
    pub status: RuntimeStatusEnum,
    // Do we need/want also RAM usage?
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeStatus {
    pub id: Uuid,
    pub status: RuntimeStatusEnum,
    pub running_flows: usize,
    pub running_operators: usize,
    pub running_sources: usize,
    pub running_sinks: usize,
    pub running_connectors: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ZenohConfigKind {
    Peer,
    Client,
}

impl std::fmt::Display for ZenohConfigKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ZenohConfigKind::Peer => write!(f, "peer"),
            ZenohConfigKind::Client => write!(f, "client"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZenohConfig {
    pub kind: ZenohConfigKind, // whether the runtime is a peer or a client
    pub listen: Vec<String>,   // if the runtime is a peer, where it listens
    pub locators: Vec<String>, // where to connect (eg. a router if the runtime is a client, or other peers/routers if the runtime is a peer)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeConfig {
    pub pid_file: String, //Where the PID file resides
    pub path: String,     //Where the libraries are downloaded/located
    pub name: Option<String>,
    pub uuid: Option<Uuid>,
    pub zenoh: ZenohConfig,
}
