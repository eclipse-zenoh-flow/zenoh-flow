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
use uuid::Uuid;
use zenoh_flow::runtime::graph::DataFlowGraph;
use zenoh::net::Session as ZSession;
use std::collections::HashMap;
use zenoh_flow::serde::{Serialize, Deserialize};
use zenoh_flow::types::{ZFResult, ZFError};

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    pub listen: Vec<String>, // if the runtime is a peer, where it listens
    pub locators: Vec<String>, // where to connect (eg. a router if the runtime is a client, or other peers/routers if the runtime is a peer)
}



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeConfig {
    pub pid_file: String, //Where the PID file resides
    pub path: String, //Where the libraries are downloaded/located
    pub name: Option<String>,
    pub uuid: Option<Uuid>,
    pub zenoh: ZenohConfig,
}


#[derive(Clone)]
pub struct Runtime {
    pub z: Arc<ZSession>,
    pub graphs: HashMap<String,DataFlowGraph>,
    pub runtime_uuid: Uuid,
    pub runtime_name: String,
    pub config: RuntimeConfig,
}


impl Runtime {
    pub fn new(z: Arc<ZSession>, runtime_uuid: Uuid, runtime_name: String, config: RuntimeConfig) -> Self {
        Self {
            z,
            runtime_uuid,
            runtime_name,
            graph: HashMap::new(),
        }
    }

    pub fn from_config(config: RuntimeConfig) -> ZFResult<Self> {
        let uuid = match config.uuid {
            Some(u) => u,
            None => get_machine_uuid()?
        };

        let name = match config.name {
            Some(n) => n,
            None => String::from(hostname::get()?.to_str()?),
        };

        let zenoh_properties = zenoh::Properties::from(format!("mode={};peer={};listener={}", config.zenoh.kind, config.zenoh.locators.join(","), config.zenoh.listen.join(",")));

        let zenoh = Arc::new(zenoh::net::open(zproperties.into()).await?);

        Ok(Self::new(zenoh, uuid, name, config))
    }
}



pub fn get_machine_uuid() -> ZFResult<Uuid> {
    let machine_id_raw = machine_uid::get()?;
    let node_str: &str = &machine_id_raw;
    Ok(Uuid::parse_str(node_str)?)
}