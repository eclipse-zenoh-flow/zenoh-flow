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
use std::collections::HashMap;
use uuid::Uuid;
use zenoh::net::Session as ZSession;
use zenoh::ZFuture;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::runtime::graph::DataFlowGraph;
use zenoh_flow::runtime::RuntimeConfig;
use zenoh_flow::serde::{Deserialize, Serialize};
use zenoh_flow::types::{ZFError, ZFResult};

pub struct Runtime {
    pub z: Arc<ZSession>,
    pub graphs: HashMap<String, DataFlowGraph>,
    pub runtime_uuid: Uuid,
    pub runtime_name: String,
    pub config: RuntimeConfig,
}

impl Runtime {
    pub fn new(
        z: Arc<ZSession>,
        runtime_uuid: Uuid,
        runtime_name: String,
        config: RuntimeConfig,
    ) -> Self {
        Self {
            z,
            runtime_uuid,
            runtime_name,
            config,
            graphs: HashMap::new(),
        }
    }

    pub fn from_config(config: RuntimeConfig) -> ZFResult<Self> {
        let uuid = match &config.uuid {
            Some(u) => *u,
            None => get_machine_uuid()?,
        };

        let name = match &config.name {
            Some(n) => n.clone(),
            None => String::from(hostname::get()?.to_str().ok_or(ZFError::GenericError)?),
        };

        let zenoh_properties = zenoh::Properties::from(format!(
            "mode={};peer={};listener={}",
            &config.zenoh.kind,
            &config.zenoh.locators.join(","),
            &config.zenoh.listen.join(",")
        ));

        let zenoh = Arc::new(zenoh::net::open(zenoh_properties.into()).wait()?);

        Ok(Self::new(zenoh, uuid, name, config))
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
}

pub fn get_machine_uuid() -> ZFResult<Uuid> {
    let machine_id_raw = machine_uid::get().map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
    let node_str: &str = &machine_id_raw;
    Uuid::parse_str(node_str).map_err(|e| ZFError::ParsingError(format!("{}", e)))
}
