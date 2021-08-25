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
use zenoh_flow::model::{
    dataflow::DataFlowRecord,
    operator::{ZFOperatorDescriptor, ZFSinkDescriptor, ZFSourceDescriptor},
};
use zenoh_flow::runtime::graph::DataFlowGraph;
use zenoh_flow::runtime::message::ZFControlMessage;
use zenoh_flow::runtime::resources::ZFDataStore;
use zenoh_flow::runtime::{ZFRuntime, ZFRuntimeConfig};
use zenoh_flow::serde::{Deserialize, Serialize};
use zenoh_flow::types::{ZFError, ZFResult};

use znrpc_macros::znserver;
use zrpc::ZNServe;

#[derive(Clone)]
pub struct Runtime {
    pub zn: Arc<ZSession>,
    pub store: ZFDataStore,
    pub graphs: HashMap<String, DataFlowGraph>,
    pub runtime_uuid: Uuid,
    pub runtime_name: String,
    pub config: ZFRuntimeConfig,
}

impl Runtime {
    pub fn new(
        zn: Arc<ZSession>,
        z: Arc<zenoh::Zenoh>,
        runtime_uuid: Uuid,
        runtime_name: String,
        config: ZFRuntimeConfig,
    ) -> Self {
        Self {
            zn,
            store: ZFDataStore::new(z),
            runtime_uuid,
            runtime_name,
            config,
            graphs: HashMap::new(),
        }
    }

    pub fn from_config(config: ZFRuntimeConfig) -> ZFResult<Self> {
        let uuid = match &config.uuid {
            Some(u) => *u,
            None => get_machine_uuid()?,
        };

        let name = match &config.name {
            Some(n) => n.clone(),
            None => String::from(hostname::get()?.to_str().ok_or(ZFError::GenericError)?),
        };

        let zn_properties = zenoh::Properties::from(format!(
            "mode={};peer={};listener={}",
            &config.zenoh.kind,
            &config.zenoh.locators.join(","),
            &config.zenoh.listen.join(",")
        ));

        let zenoh_properties = zenoh::Properties::from(format!(
            "mode={};peer={},{}",
            &config.zenoh.kind,
            &config.zenoh.listen.join(","),
            &config.zenoh.locators.join(","),
        ));

        let zn = Arc::new(zenoh::net::open(zn_properties.into()).wait()?);
        let z = Arc::new(zenoh::Zenoh::new(zenoh_properties.into()).wait()?);

        Ok(Self::new(zn, z, uuid, name, config))
    }

    pub async fn run(&self, stop: async_std::channel::Receiver<()>) -> ZFResult<()> {
        log::info!("Runtime main loop starting");

        let rt_server = self.clone().get_zf_runtime_server(self.zn.clone(), None);
        let (rt_stopper, _hrt) = rt_server
            .connect()
            .await
            .map_err(|_e| ZFError::GenericError)?;
        rt_server
            .initialize()
            .await
            .map_err(|_e| ZFError::GenericError)?;
        rt_server
            .register()
            .await
            .map_err(|_e| ZFError::GenericError)?;

        log::trace!("Staring ZRPC Servers");
        let (srt, hrt) = rt_server
            .start()
            .await
            .map_err(|_e| ZFError::GenericError)?;

        let _ = stop
            .recv()
            .await
            .map_err(|e| ZFError::RecvError(format!("{}", e)));

        rt_server
            .stop(srt)
            .await
            .map_err(|_e| ZFError::GenericError)?;
        rt_server
            .unregister()
            .await
            .map_err(|_e| ZFError::GenericError)?;
        rt_server
            .disconnect(rt_stopper)
            .await
            .map_err(|_e| ZFError::GenericError)?;

        log::info!("Runtime main loop exiting...");
        Ok(())
    }

    pub async fn start(
        &self,
    ) -> (
        async_std::channel::Sender<()>,
        async_std::task::JoinHandle<ZFResult<()>>,
    ) {
        // Starting main loop in a task
        let (s, r) = async_std::channel::bounded::<()>(1);
        let rt = self.clone();
        let h = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async { rt.run(r).await })
        });
        (s, h)
    }

    pub async fn stop(&self, stop: async_std::channel::Sender<()>) {
        stop.send(()).await.unwrap();
    }
}

pub fn get_machine_uuid() -> ZFResult<Uuid> {
    let machine_id_raw = machine_uid::get().map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
    let node_str: &str = &machine_id_raw;
    Uuid::parse_str(node_str).map_err(|e| ZFError::ParsingError(format!("{}", e)))
}

#[znserver]
impl ZFRuntime for Runtime {
    async fn instantiate(&self, flow_id: String) -> ZFResult<DataFlowRecord> {
        Err(ZFError::Unimplemented)
    }

    async fn teardown(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        Err(ZFError::Unimplemented)
    }
    async fn prepare(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        Err(ZFError::Unimplemented)
    }
    async fn clean(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        Err(ZFError::Unimplemented)
    }

    async fn start(&self, record_id: Uuid) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn start_sources(&self, record_id: Uuid) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn stop(&self, record_id: Uuid) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn stop_sources(&self, record_id: Uuid) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn start_node(&self, record_id: Uuid, node: String) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn stop_node(&self, record_id: Uuid, node: String) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn notify_node(
        &self,
        record_id: Uuid,
        node: String,
        message: ZFControlMessage,
    ) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn check_operator_compatibility(&self, operator: ZFOperatorDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
    async fn check_source_compatibility(&self, source: ZFSourceDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
    async fn check_sink_compatibility(&self, sink: ZFSinkDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
}
