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
use std::convert::TryFrom;
use uhlc::HLC;
use uuid::Uuid;
use zenoh::prelude::*;
use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::model::dataflow::descriptor::DataFlowDescriptor;
use zenoh_flow::model::{
    dataflow::record::DataFlowRecord,
    node::{OperatorDescriptor, SinkDescriptor, SourceDescriptor},
};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::dataflow::loader::Loader;
use zenoh_flow::runtime::dataflow::Dataflow;
use zenoh_flow::runtime::message::ControlMessage;
use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::RuntimeClient;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::runtime::{Runtime, RuntimeConfig, RuntimeInfo, RuntimeStatus, RuntimeStatusKind};
use zenoh_flow::types::{ZFError, ZFResult};
use zenoh_flow::NodeId;
use znrpc_macros::znserver;
use zrpc::ZNServe;

pub struct RTState {
    pub graphs: HashMap<Uuid, DataflowInstance>,
    pub config: RuntimeConfig,
}

#[derive(Clone)]
pub struct Daemon {
    pub store: DataStore,
    pub state: Arc<Mutex<RTState>>,
    pub ctx: RuntimeContext,
}

impl Daemon {
    pub fn new(z: Arc<zenoh::Session>, ctx: RuntimeContext, config: RuntimeConfig) -> Self {
        let state = Arc::new(Mutex::new(RTState {
            graphs: HashMap::new(),
            config,
        }));

        Self {
            store: DataStore::new(z),
            ctx,
            state,
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

        let mut zconfig = zenoh::config::Config::default();

        zconfig
            .set_mode(Some(config.zenoh.kind.clone().into()))
            .map_err(|_| {
                ZFError::ZenohError(format!(
                    "Unable to configure Zenoh mode {:?}",
                    config.zenoh.locators
                ))
            })?;
        zconfig
            .set_peers(
                config
                    .zenoh
                    .locators
                    .iter()
                    .filter_map(|l| l.parse().ok())
                    .collect(),
            )
            .map_err(|_| {
                ZFError::ZenohError(format!(
                    "Unable to configure Zenoh peers {:?}",
                    config.zenoh.locators
                ))
            })?;
        zconfig
            .set_listeners(
                config
                    .zenoh
                    .listen
                    .iter()
                    .filter_map(|l| l.parse().ok())
                    .collect(),
            )
            .map_err(|_| {
                ZFError::ZenohError(format!(
                    "Unable to configure Zenoh listeners {:?}",
                    config.zenoh.listen
                ))
            })?;

        let session = Arc::new(zenoh::open(zconfig).wait()?);
        let hlc = Arc::new(HLC::default());
        let loader = Arc::new(Loader::new(config.loader.clone()));

        let ctx = RuntimeContext {
            session: session.clone(),
            hlc,
            loader,
            runtime_name: name.into(),
            runtime_uuid: uuid,
        };

        Ok(Self::new(session, ctx, config))
    }

    pub async fn run(&self, stop: async_std::channel::Receiver<()>) -> ZFResult<()> {
        log::info!("Runtime main loop starting");

        let rt_server = self
            .clone()
            .get_runtime_server(self.ctx.session.clone(), Some(self.ctx.runtime_uuid));
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
        let (srt, _hrt) = rt_server
            .start()
            .await
            .map_err(|_e| ZFError::GenericError)?;

        log::trace!("Setting state as Ready");

        let mut rt_info = self.store.get_runtime_info(&self.ctx.runtime_uuid).await?;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        rt_info.status = RuntimeStatusKind::Ready;
        rt_status.status = RuntimeStatusKind::Ready;

        self.store
            .add_runtime_info(&self.ctx.runtime_uuid, &rt_info)
            .await?;
        self.store
            .add_runtime_status(&self.ctx.runtime_uuid, &rt_status)
            .await?;

        log::trace!("Running...");

        let _ = stop
            .recv()
            .await
            .map_err(|e| ZFError::RecvError(format!("{}", e)))?;

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
    ) -> ZFResult<(
        async_std::channel::Sender<()>,
        async_std::task::JoinHandle<ZFResult<()>>,
    )> {
        // Starting main loop in a task
        let (s, r) = async_std::channel::bounded::<()>(1);
        let rt = self.clone();

        let rt_info = RuntimeInfo {
            id: self.ctx.runtime_uuid,
            name: self.ctx.runtime_name.clone(),
            tags: Vec::new(),
            status: RuntimeStatusKind::NotReady,
        };

        let rt_status = RuntimeStatus {
            id: self.ctx.runtime_uuid,
            status: RuntimeStatusKind::NotReady,
            running_flows: 0,
            running_operators: 0,
            running_sources: 0,
            running_sinks: 0,
            running_connectors: 0,
        };

        let self_state = self.state.lock().await;
        self.store
            .add_runtime_config(&self.ctx.runtime_uuid, &self_state.config)
            .await?;
        drop(self_state);

        self.store
            .add_runtime_info(&self.ctx.runtime_uuid, &rt_info)
            .await?;
        self.store
            .add_runtime_status(&self.ctx.runtime_uuid, &rt_status)
            .await?;

        let h = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async { rt.run(r).await })
        });
        Ok((s, h))
    }

    pub async fn stop(&self, stop: async_std::channel::Sender<()>) -> ZFResult<()> {
        stop.send(())
            .await
            .map_err(|e| ZFError::SendError(format!("{}", e)))?;

        self.store
            .remove_runtime_config(&self.ctx.runtime_uuid)
            .await?;
        self.store
            .remove_runtime_info(&self.ctx.runtime_uuid)
            .await?;
        self.store
            .remove_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        Ok(())
    }
}

pub fn get_machine_uuid() -> ZFResult<Uuid> {
    let machine_id_raw = machine_uid::get().map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
    let node_str: &str = &machine_id_raw;
    Uuid::parse_str(node_str).map_err(|e| ZFError::ParsingError(format!("{}", e)))
}

#[znserver]
impl Runtime for Daemon {
    async fn instantiate(&self, flow: DataFlowDescriptor) -> ZFResult<DataFlowRecord> {
        //TODO: workaround - it should just take the ID of the flow...

        let record_uuid = Uuid::new_v4();
        let flow_name = flow.flow.clone();

        log::info!(
            "Instantiating Flow {} - Instance UUID: {}",
            flow_name,
            record_uuid
        );

        let mut rt_clients = vec![];

        let mapped =
            zenoh_flow::runtime::map_to_infrastructure(flow, &self.ctx.runtime_name).await?;

        let involved_runtimes = mapped.get_runtimes();
        let involved_runtimes = involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_name);

        for rt in involved_runtimes {
            let rt_info = self.store.get_runtime_info_by_name(&rt).await?;
            let client = RuntimeClient::new(self.ctx.session.clone(), rt_info.id);
            rt_clients.push(client);
        }

        // remote prepare
        for client in rt_clients.iter() {
            client.prepare(mapped.clone(), record_uuid).await??;
        }

        // self prepare
        let dfr = Runtime::prepare(self, mapped.clone(), record_uuid).await?;

        // remote start
        for client in rt_clients.iter() {
            client.start(record_uuid).await??;
        }

        // self start
        Runtime::start(self, record_uuid).await?;

        // remote start sources
        for client in rt_clients.iter() {
            client.start_sources(record_uuid).await??;
        }

        // self start sources
        Runtime::start_sources(self, record_uuid).await?;

        log::info!(
            "Done Instantiating Flow {} - Instance UUID: {}",
            flow_name,
            record_uuid
        );

        Ok(dfr)
    }

    async fn teardown(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        log::info!("Tearing down Instance UUID: {}", record_id);
        let record = self.store.get_flow_by_instance(&record_id).await?;

        let mut rt_clients = vec![];

        let all_involved_runtimes = self.store.get_flow_instance_runtimes(&record_id).await?;

        let is_also_local = all_involved_runtimes.contains(&self.ctx.runtime_uuid);

        let remote_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_uuid);

        for rt in remote_involved_runtimes {
            let client = RuntimeClient::new(self.ctx.session.clone(), rt);
            rt_clients.push(client);
        }

        // remote stop sources
        for client in rt_clients.iter() {
            client.stop_sources(record_id).await??;
        }

        // local stop sources
        if is_also_local {
            self.stop_sources(record_id).await?;
        }

        // remote stop
        for client in rt_clients.iter() {
            client.stop(record_id).await??;
        }

        // local stop
        if is_also_local {
            Runtime::stop(self, record_id).await?;
        }

        // remote stop clean
        for client in rt_clients.iter() {
            client.clean(record_id).await??;
        }

        // local clean
        if is_also_local {
            self.clean(record_id).await;
        }

        log::info!("Done teardown down Instance UUID: {}", record_id);

        Ok(record)
    }

    async fn prepare(&self, flow: DataFlowDescriptor, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        let flow_name = flow.flow.clone();

        log::info!(
            "Preparing for Flow {} Instance UUID: {}",
            flow_name,
            record_id
        );

        let mut dfr = DataFlowRecord::try_from((flow, record_id))?;

        let mut dataflow = Dataflow::try_new(self.ctx.clone(), dfr.clone())?;
        let mut instance = DataflowInstance::try_instantiate(dataflow)?;

        let mut self_state = self.state.lock().await;
        self_state.graphs.insert(dfr.uuid, instance);
        drop(self_state);
        self.store
            .add_runtime_flow(&self.ctx.runtime_uuid, &dfr)
            .await?;

        log::info!(
            "Done preparation for Flow {} Instance UUID: {}",
            flow_name,
            record_id
        );

        Ok(dfr)
    }
    async fn clean(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        log::info!("Cleaning for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let data = _state.graphs.remove(&record_id);

        match data {
            Some(dfg) => {
                let record = self
                    .store
                    .get_runtime_flow_by_instance(&self.ctx.runtime_uuid, &record_id)
                    .await?;

                self.store
                    .remove_runtime_flow_instance(
                        &self.ctx.runtime_uuid,
                        &record.flow,
                        &record.uuid,
                    )
                    .await?;

                Ok(record)
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }

    async fn start(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!(
            "Starting nodes (not sources) for Instance UUID: {}",
            record_id
        );

        let mut _state = self.state.lock().await;

        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut sinks = instance.get_sinks();
                for id in sinks.drain(..) {
                    instance.start_node(&id).await?;
                    rt_status.running_sinks += 1;
                }

                let mut operators = instance.get_operators();
                for id in operators.drain(..) {
                    instance.start_node(&id).await?;
                    rt_status.running_operators += 1;
                }

                let mut connectors = instance.get_connectors();
                for id in connectors.drain(..) {
                    instance.start_node(&id).await?;
                    rt_status.running_connectors += 1;
                }

                self.store
                    .add_runtime_status(&self.ctx.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn start_sources(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!("Starting sources for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;

        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut sources = instance.get_sources();
                for id in sources.drain(..) {
                    instance.start_node(&id).await?;
                    rt_status.running_sources += 1;
                }

                rt_status.running_flows += 1;

                self.store
                    .add_runtime_status(&self.ctx.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn stop(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!(
            "Stopping nodes (not sources) for Instance UUID: {}",
            record_id
        );

        let mut _state = self.state.lock().await;

        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut sinks = instance.get_sinks();
                for id in sinks.drain(..) {
                    instance.stop_node(&id).await?;
                    rt_status.running_sinks -= 1;
                }

                let mut operators = instance.get_operators();
                for id in operators.drain(..) {
                    instance.stop_node(&id).await?;
                    rt_status.running_operators -= 1;
                }

                let mut connectors = instance.get_connectors();
                for id in connectors.drain(..) {
                    instance.stop_node(&id).await?;
                    rt_status.running_connectors -= 1;
                }

                self.store
                    .add_runtime_status(&self.ctx.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn stop_sources(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!("Stopping sources for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut sources = instance.get_sources();
                for id in sources.drain(..) {
                    instance.stop_node(&id).await?;
                    rt_status.running_sources -= 1;
                }

                rt_status.running_flows -= 1;

                self.store
                    .add_runtime_status(&self.ctx.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn start_node(&self, instance_id: Uuid, node: String) -> ZFResult<()> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(mut instance) => Ok(instance.start_node(&node.into()).await?),
            None => Err(ZFError::InstanceNotFound(instance_id)),
        }
    }
    async fn stop_node(&self, instance_id: Uuid, node: String) -> ZFResult<()> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(mut instance) => Ok(instance.stop_node(&node.into()).await?),
            None => Err(ZFError::InstanceNotFound(instance_id)),
        }
    }

    async fn start_record(&self, instance_id: Uuid, source_id: NodeId) -> ZFResult<String> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get(&instance_id) {
            Some(instance) => {
                let key_expr = instance.start_recording(&source_id).await?;
                Ok(key_expr)
            }
            None => Err(ZFError::InstanceNotFound(instance_id)),
        }
    }

    async fn stop_record(&self, instance_id: Uuid, source_id: NodeId) -> ZFResult<String> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get(&instance_id) {
            Some(instance) => {
                let key_expr = instance.stop_recording(&source_id).await?;
                Ok(key_expr)
            }
            None => Err(ZFError::InstanceNotFound(instance_id)),
        }
    }

    async fn start_replay(
        &self,
        instance_id: Uuid,
        source_id: NodeId,
        key_expr: String,
    ) -> ZFResult<NodeId> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(mut instance) => {
                if !(instance.is_node_running(&source_id).await?) {
                    let replay_id = instance.start_replay(&source_id, key_expr).await?;
                    Ok(replay_id)
                } else {
                    Err(ZFError::InvalidState)
                }
            }
            None => Err(ZFError::InstanceNotFound(instance_id)),
        }
    }

    async fn stop_replay(
        &self,
        instance_id: Uuid,
        source_id: NodeId,
        replay_id: NodeId,
    ) -> ZFResult<NodeId> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(mut instance) => {
                instance.stop_replay(&replay_id).await?;
                Ok(replay_id)
            }
            None => Err(ZFError::InstanceNotFound(instance_id)),
        }
    }

    async fn notify_runtime(
        &self,
        record_id: Uuid,
        node: String,
        message: ControlMessage,
    ) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn check_operator_compatibility(&self, operator: OperatorDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
    async fn check_source_compatibility(&self, source: SourceDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
    async fn check_sink_compatibility(&self, sink: SinkDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
}
