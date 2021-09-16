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
use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::model::dataflow::DataFlowDescriptor;
use zenoh_flow::model::{
    component::{OperatorDescriptor, SinkDescriptor, SourceDescriptor},
    dataflow::DataFlowRecord,
};
use zenoh_flow::runtime::graph::DataFlowGraph;
use zenoh_flow::runtime::message::ControlMessage;
use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::runners::{RunnerKind, RunnerManager};
use zenoh_flow::runtime::ZFRuntimeClient;
use zenoh_flow::runtime::{Runtime, RuntimeConfig, RuntimeInfo, RuntimeStatus, RuntimeStatusKind};
use zenoh_flow::types::{ZFError, ZFResult};

use std::convert::TryFrom;
use znrpc_macros::znserver;
use zrpc::ZNServe;

pub struct RTState {
    pub graphs: HashMap<Uuid, (DataFlowGraph, Vec<RunnerManager>)>,
    pub config: RuntimeConfig,
}

#[derive(Clone)]
pub struct Daemon {
    pub zn: Arc<ZSession>,
    pub store: DataStore,
    pub state: Arc<Mutex<RTState>>,
    pub runtime_uuid: Uuid,
    pub runtime_name: Arc<str>,
}

impl Daemon {
    pub fn new(
        zn: Arc<ZSession>,
        z: Arc<zenoh::Zenoh>,
        runtime_uuid: Uuid,
        runtime_name: String,
        config: RuntimeConfig,
    ) -> Self {
        let state = Arc::new(Mutex::new(RTState {
            graphs: HashMap::new(),
            config,
        }));

        Self {
            zn,
            store: DataStore::new(z),
            runtime_uuid,
            runtime_name: runtime_name.into(),
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

        let rt_server = self
            .clone()
            .get_zf_runtime_server(self.zn.clone(), Some(self.runtime_uuid));
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

        let mut rt_info = self.store.get_runtime_info(&self.runtime_uuid).await?;
        let mut rt_status = self.store.get_runtime_status(&self.runtime_uuid).await?;

        rt_info.status = RuntimeStatusKind::Ready;
        rt_status.status = RuntimeStatusKind::Ready;

        self.store
            .add_runtime_info(&self.runtime_uuid, &rt_info)
            .await?;
        self.store
            .add_runtime_status(&self.runtime_uuid, &rt_status)
            .await?;

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
            id: self.runtime_uuid,
            name: self.runtime_name.clone(),
            tags: Vec::new(),
            status: RuntimeStatusKind::NotReady,
        };

        let rt_status = RuntimeStatus {
            id: self.runtime_uuid,
            status: RuntimeStatusKind::NotReady,
            running_flows: 0,
            running_operators: 0,
            running_sources: 0,
            running_sinks: 0,
            running_connectors: 0,
        };

        let self_state = self.state.lock().await;
        self.store
            .add_runtime_config(&self.runtime_uuid, &self_state.config)
            .await?;
        drop(self_state);

        self.store
            .add_runtime_info(&self.runtime_uuid, &rt_info)
            .await?;
        self.store
            .add_runtime_status(&self.runtime_uuid, &rt_status)
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

        self.store.remove_runtime_config(&self.runtime_uuid).await?;
        self.store.remove_runtime_info(&self.runtime_uuid).await?;
        self.store.remove_runtime_status(&self.runtime_uuid).await?;

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

        let mapped = zenoh_flow::runtime::map_to_infrastructure(flow, &self.runtime_name).await?;

        let involved_runtimes = mapped.get_runtimes();
        let involved_runtimes = involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.runtime_name);

        for rt in involved_runtimes {
            let rt_info = self.store.get_runtime_info_by_name(&rt).await?;
            let client = ZFRuntimeClient::new(self.zn.clone(), rt_info.id);
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

        let is_also_local = all_involved_runtimes.contains(&self.runtime_uuid);

        let remote_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.runtime_uuid);

        for rt in remote_involved_runtimes {
            let client = ZFRuntimeClient::new(self.zn.clone(), rt);
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

        let mut dataflow_graph = DataFlowGraph::try_from(dfr.clone())?;

        dataflow_graph.load(&self.runtime_name)?;

        dataflow_graph.make_connections(&self.runtime_name).await?;

        let mut self_state = self.state.lock().await;
        self_state.graphs.insert(dfr.uuid, (dataflow_graph, vec![]));
        drop(self_state);
        self.store
            .add_runtime_flow(&self.runtime_uuid, &dfr)
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
            Some((dfg, _)) => {
                let record = self
                    .store
                    .get_runtime_flow_by_instance(&self.runtime_uuid, &record_id)
                    .await?;

                self.store
                    .remove_runtime_flow_instance(&self.runtime_uuid, &record.flow, &record.uuid)
                    .await?;

                Ok(record)
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }

    async fn start(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!(
            "Starting components (not sources) for Instance UUID: {}",
            record_id
        );

        let mut _state = self.state.lock().await;

        let mut rt_status = self.store.get_runtime_status(&self.runtime_uuid).await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut sinks = instance.0.get_sinks();
                for runner in sinks.drain(..) {
                    let m = runner.start();
                    instance.1.push(m);
                    rt_status.running_sinks += 1;
                }

                let mut operators = instance.0.get_operators();
                for runner in operators.drain(..) {
                    let m = runner.start();
                    instance.1.push(m);
                    rt_status.running_operators += 1;
                }

                let mut connectors = instance.0.get_connectors();
                for runner in connectors.drain(..) {
                    let m = runner.start();
                    instance.1.push(m);
                    rt_status.running_connectors += 1;
                }

                self.store
                    .add_runtime_status(&self.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn start_sources(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!("Starting sources for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;

        let mut rt_status = self.store.get_runtime_status(&self.runtime_uuid).await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut sources = instance.0.get_sources();
                for runner in sources.drain(..) {
                    let m = runner.start();
                    instance.1.push(m);
                    rt_status.running_sources += 1;
                }

                rt_status.running_flows += 1;

                self.store
                    .add_runtime_status(&self.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn stop(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!(
            "Stopping components (not sources) for Instance UUID: {}",
            record_id
        );

        let mut _state = self.state.lock().await;

        let mut rt_status = self.store.get_runtime_status(&self.runtime_uuid).await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                let mut to_be_removed = vec![];

                for (i, m) in instance.1.iter().enumerate() {
                    match m.get_kind() {
                        RunnerKind::Source => continue,
                        RunnerKind::Sink => {
                            m.kill().await?;
                            to_be_removed.push(i);
                            rt_status.running_sinks -= 1;
                        }
                        RunnerKind::Operator => {
                            m.kill().await?;
                            to_be_removed.push(i);
                            rt_status.running_operators -= 1;
                        }
                        RunnerKind::Connector => {
                            m.kill().await?;
                            to_be_removed.push(i);
                            rt_status.running_connectors -= 1;
                        }
                    }
                }
                to_be_removed.reverse();
                for i in to_be_removed.iter() {
                    let manager = instance.1.remove(*i);
                    futures::join!(manager);
                }

                let mut sinks = instance.0.get_sinks();
                for runner in sinks.drain(..) {
                    runner.clean().await?;
                }

                let mut operators = instance.0.get_operators();
                for runner in operators.drain(..) {
                    runner.clean().await?;
                }

                self.store
                    .add_runtime_status(&self.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn stop_sources(&self, record_id: Uuid) -> ZFResult<()> {
        log::info!("Stopping sources for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let mut rt_status = self.store.get_runtime_status(&self.runtime_uuid).await?;

        match _state.graphs.get_mut(&record_id) {
            Some(mut instance) => {
                //let (graph, mut managers) = d;
                let mut to_be_removed = vec![];

                for (i, m) in instance.1.iter().enumerate() {
                    match m.get_kind() {
                        RunnerKind::Source => {
                            m.kill().await?;
                            to_be_removed.push(i);
                            rt_status.running_sources -= 1;
                        }
                        _ => continue,
                    }
                }
                to_be_removed.reverse();

                for i in to_be_removed.iter() {
                    let manager = instance.1.remove(*i);
                    futures::join!(manager);
                }

                rt_status.running_flows -= 1;

                let mut sources = instance.0.get_sources();
                for runner in sources.drain(..) {
                    runner.clean().await?;
                }

                self.store
                    .add_runtime_status(&self.runtime_uuid, &rt_status)
                    .await?;

                Ok(())
            }
            None => Err(ZFError::InstanceNotFound(record_id)),
        }
    }
    async fn start_node(&self, record_id: Uuid, node: String) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn stop_node(&self, record_id: Uuid, node: String) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
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
