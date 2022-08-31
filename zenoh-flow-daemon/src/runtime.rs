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
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use async_std::sync::Mutex;
use uuid::Uuid;
use zenoh_flow::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::model::{
    dataflow::record::DataFlowRecord,
    node::{SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor},
};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::dataflow::Dataflow;
use zenoh_flow::runtime::message::ControlMessage;
use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::{
    DaemonInterfaceInternalClient, RuntimeConfig, RuntimeContext, RuntimeInfo, RuntimeStatus,
    RuntimeStatusKind,
};
use zenoh_flow::zfresult::ErrorKind;
use zenoh_flow::DaemonResult;
use zenoh_flow::Result as ZFResult;

/// The internal runtime state.
///
/// It keeps track of running instances and runtime configuration.
pub struct RTState {
    pub graphs: HashMap<Uuid, DataflowInstance>,
    pub config: RuntimeConfig,
}

#[derive(Clone)]
pub struct Runtime {
    pub store: DataStore,
    pub state: Arc<Mutex<RTState>>,
    pub ctx: RuntimeContext,
}

impl Runtime {
    pub(crate) fn new(z: Arc<zenoh::Session>, ctx: RuntimeContext, config: RuntimeConfig) -> Self {
        let store = DataStore::new(z);

        let state = Arc::new(Mutex::new(RTState {
            graphs: HashMap::new(),
            config,
        }));

        Self { store, ctx, state }
    }

    pub(crate) async fn start(&self) -> ZFResult<()> {
        let rt_info = RuntimeInfo {
            id: self.ctx.runtime_uuid,
            name: self.ctx.runtime_name.clone(),
            tags: Vec::new(),
            status: RuntimeStatusKind::NotReady,
        };

        let rt_status = RuntimeStatus {
            id: self.ctx.runtime_uuid,
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
            .await
    }

    pub(crate) async fn ready(&self) -> ZFResult<()> {
        let mut rt_info = self.store.get_runtime_info(&self.ctx.runtime_uuid).await?;

        rt_info.status = RuntimeStatusKind::Ready;

        self.store
            .add_runtime_info(&self.ctx.runtime_uuid, &rt_info)
            .await?;

        Ok(())
    }

    pub(crate) async fn stop(&self) -> ZFResult<()> {
        self.store
            .remove_runtime_config(&self.ctx.runtime_uuid)
            .await?;
        self.store
            .remove_runtime_info(&self.ctx.runtime_uuid)
            .await?;
        self.store
            .remove_runtime_status(&self.ctx.runtime_uuid)
            .await
    }

    // Zenoh Flow runtime operations

    pub(crate) async fn create_instance(
        &self,
        flow: FlattenDataFlowDescriptor,
        record_uuid: Uuid,
    ) -> DaemonResult<DataFlowRecord> {
        //TODO: workaround - it should just take the ID of the flow (when
        // the registry will be in place)

        let flow_name = flow.flow.clone();

        log::info!(
            "Creating Flow {} - Instance UUID: {}",
            flow_name,
            record_uuid
        );

        let mut rt_clients = vec![];

        // TODO: flatting of a descriptor, when the registry will be in place

        // Mapping to infrastructure
        let mapped =
            zenoh_flow::runtime::map_to_infrastructure(flow, &self.ctx.runtime_name).await?;

        // Getting runtime involved in this instance
        let involved_runtimes = mapped.get_runtimes();
        let involved_runtimes = involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_name);

        // Creating the record
        let dfr = DataFlowRecord::try_from((mapped, record_uuid))?;

        self.store
            .add_runtime_flow(&self.ctx.runtime_uuid, &dfr)
            .await?;

        // Creating clients to talk with other runtimes
        for rt in involved_runtimes {
            let rt_info = self.store.get_runtime_info_by_name(&rt).await?;
            let client = DaemonInterfaceInternalClient::new(self.ctx.session.clone(), rt_info.id);
            rt_clients.push(client);
        }

        // remote prepare
        for client in rt_clients.iter() {
            client.prepare(dfr.uuid).await??;
        }

        // self prepare
        self.prepare(dfr.uuid).await?;

        log::info!(
            "Created Flow {} - Instance UUID: {}",
            flow_name,
            record_uuid
        );

        Ok(dfr)
    }

    pub(crate) async fn delete_instance(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Delete Instance UUID: {}", record_id);
        let record = self.store.get_flow_by_instance(&record_id).await?;

        let mut rt_clients = vec![];

        let all_involved_runtimes = self.store.get_flow_instance_runtimes(&record_id).await?;

        let is_also_local = all_involved_runtimes.contains(&self.ctx.runtime_uuid);

        let remote_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_uuid);

        for rt in remote_involved_runtimes {
            let client = DaemonInterfaceInternalClient::new(self.ctx.session.clone(), rt);
            rt_clients.push(client);
        }

        // remote clean
        for client in rt_clients.iter() {
            client.clean(record_id).await??;
        }

        // local clean
        if is_also_local {
            self.clean(record_id).await?;
        }

        self.store
            .remove_runtime_flow_instance(&self.ctx.runtime_uuid, &record.flow, &record.uuid)
            .await?;

        log::info!("Done delete Instance UUID: {}", record_id);

        Ok(record)
    }

    pub(crate) async fn instantiate(
        &self,
        flow: FlattenDataFlowDescriptor,
        record_uuid: Uuid,
    ) -> DaemonResult<DataFlowRecord> {
        //TODO: workaround - it should just take the ID of the flow (when
        // the registry will be in place)

        log::info!("Instantiating: {}", flow.flow);

        // Creating
        let dfr = self.create_instance(flow.clone(), record_uuid).await?;

        // Starting
        self.start_instance(dfr.uuid).await?;

        log::info!(
            "Done Instantiation Flow {} - Instance UUID: {}",
            flow.flow,
            dfr.uuid,
        );

        Ok(dfr)
    }

    pub(crate) async fn teardown(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Tearing down Instance UUID: {}", record_id);

        // Stopping
        self.stop_instance(record_id).await?;

        // Clean-up
        let dfr = self.delete_instance(record_id).await?;

        log::info!("Done teardown down Instance UUID: {}", record_id);

        Ok(dfr)
    }

    pub(crate) async fn prepare(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Preparing for Instance UUID: {}", record_id);

        let dfr = self.store.get_flow_by_instance(&record_id).await?;

        let dataflow = Dataflow::try_new(self.ctx.clone(), dfr.clone())?;
        let instance = DataflowInstance::try_instantiate(dataflow, self.ctx.hlc.clone())?;

        let mut self_state = self.state.lock().await;
        self_state.graphs.insert(dfr.uuid, instance);
        drop(self_state);

        self.store
            .add_runtime_flow(&self.ctx.runtime_uuid, &dfr)
            .await?;

        log::info!("Done preparation for Instance UUID: {}", record_id);

        Ok(dfr)
    }
    pub(crate) async fn clean(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Cleaning for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let data = _state.graphs.remove(&record_id);
        match data {
            Some(_dfi) => {
                // Calling finalize on all nodes of a the graph.

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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }

    pub(crate) async fn start_instance(&self, record_id: Uuid) -> DaemonResult<()> {
        log::info!("Staring Instance UUID: {}", record_id);

        let mut rt_clients = vec![];

        let all_involved_runtimes = self.store.get_flow_instance_runtimes(&record_id).await?;

        let is_also_local = all_involved_runtimes.contains(&self.ctx.runtime_uuid);

        let all_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_uuid);

        for rt in all_involved_runtimes {
            let client = DaemonInterfaceInternalClient::new(self.ctx.session.clone(), rt);
            rt_clients.push(client);
        }

        //Note: We need to first start the nodes of the graphs in all the
        // involved runtimes before starting the sources.
        // Otherwise it may happen that operators inputs are filled with data
        // while the operator is not yet started.
        // Thus, first we start all the operators (and sinks) across the runtimes
        // then we can safely start the sources.

        // remote start
        for client in rt_clients.iter() {
            client.start(record_id).await??;
        }

        if is_also_local {
            // self start
            self.start_nodes(record_id).await?;
        }

        // remote start sources
        for client in rt_clients.iter() {
            client.start_sources(record_id).await??;
        }

        if is_also_local {
            // self start sources
            self.start_sources(record_id).await?;
        }

        log::info!("Started Instance UUID: {}", record_id);

        Ok(())
    }

    pub(crate) async fn stop_instance(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Stopping Instance UUID: {}", record_id);
        let record = self.store.get_flow_by_instance(&record_id).await?;

        let mut rt_clients = vec![];

        let all_involved_runtimes = self.store.get_flow_instance_runtimes(&record_id).await?;

        let is_also_local = all_involved_runtimes.contains(&self.ctx.runtime_uuid);

        let all_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_uuid);

        for rt in all_involved_runtimes {
            let client = DaemonInterfaceInternalClient::new(self.ctx.session.clone(), rt);
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
            self.stop_nodes(record_id).await?;
        }

        log::info!("Stopped Instance UUID: {}", record_id);

        Ok(record)
    }

    pub(crate) async fn start_nodes(&self, record_id: Uuid) -> DaemonResult<()> {
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
            Some(instance) => {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }

    pub(crate) async fn start_sources(&self, record_id: Uuid) -> DaemonResult<()> {
        log::info!("Starting sources for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;

        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&record_id) {
            Some(instance) => {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }

    pub(crate) async fn stop_nodes(&self, record_id: Uuid) -> DaemonResult<()> {
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
            Some(instance) => {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }

    pub(crate) async fn stop_sources(&self, record_id: Uuid) -> DaemonResult<()> {
        log::info!("Stopping sources for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&record_id) {
            Some(instance) => {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn start_node(&self, instance_id: Uuid, node: String) -> DaemonResult<()> {
        let mut _state = self.state.lock().await;
        // let mut rt_status = self
        //     .store
        //     .get_runtime_status(&self.ctx.runtime_uuid)
        //     .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(instance) => Ok(instance.start_node(&node.into()).await?),
            None => Err(ErrorKind::InstanceNotFound(instance_id)),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn stop_node(&self, instance_id: Uuid, node: String) -> DaemonResult<()> {
        let mut _state = self.state.lock().await;
        // let mut rt_status = self
        //     .store
        //     .get_runtime_status(&self.ctx.runtime_uuid)
        //     .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(instance) => Ok(instance.stop_node(&node.into()).await?),
            None => Err(ErrorKind::InstanceNotFound(instance_id)),
        }
    }

    // pub(crate) async fn start_record(&self, instance_id: Uuid, source_id: NodeId) -> DaemonResult<String> {
    //     let mut _state = self.state.lock().await;
    //     let mut rt_status = self
    //         .store
    //         .get_runtime_status(&self.ctx.runtime_uuid)
    //         .await?;

    //     match _state.graphs.get(&instance_id) {
    //         Some(instance) => {
    //             let key_expr = instance.start_recording(&source_id).await?;
    //             Ok(key_expr)
    //         }
    //         None => Err(ErrorKind::InstanceNotFound(instance_id)),
    //     }
    // }

    // pub(crate) sync fn stop_record(&self, instance_id: Uuid, source_id: NodeId) -> DaemonResult<String> {
    //     let mut _state = self.state.lock().await;
    //     let mut rt_status = self
    //         .store
    //         .get_runtime_status(&self.ctx.runtime_uuid)
    //         .await?;

    //     match _state.graphs.get(&instance_id) {
    //         Some(instance) => {
    //             let key_expr = instance.stop_recording(&source_id).await?;
    //             Ok(key_expr)
    //         }
    //         None => Err(ErrorKind::InstanceNotFound(instance_id)),
    //     }
    // }

    // pub(crate) async fn start_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     key_expr: String,
    // ) -> DaemonResult<NodeId> {
    //     let mut _state = self.state.lock().await;
    //     let mut rt_status = self
    //         .store
    //         .get_runtime_status(&self.ctx.runtime_uuid)
    //         .await?;

    //     match _state.graphs.get_mut(&instance_id) {
    //         Some(mut instance) => {
    //             if !(instance.is_node_running(&source_id).await?) {
    //                 let replay_id = instance.start_replay(&source_id, key_expr).await?;
    //                 Ok(replay_id)
    //             } else {
    //                 Err(ErrorKind::InvalidState)
    //             }
    //         }
    //         None => Err(ErrorKind::InstanceNotFound(instance_id)),
    //     }
    // }

    // pub(crate) async fn stop_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     replay_id: NodeId,
    // ) -> DaemonResult<NodeId> {
    //     let mut _state = self.state.lock().await;
    //     let mut rt_status = self
    //         .store
    //         .get_runtime_status(&self.ctx.runtime_uuid)
    //         .await?;

    //     match _state.graphs.get_mut(&instance_id) {
    //         Some(mut instance) => {
    //             instance.stop_replay(&replay_id).await?;
    //             Ok(replay_id)
    //         }
    //         None => Err(ErrorKind::InstanceNotFound(instance_id)),
    //     }
    // }

    pub(crate) async fn notify_runtime(
        &self,
        _record_id: Uuid,
        _node: String,
        _message: ControlMessage,
    ) -> DaemonResult<()> {
        Err(ErrorKind::Unimplemented)
    }
    pub(crate) async fn check_operator_compatibility(
        &self,
        _operator: SimpleOperatorDescriptor,
    ) -> DaemonResult<bool> {
        Err(ErrorKind::Unimplemented)
    }
    pub(crate) async fn check_source_compatibility(
        &self,
        _source: SourceDescriptor,
    ) -> DaemonResult<bool> {
        Err(ErrorKind::Unimplemented)
    }
    pub(crate) async fn check_sink_compatibility(
        &self,
        _sink: SinkDescriptor,
    ) -> DaemonResult<bool> {
        Err(ErrorKind::Unimplemented)
    }
}
