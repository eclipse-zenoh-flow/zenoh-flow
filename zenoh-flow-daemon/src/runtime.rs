//
// Copyright (c) 2022 ZettaScale Technology
//
// This progr&am and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::convert::TryFrom;

use crate::daemon::Daemon;
use uuid::Uuid;
use zenoh_flow::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::model::{
    dataflow::record::DataFlowRecord,
    node::{SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor},
};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::dataflow::Dataflow;
use zenoh_flow::runtime::message::ControlMessage;
use zenoh_flow::runtime::{Runtime, RuntimeClient};
use zenoh_flow::zfresult::ErrorKind;
use zenoh_flow::DaemonResult;
use zrpc_macros::znserver;

// Implementation of [`Runtime`](`Runtime`) trait for the Daemon
// This implementation does asynchronous operations, via zrpc/REST.
// The deamon implements the actual logic for each operation.

#[znserver]
impl Runtime for Daemon {
    async fn create_instance(
        &self,
        flow: FlattenDataFlowDescriptor,
    ) -> DaemonResult<DataFlowRecord> {
        //TODO: workaround - it should just take the ID of the flow (when
        // the registry will be in place)
        //TODO: this has to run asynchronously, this means that it must
        // create the record and return it, in order to not block the caller.
        // Creating an instance can involved downloading nodes from different
        // locations, communication towards others runtimes, therefore
        // the caller should not be blocked.
        // The status of an instance can be check asynchronously by the caller
        // once it knows the Uuid.
        // Therefore instantiation errors should be logged as instance status

        let record_uuid = Uuid::new_v4();
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
        let mut dfr = DataFlowRecord::try_from((mapped, record_uuid))?;

        self.store
            .add_runtime_flow(&self.ctx.runtime_uuid, &dfr)
            .await?;

        // Creating clients to talk with other runtimes
        for rt in involved_runtimes {
            let rt_info = self.store.get_runtime_info_by_name(&rt).await?;
            let client = RuntimeClient::new(self.ctx.session.clone(), rt_info.id);
            rt_clients.push(client);
        }

        // remote prepare
        for client in rt_clients.iter() {
            client.prepare(dfr.uuid).await??;
        }

        // self prepare
        Runtime::prepare(self, dfr.uuid).await?;

        log::info!(
            "Created Flow {} - Instance UUID: {}",
            flow_name,
            record_uuid
        );

        Ok(dfr)
    }

    async fn delete_instance(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Delete Instance UUID: {}", record_id);
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

        // remote clean
        for client in rt_clients.iter() {
            client.clean(record_id).await??;
        }

        // local clean
        if is_also_local {
            self.clean(record_id).await;
        }

        self.store
            .remove_runtime_flow_instance(&self.ctx.runtime_uuid, &record.flow, &record.uuid)
            .await?;

        log::info!("Done delete Instance UUID: {}", record_id);

        Ok(record)
    }

    async fn instantiate(&self, flow: FlattenDataFlowDescriptor) -> DaemonResult<DataFlowRecord> {
        //TODO: workaround - it should just take the ID of the flow (when
        // the registry will be in place)
        //TODO: this has to run asynchronously, this means that it must
        // create the record and return it, in order to not block the caller.
        // Creating an instance can involved downloading nodes from different
        // locations, communication towards others runtimes, therefore
        // the caller should not be blocked.
        // The status of an instance can be check asynchronously by the caller
        // once it knows the Uuid.
        // Therefore instantiation errors should be logged as instance status

        log::info!("Instantiating: {}", flow.flow);

        // Creating
        let dfr = Runtime::create_instance(self, flow.clone()).await?;

        // Starting
        Runtime::start_instance(self, dfr.uuid).await?;

        log::info!(
            "Done Instantiation Flow {} - Instance UUID: {}",
            flow.flow,
            dfr.uuid,
        );

        Ok(dfr)
    }

    async fn teardown(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Tearing down Instance UUID: {}", record_id);

        // Stopping
        Runtime::stop_instance(self, record_id).await?;

        // Clean-up
        let dfr = Runtime::delete_instance(self, record_id).await?;

        log::info!("Done teardown down Instance UUID: {}", record_id);

        Ok(dfr)
    }

    async fn prepare(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Preparing for Instance UUID: {}", record_id);

        let dfr = self.store.get_flow_by_instance(&record_id).await?;
        self.store
            .add_runtime_flow(&self.ctx.runtime_uuid, &dfr)
            .await?;

        let mut dataflow = Dataflow::try_new(self.ctx.clone(), dfr.clone())?;
        let mut instance = DataflowInstance::try_instantiate(dataflow, self.ctx.hlc.clone())?;

        let mut self_state = self.state.lock().await;
        self_state.graphs.insert(dfr.uuid, instance);
        drop(self_state);

        log::info!("Done preparation for Instance UUID: {}", record_id);

        Ok(dfr)
    }
    async fn clean(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Cleaning for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let data = _state.graphs.remove(&record_id);
        match data {
            Some(mut dfg) => {
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

    async fn start_instance(&self, record_id: Uuid) -> DaemonResult<()> {
        log::info!("Staring Instance UUID: {}", record_id);

        let mut rt_clients = vec![];

        let all_involved_runtimes = self.store.get_flow_instance_runtimes(&record_id).await?;

        let is_also_local = all_involved_runtimes.contains(&self.ctx.runtime_uuid);

        let all_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_uuid);

        for rt in all_involved_runtimes {
            let client = RuntimeClient::new(self.ctx.session.clone(), rt);
            rt_clients.push(client);
        }

        // remote start
        for client in rt_clients.iter() {
            client.start(record_id).await??;
        }

        if is_also_local {
            // self start
            Runtime::start(self, record_id).await?;
        }

        // remote start sources
        for client in rt_clients.iter() {
            client.start_sources(record_id).await??;
        }

        if is_also_local {
            // self start sources
            Runtime::start_sources(self, record_id).await?;
        }

        log::info!("Started Instance UUID: {}", record_id);

        Ok(())
    }

    async fn stop_instance(&self, record_id: Uuid) -> DaemonResult<DataFlowRecord> {
        log::info!("Stopping Instance UUID: {}", record_id);
        let record = self.store.get_flow_by_instance(&record_id).await?;

        let mut rt_clients = vec![];

        let all_involved_runtimes = self.store.get_flow_instance_runtimes(&record_id).await?;

        let is_also_local = all_involved_runtimes.contains(&self.ctx.runtime_uuid);

        let all_involved_runtimes = all_involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.ctx.runtime_uuid);

        for rt in all_involved_runtimes {
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

        log::info!("Stopped Instance UUID: {}", record_id);

        Ok(record)
    }

    async fn start(&self, record_id: Uuid) -> DaemonResult<()> {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }
    async fn start_sources(&self, record_id: Uuid) -> DaemonResult<()> {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }
    async fn stop(&self, record_id: Uuid) -> DaemonResult<()> {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }
    async fn stop_sources(&self, record_id: Uuid) -> DaemonResult<()> {
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
            None => Err(ErrorKind::InstanceNotFound(record_id)),
        }
    }
    async fn start_node(&self, instance_id: Uuid, node: String) -> DaemonResult<()> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(mut instance) => Ok(instance.start_node(&node.into()).await?),
            None => Err(ErrorKind::InstanceNotFound(instance_id)),
        }
    }
    async fn stop_node(&self, instance_id: Uuid, node: String) -> DaemonResult<()> {
        let mut _state = self.state.lock().await;
        let mut rt_status = self
            .store
            .get_runtime_status(&self.ctx.runtime_uuid)
            .await?;

        match _state.graphs.get_mut(&instance_id) {
            Some(mut instance) => Ok(instance.stop_node(&node.into()).await?),
            None => Err(ErrorKind::InstanceNotFound(instance_id)),
        }
    }

    // async fn start_record(&self, instance_id: Uuid, source_id: NodeId) -> DaemonResult<String> {
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

    // async fn stop_record(&self, instance_id: Uuid, source_id: NodeId) -> DaemonResult<String> {
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

    // async fn start_replay(
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

    // async fn stop_replay(
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

    async fn notify_runtime(
        &self,
        record_id: Uuid,
        node: String,
        message: ControlMessage,
    ) -> DaemonResult<()> {
        Err(ErrorKind::Unimplemented)
    }
    async fn check_operator_compatibility(
        &self,
        operator: SimpleOperatorDescriptor,
    ) -> DaemonResult<bool> {
        Err(ErrorKind::Unimplemented)
    }
    async fn check_source_compatibility(&self, source: SourceDescriptor) -> DaemonResult<bool> {
        Err(ErrorKind::Unimplemented)
    }
    async fn check_sink_compatibility(&self, sink: SinkDescriptor) -> DaemonResult<bool> {
        Err(ErrorKind::Unimplemented)
    }
}
