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

use crate::CZFResult;
use crate::Registry;
use std::convert::TryFrom;

use futures::prelude::*;
use futures::select;

use zenoh::net::Session as ZSession;
use zenoh::net::*;
use zenoh::ZFuture;

use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::model::dataflow::DataFlowDescriptor;
use zenoh_flow::OperatorId;

use zenoh_flow::model::{
    component::{OperatorDescriptor, SinkDescriptor, SourceDescriptor},
    RegistryComponent,
};

use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::ZenohConfig;
use zenoh_flow::serde::{Deserialize, Serialize};
use zenoh_flow::types::{ZFError, ZFResult};

use zenoh_cdn::server::Server;
use zenoh_cdn::types::ServerConfig;

use znrpc_macros::znserver;
use zrpc::ZNServe;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegistryConfig {
    pub pid_file: String,
    pub path: String,
    pub zenoh: ZenohConfig,
    pub cdn: ServerConfig,
}

impl TryFrom<String> for RegistryConfig {
    type Error = ZFError;

    fn try_from(d: String) -> Result<Self, Self::Error> {
        serde_yaml::from_str::<Self>(&d).map_err(|e| ZFError::ParsingError(format!("{}", e)))
    }
}

#[derive(Debug)]
pub struct RegistryState {
    pub config: RegistryConfig,
}

#[derive(Clone)]
pub struct ZFRegistry {
    pub zn: Arc<ZSession>,
    pub store: DataStore,
    pub state: Arc<Mutex<RegistryState>>,
    pub cdn_server: Server,
}

impl ZFRegistry {
    pub fn new(zn: Arc<ZSession>, z: Arc<zenoh::Zenoh>, config: RegistryConfig) -> Self {
        let state = Arc::new(Mutex::new(RegistryState {
            config: config.clone(),
        }));
        Self {
            zn,
            store: DataStore::new(z.clone()),
            state,
            cdn_server: Server::new(z, config.cdn),
        }
    }

    pub async fn run(&self, stop: async_std::channel::Receiver<()>) -> ZFResult<()> {
        log::info!("Registry main loop starting");

        let reg_server = self.clone().get_registry_server(self.zn.clone(), None);
        let (reg_stopper, _hrt) = reg_server.connect().await?;

        reg_server.initialize().await?;

        reg_server.register().await?;

        log::trace!("Starting Zenoh-CDN Server");
        let _cdn_handler = self.cdn_server.serve();

        log::trace!("Staring ZRPC Servers");
        let (srt, _hrt) = reg_server.start().await?;

        let run_loop = async {
            //Setting up local metadata storage
            let sub_info = SubInfo {
                reliability: Reliability::Reliable,
                mode: SubMode::Push,
                period: None,
            };
            let db_path = std::path::PathBuf::from("/tmp/zf-registry.sqlite");
            let db = rusqlite::Connection::open(&db_path)?;

            db.execute(
                "CREATE TABLE data (key STRING PRIMARY KEY NOT NULL, value BLOB NOT NULL)",
                [],
            )?;

            let resource_space = ResKey::from("/zenoh-flow/registry/**".to_string());
            let mut subscriber = self
                .zn
                .declare_subscriber(&resource_space, &sub_info)
                .await?;
            let sample_stream = subscriber.receiver();
            let mut queryable = self
                .zn
                .declare_queryable(&resource_space, zenoh::net::queryable::EVAL)
                .await?;
            let query_steam = queryable.receiver();

            loop {
                select!(
                    sample = sample_stream.next().fuse() => {
                        log::debug!("Received sample {:?}", sample);
                    }
                    query = query_steam.next().fuse() => {
                        log::debug!("Received query {:?}", query)
                    }
                    _ = stop.recv().fuse() => {
                        break Ok(());
                    }
                )
            }
        };

        // let _ = stop
        //     .recv()
        //     .await
        //     .map_err(|e| ZFError::RecvError(format!("{}", e)))?;
        let _: CZFResult<()> = run_loop.await;

        reg_server.stop(srt).await?;

        reg_server.unregister().await?;

        reg_server.disconnect(reg_stopper).await?;

        log::info!("Registry main loop exiting...");
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

        let h = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async { rt.run(r).await })
        });
        Ok((s, h))
    }

    pub async fn stop(&self, stop: async_std::channel::Sender<()>) -> ZFResult<()> {
        stop.send(())
            .await
            .map_err(|e| ZFError::SendError(format!("{}", e)))?;

        Ok(())
    }
}

impl TryFrom<RegistryConfig> for ZFRegistry {
    type Error = ZFError;

    fn try_from(config: RegistryConfig) -> Result<Self, Self::Error> {
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

        Ok(Self::new(zn, z, config))
    }
}

#[znserver]
impl Registry for ZFRegistry {
    async fn get_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor> {
        Err(ZFError::Unimplemented)
    }

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_all_graphs(&self) -> ZFResult<Vec<RegistryComponent>> {
        self.store.get_all_graphs().await
    }

    async fn get_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
        os: String,
        arch: String,
    ) -> ZFResult<OperatorDescriptor> {
        let metadata = self.store.get_graph(&operator_id).await?;

        let tag = tag.unwrap_or_else(|| String::from("latest"));

        let filtered_metadata = || {
            for m_tag in metadata.clone().tags {
                if m_tag.name == tag {
                    for m_arch in m_tag.architectures {
                        if m_arch.os == os && m_arch.arch == arch {
                            return Ok(m_arch);
                        }
                    }
                }
            }
            Err(ZFError::OperatorNotFound(operator_id))
        };

        let specific_metadata = filtered_metadata()?;

        let descriptor = OperatorDescriptor {
            id: metadata.id.clone(),
            inputs: metadata.inputs.clone(),
            outputs: metadata.outputs.clone(),
            uri: Some(specific_metadata.uri),
            configuration: None,
            runtime: None,
        };

        Ok(descriptor)
    }

    async fn get_sink(&self, sink_id: OperatorId, tag: Option<String>) -> ZFResult<SinkDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn get_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor> {
        Err(ZFError::Unimplemented)
    }

    // async fn remove_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn remove_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<OperatorDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_sink(
        &self,
        sink_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SinkDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn add_flow(&self, flow: DataFlowDescriptor) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }

    async fn add_graph(&self, graph: RegistryComponent) -> ZFResult<OperatorId> {
        log::info!("Adding graph {:?}", graph);
        match self.store.get_graph(&graph.id).await {
            Ok(mut metadata) => {
                // This is an update of something that is already there.
                for tag in graph.tags.into_iter() {
                    metadata.add_tag(tag);
                }
                self.store.add_graph(&metadata).await?;
                Ok(graph.id)
            }
            Err(ZFError::Empty) => {
                // this is a new addition, simple case.

                self.store.add_graph(&graph).await?;
                Ok(graph.id)
            }
            Err(e) => Err(e), //this is any other error so we propagate it
        }
    }

    async fn add_operator(
        &self,
        operator: RegistryComponent,
        tag: Option<String>,
    ) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }

    async fn add_sink(&self, sink: RegistryComponent, tag: Option<String>) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }

    async fn add_source(
        &self,
        source: RegistryComponent,
        tag: Option<String>,
    ) -> ZFResult<OperatorId> {
        Err(ZFError::Unimplemented)
    }
}
