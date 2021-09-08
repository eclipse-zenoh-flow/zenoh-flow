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

use std::convert::TryFrom;

use zenoh::net::Session as ZSession;
use zenoh::ZFuture;
use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::model::dataflow::DataFlowDescriptor;

use zenoh_flow::model::{
    operator::{ZFOperatorDescriptor, ZFSinkDescriptor, ZFSourceDescriptor},
    ZFRegistryGraph,
};

use zenoh_flow::runtime::resources::ZFDataStore;
use zenoh_flow::serde::{Deserialize, Serialize};

use crate::ZFRegistry;
use zenoh_flow::runtime::ZenohConfig;
use zenoh_flow::types::{ZFError, ZFResult};

use znrpc_macros::znserver;
use zrpc::ZNServe;

#[derive(Serialize, Deserialize, Debug)]
pub struct RegistryConfig {
    pub pid_file: String,
    pub path: String,
    pub zenoh: ZenohConfig,
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
pub struct Registry {
    pub zn: Arc<ZSession>,
    pub store: ZFDataStore,
    pub state: Arc<Mutex<RegistryState>>,
}

impl Registry {
    pub fn new(zn: Arc<ZSession>, z: Arc<zenoh::Zenoh>, config: RegistryConfig) -> Self {
        let state = Arc::new(Mutex::new(RegistryState { config }));
        Self {
            zn,
            store: ZFDataStore::new(z),
            state,
        }
    }

    pub async fn run(&self, stop: async_std::channel::Receiver<()>) -> ZFResult<()> {
        log::info!("Registry main loop starting");

        let reg_server = self.clone().get_zf_registry_server(self.zn.clone(), None);
        let (reg_stopper, _hrt) = reg_server.connect().await?;

        reg_server.initialize().await?;

        reg_server.register().await?;

        log::trace!("Staring ZRPC Servers");
        let (srt, _hrt) = reg_server.start().await?;

        let _ = stop
            .recv()
            .await
            .map_err(|e| ZFError::RecvError(format!("{}", e)))?;

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

impl TryFrom<RegistryConfig> for Registry {
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
impl ZFRegistry for Registry {
    async fn get_flow(&self, flow_id: String) -> ZFResult<DataFlowDescriptor> {
        Err(ZFError::Unimplemented)
    }

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_all_graphs(&self) -> ZFResult<Vec<ZFRegistryGraph>> {
        self.store.get_all_graphs().await
    }

    async fn get_operator(
        &self,
        operator_id: String,
        tag: Option<String>,
        os: String,
        arch: String,
    ) -> ZFResult<ZFOperatorDescriptor> {
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

        let descriptor = ZFOperatorDescriptor {
            id: metadata.id.clone(),
            inputs: metadata.inputs.clone(),
            outputs: metadata.outputs.clone(),
            uri: Some(specific_metadata.uri),
            configuration: None,
            runtime: None,
        };

        Ok(descriptor)
    }

    async fn get_sink(&self, sink_id: String, tag: Option<String>) -> ZFResult<ZFSinkDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn get_source(
        &self,
        source_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFSourceDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_flow(&self, flow_id: String) -> ZFResult<DataFlowDescriptor> {
        Err(ZFError::Unimplemented)
    }

    // async fn remove_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn remove_operator(
        &self,
        operator_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFOperatorDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_sink(
        &self,
        sink_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFSinkDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn remove_source(
        &self,
        source_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFSourceDescriptor> {
        Err(ZFError::Unimplemented)
    }

    async fn add_flow(&self, flow: DataFlowDescriptor) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn add_graph(&self, graph: ZFRegistryGraph) -> ZFResult<String> {
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
        operator: ZFOperatorDescriptor,
        tag: Option<String>,
    ) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn add_sink(&self, sink: ZFSinkDescriptor, tag: Option<String>) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn add_source(
        &self,
        source: ZFSourceDescriptor,
        tag: Option<String>,
    ) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }
}
