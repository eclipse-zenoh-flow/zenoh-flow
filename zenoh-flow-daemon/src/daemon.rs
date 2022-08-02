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
use std::fs;
use std::path::Path;

use uhlc::{HLCBuilder, ID};
use uuid::Uuid;
use zenoh::prelude::*;
use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::model::{
    dataflow::record::DataFlowRecord,
    node::{SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor},
};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::dataflow::loader::{
    ExtensibleImplementation, Loader, LoaderConfig, EXT_FILE_EXTENSION,
};
use zenoh_flow::runtime::dataflow::Dataflow;
use zenoh_flow::runtime::message::ControlMessage;
use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::RuntimeClient;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::serde::{Deserialize, Serialize};

use zenoh_flow::runtime::{Runtime, RuntimeConfig, RuntimeInfo, RuntimeStatus, RuntimeStatusKind};
use zenoh_flow::types::{ZFError, ZFResult};
use znrpc_macros::znserver;
use zrpc::ZNServe;

use crate::util::{get_zenoh_config, read_file};

/// The daemon configuration file.
/// The daemon loads this file and uses the informations it contains to
/// generate a (`RuntimeConfig`)[`RuntimeConfig`]

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DaemonConfig {
    /// Where the daemon PID file resides.
    pub pid_file: String,
    /// Where the libraries are downloaded/located
    pub path: String,
    /// Name of the runtime, if None the hostname will be used.
    pub name: Option<String>,
    /// Uuid of the runtime, if None the machine id will be used.
    pub uuid: Option<Uuid>,
    /// Where to find the Zenoh configuration file
    pub zenoh_config: String,
    /// Where to locate the extension files.
    pub extensions: String,
}

/// The internal runtime state.
///
/// It keeps track of running instances and runtime configuration.
pub struct RTState {
    pub graphs: HashMap<Uuid, DataflowInstance>,
    pub config: RuntimeConfig,
}

/// The Zenoh flow daemon
///
/// It keeps track of the state, with an `Arc<RTState>`
/// and the `RuntimeContext`, it has an handle to the `DataStore`
/// for storing/retrieving data from Zenoh.
#[derive(Clone)]
pub struct Daemon {
    pub store: DataStore,
    pub state: Arc<Mutex<RTState>>,
    pub ctx: RuntimeContext,
}

impl Daemon {
    /// Creates a new `Daemon` from the given parameters.
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

    /// The daemon run.
    ///
    /// It starts the zenoh-rpc services.
    /// Sets the status to ready and serves all the requests.
    ///
    /// It stops when receives the stop signal.
    ///
    /// # Errors
    /// Returns an error variant if zenoh-rpc fails.
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

        stop.recv()
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

    /// Starts the daemon.
    ///
    /// It stores the configuration and runtime information in Zenoh.
    ///
    /// The daemon is started on a separated blocking task.
    /// And the stop sender and task handler are returned to the caller.
    ///
    /// # Errors
    /// Returns an error variant if zenoh fails.
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

    /// Stops the daemon.
    ///
    /// Removes information, configuration and status from Zenoh.
    ///
    /// # Errors
    /// Returns an error variant if zenoh fails, or if the stop
    /// channels is disconnected.
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

/// Gets the machine Uuid.
///
/// # Errors
/// Returns an error variant if unable to get or parse the Uuid.
pub fn get_machine_uuid() -> ZFResult<Uuid> {
    let machine_id_raw = machine_uid::get().map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
    let node_str: &str = &machine_id_raw;
    Uuid::parse_str(node_str).map_err(|e| ZFError::ParsingError(format!("{}", e)))
}

/// Creates a new `Daemon` from a configuration file.
///
/// # Errors
/// This function can fail if:
/// - unable to configure zenoh.
/// - unable to get the machine hostname.
/// - unable to get the machine uuid.
/// - unable to open the zenoh session.
impl TryFrom<DaemonConfig> for Daemon {
    type Error = ZFError;

    fn try_from(config: DaemonConfig) -> Result<Self, Self::Error> {
        // If Uuid is not specified uses machine id.
        let uuid = match &config.uuid {
            Some(u) => *u,
            None => get_machine_uuid()?,
        };

        // If name is not specified uses hostname.
        let name = match &config.name {
            Some(n) => n.clone(),
            None => String::from(hostname::get()?.to_str().ok_or(ZFError::GenericError)?),
        };

        // Loading Zenoh configuration
        let zconfig = get_zenoh_config(&config.zenoh_config)?;

        // Generates the loader configuration.
        let mut extensions = LoaderConfig::new();

        let ext_dir = Path::new(&config.extensions);

        // Loading extensions, if an error happens, we do not return
        // instead we log it.
        if ext_dir.is_dir() {
            let ext_dir_entries = fs::read_dir(ext_dir)?;
            for entry in ext_dir_entries {
                match entry {
                    Ok(entry) => {
                        let entry_path = entry.path();
                        if entry_path.is_file() {
                            match entry_path.extension() {
                                Some(entry_ext) => {
                                    if entry_ext != EXT_FILE_EXTENSION {
                                        log::warn!(
                                            "Skipping {} as it does not match the extension {}",
                                            entry_path.display(),
                                            EXT_FILE_EXTENSION
                                        );
                                        continue;
                                    }

                                    // Read the files.

                                    match read_file(&entry_path) {
                                        Ok(ext_file_content) => {
                                            match serde_yaml::from_str::<ExtensibleImplementation>(
                                                &ext_file_content,
                                            ) {
                                                Ok(ext) => {
                                                    match extensions.try_add_extension(ext) {
                                                        Ok(_) => log::info!(
                                                            "Loaded extension {}",
                                                            entry_path.display()
                                                        ),
                                                        Err(e) => log::warn!(
                                                            "Unable to load extension {}: {}",
                                                            entry_path.display(),
                                                            e
                                                        ),
                                                    }
                                                }
                                                Err(e) => log::warn!(
                                                    "Unable to parse extension file {}: {}",
                                                    entry_path.display(),
                                                    e
                                                ),
                                            }
                                        }
                                        Err(e) => log::warn!(
                                            "Unable to read extension file {}: {}",
                                            entry_path.display(),
                                            e
                                        ),
                                    }
                                }
                                None => log::warn!(
                                    "Skipping {} as it as no extension",
                                    entry_path.display()
                                ),
                            }
                        } else {
                            log::warn!("Skipping {} as it is not a file", entry_path.display());
                        }
                    }
                    Err(e) => log::warn!("Unable to access extension file: {}", e),
                }
            }
        } else {
            log::warn!(
                "The extension parameter: {} is not a directory",
                ext_dir.display()
            );
        }

        // Generates the RuntimeConfig
        let rt_config = RuntimeConfig {
            pid_file: config.pid_file,
            path: config.path,
            name,
            uuid,
            zenoh: zconfig.clone(),
            loader: extensions.clone(),
        };

        // Creates the zenoh session.
        let session = Arc::new(zenoh::open(zconfig).wait()?);

        // Creates the HLC.
        let uhlc_id = ID::try_from(uuid.as_bytes())
            .map_err(|e| ZFError::InvalidData(format!("Unable to create ID {e:?}")))?;
        let hlc = Arc::new(HLCBuilder::new().with_id(uhlc_id).build());

        // Creates the loader.
        let loader = Arc::new(Loader::new(extensions));

        let ctx = RuntimeContext {
            session: session.clone(),
            hlc,
            loader,
            runtime_name: rt_config.name.clone().into(),
            runtime_uuid: uuid,
        };

        Ok(Self::new(session, ctx, rt_config))
    }
}

#[znserver]
impl Runtime for Daemon {
    async fn create_instance(&self, flow: FlattenDataFlowDescriptor) -> ZFResult<DataFlowRecord> {
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

    async fn delete_instance(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
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

    async fn instantiate(&self, flow: FlattenDataFlowDescriptor) -> ZFResult<DataFlowRecord> {
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

    async fn teardown(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        log::info!("Tearing down Instance UUID: {}", record_id);

        // Stopping
        Runtime::stop_instance(self, record_id).await?;

        // Clean-up
        let dfr = Runtime::delete_instance(self, record_id).await?;

        log::info!("Done teardown down Instance UUID: {}", record_id);

        Ok(dfr)
    }

    async fn prepare(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
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
    async fn clean(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
        log::info!("Cleaning for Instance UUID: {}", record_id);

        let mut _state = self.state.lock().await;
        let data = _state.graphs.remove(&record_id);
        match data {
            Some(mut dfg) => {
                // Calling finalize on all nodes of a the graph.

                let mut sources = dfg.get_sources();
                for id in sources.drain(..) {
                    dfg.clean_node(&id).await.map_or_else(
                        |e| {
                            log::error!("Unable to clean source {}, got error: {}", &id, e);
                        },
                        |_| (),
                    );
                }

                let mut sinks = dfg.get_sinks();
                for id in sinks.drain(..) {
                    dfg.clean_node(&id).await.map_or_else(
                        |e| {
                            log::error!("Unable to clean sink {}, got error: {}", &id, e);
                        },
                        |_| (),
                    );
                }

                let mut operators = dfg.get_operators();
                for id in operators.drain(..) {
                    dfg.clean_node(&id).await.map_or_else(
                        |e| {
                            log::error!("Unable to operator source {}, got error: {}", &id, e);
                        },
                        |_| (),
                    );
                }

                let mut connectors = dfg.get_connectors();
                for id in connectors.drain(..) {
                    dfg.clean_node(&id).await.map_or_else(
                        |e| {
                            log::error!("Unable to clean connector {}, got error: {}", &id, e);
                        },
                        |_| (),
                    );
                }

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

    async fn start_instance(&self, record_id: Uuid) -> ZFResult<()> {
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

    async fn stop_instance(&self, record_id: Uuid) -> ZFResult<DataFlowRecord> {
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

    // async fn start_record(&self, instance_id: Uuid, source_id: NodeId) -> ZFResult<String> {
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
    //         None => Err(ZFError::InstanceNotFound(instance_id)),
    //     }
    // }

    // async fn stop_record(&self, instance_id: Uuid, source_id: NodeId) -> ZFResult<String> {
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
    //         None => Err(ZFError::InstanceNotFound(instance_id)),
    //     }
    // }

    // async fn start_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     key_expr: String,
    // ) -> ZFResult<NodeId> {
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
    //                 Err(ZFError::InvalidState)
    //             }
    //         }
    //         None => Err(ZFError::InstanceNotFound(instance_id)),
    //     }
    // }

    // async fn stop_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     replay_id: NodeId,
    // ) -> ZFResult<NodeId> {
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
    //         None => Err(ZFError::InstanceNotFound(instance_id)),
    //     }
    // }

    async fn notify_runtime(
        &self,
        record_id: Uuid,
        node: String,
        message: ControlMessage,
    ) -> ZFResult<()> {
        Err(ZFError::Unimplemented)
    }
    async fn check_operator_compatibility(
        &self,
        operator: SimpleOperatorDescriptor,
    ) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
    async fn check_source_compatibility(&self, source: SourceDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
    async fn check_sink_compatibility(&self, sink: SinkDescriptor) -> ZFResult<bool> {
        Err(ZFError::Unimplemented)
    }
}
