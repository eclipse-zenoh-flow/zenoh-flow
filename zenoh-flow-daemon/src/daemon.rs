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

use std::convert::TryFrom;
use std::fs;
use std::path::Path;

use async_std::sync::RwLock;
// use futures::stream::{AbortHandle, Abortable, Aborted};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uhlc::{HLCBuilder, ID};
use uuid::Uuid;

use zenoh_flow::model::descriptor::{
    FlattenDataFlowDescriptor, OperatorDescriptor, SinkDescriptor, SourceDescriptor,
};
use zenoh_flow::model::record::DataFlowRecord;
use zenoh_flow::prelude::{zferror, ErrorKind, Result as ZFResult};

use zenoh_flow::runtime::dataflow::loader::{
    ExtensibleImplementation, Loader, LoaderConfig, EXT_FILE_EXTENSION,
};

use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::worker_pool::{WorkerPool, WorkerTrait};
use zenoh_flow::runtime::{
    DaemonInterface, DaemonInterfaceInternal, RuntimeConfig, RuntimeContext,
};
use zenoh_flow::types::ControlMessage;
use zenoh_flow::utils::{deserialize_size, deserialize_time};
use zenoh_flow::{
    bail, DaemonResult, DEFAULT_SHM_ALLOCATION_BACKOFF_NS, DEFAULT_SHM_ELEMENT_SIZE,
    DEFAULT_SHM_TOTAL_ELEMENTS, DEFAULT_USE_SHM,
};
use zrpc::ZServe;
use zrpc_macros::zserver;

use crate::runtime::Runtime;
use crate::util::{get_zenoh_config, read_file};
use crate::worker::Worker;

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
    pub zenoh_config: Option<String>,
    /// Where to locate the extension files.
    pub extensions: String,
    /// The size of the worker pool.
    pub worker_pool_size: usize,
    /// The default size of the shared memory element.
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_size")]
    pub default_shared_memory_element_size: Option<usize>,
    /// The default number of the shared memory elements.
    pub default_shared_memory_elements: Option<usize>,
    /// The default backoff when shared memory elements are not available.
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_time")]
    pub default_shared_memory_backoff: Option<u64>,
    // Whether or not Shared Memory is enabled.
    pub use_shm: Option<bool>,
}

/// The Zenoh flow daemon
///
/// It keeps track of the state, with an `Arc<RTState>`
/// and the `RuntimeContext`, it has an handle to the `DataStore`
/// for storing/retrieving data from Zenoh.
#[derive(Clone)]
pub struct Daemon {
    runtime: Runtime,
    worker_pool: Arc<RwLock<WorkerPool>>,
    ctx: RuntimeContext,
}

/// Gets the machine Uuid.
///
/// # Errors
/// Returns an error variant if unable to get or parse the Uuid.
pub fn get_machine_uuid() -> ZFResult<Uuid> {
    let machine_id_raw =
        machine_uid::get().map_err(|e| zferror!(ErrorKind::ParsingError, "{}", e))?;
    let node_str: &str = &machine_id_raw;
    Uuid::parse_str(node_str).map_err(|e| zferror!(ErrorKind::ParsingError, e).into())
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
    type Error = zenoh_flow::prelude::Error;

    fn try_from(config: DaemonConfig) -> std::result::Result<Self, Self::Error> {
        use zenoh::prelude::sync::*;

        if let Some(zenoh_config) = &config.zenoh_config {
            // Loading Zenoh configuration
            let zconfig = get_zenoh_config(zenoh_config)?;

            // Creates the zenoh session.
            let session = Arc::new(zenoh::open(zconfig).res()?);

            return Self::from_session_and_config(session, config);
        }

        bail!(
            ErrorKind::ConfigurationError,
            "zenoh_config is required when running as standalone daemon!!"
        )
    }
}

impl Daemon {
    /// Creates a new `Daemon` from the given parameters.
    pub fn new(
        z: Arc<zenoh::Session>,
        ctx: RuntimeContext,
        config: RuntimeConfig,
        pool_size: usize,
    ) -> Self {
        let store = DataStore::new(z.clone());

        let runtime = Runtime::new(z, ctx.clone(), config.clone());

        let c_runtime = runtime.clone();
        let new_worker = Arc::new(move |id, rx, hlc| {
            Box::new(Worker::new(id, rx, hlc, c_runtime)) as Box<dyn WorkerTrait>
        });

        let mut workers =
            WorkerPool::new(pool_size, store, config.uuid, ctx.hlc.clone(), new_worker);
        workers.start();

        Self {
            runtime,
            worker_pool: Arc::new(RwLock::new(workers)),
            ctx,
        }
    }

    pub fn from_session_and_config(z: Arc<zenoh::Session>, config: DaemonConfig) -> ZFResult<Self> {
        // If Uuid is not specified uses machine id.
        let uuid = match &config.uuid {
            Some(u) => *u,
            None => get_machine_uuid()?,
        };

        // If name is not specified uses hostname.
        let name = match &config.name {
            Some(n) => n.clone(),
            None => String::from(
                hostname::get()?
                    .to_str()
                    .ok_or_else(|| zferror!(ErrorKind::GenericError))?,
            ),
        };

        let pool_size = config.worker_pool_size;

        if pool_size == 0 {
            bail!(
                ErrorKind::ConfigurationError,
                "worker_pool_size cannot be 0"
            )
        }

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
            loader: extensions.clone(),
        };

        // Creates the HLC.
        let uhlc_id = ID::try_from(uuid.as_bytes())
            .map_err(|e| zferror!(ErrorKind::InvalidData, "Unable to create ID {:?}", e))?;
        let hlc = Arc::new(HLCBuilder::new().with_id(uhlc_id).build());

        // Creates the loader.
        let loader = Arc::new(Loader::new(extensions));

        let ctx = RuntimeContext {
            session: z.clone(),
            hlc,
            loader,
            runtime_name: rt_config.name.clone().into(),
            runtime_uuid: uuid,
            shared_memory_element_size: config
                .default_shared_memory_element_size
                .unwrap_or(DEFAULT_SHM_ELEMENT_SIZE as usize),
            shared_memory_elements: config
                .default_shared_memory_elements
                .unwrap_or(DEFAULT_SHM_TOTAL_ELEMENTS as usize),
            shared_memory_backoff: config
                .default_shared_memory_backoff
                .unwrap_or(DEFAULT_SHM_ALLOCATION_BACKOFF_NS),
            use_shm: config.use_shm.unwrap_or(DEFAULT_USE_SHM),
        };

        Ok(Self::new(z, ctx, rt_config, pool_size))
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

        let daemon_server = self
            .clone()
            .get_daemon_interface_server(self.ctx.session.clone(), Some(self.ctx.runtime_uuid));
        let (daemon_stopper, _hdaemon) = daemon_server
            .connect()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        daemon_server
            .initialize()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        daemon_server
            .register()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;

        let rt_server = self.clone().get_daemon_interface_internal_server(
            self.ctx.session.clone(),
            Some(self.ctx.runtime_uuid),
        );
        let (rt_stopper, _hrt) = rt_server
            .connect()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        rt_server
            .initialize()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        rt_server
            .register()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;

        log::trace!("Staring ZRPC Servers");

        // Starting internal server (to other daemons)
        let (srt, _hrt) = rt_server
            .start()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;

        // Starting daemon server (to client apis)
        let (drt, _hdaemon) = daemon_server
            .start()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;

        log::trace!("Setting state as Ready");

        self.runtime.ready().await?;

        log::trace!("Running...");

        stop.recv()
            .await
            .map_err(|e| zferror!(ErrorKind::RecvError, e))?;

        rt_server
            .stop(srt)
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        rt_server
            .unregister()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        rt_server
            .disconnect(rt_stopper)
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;

        daemon_server
            .stop(drt)
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        daemon_server
            .unregister()
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;
        daemon_server
            .disconnect(daemon_stopper)
            .await
            .map_err(|e| zferror!(ErrorKind::RPCError, e))?;

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
        //@TODO: use Abortable and AbortHandle here.
        // Currently is not in place because it does not compile.
        // Further investigation in needed.
        // AbortHandle,
        // async_std::task::JoinHandle<Result<ZFResult<()>, Aborted>>,
        // let (abort_handle, abort_registration) = AbortHandle::new_pair();
        // let run_future = async { daemon.run(r).await };
        // let handle = async_std::task::spawn(Abortable::new(run_future, abort_registration));
        // Ok((abort_handle, handle))

        let (s, r) = async_std::channel::bounded::<()>(1);

        let daemon = self.clone();

        self.runtime.start().await?;

        let handle = async_std::task::spawn_blocking(move || {
            async_std::task::block_on(async { daemon.run(r).await })
        });

        Ok((s, handle))
    }

    /// Stops the daemon.
    ///
    /// Removes information, configuration and status from Zenoh.
    ///
    /// # Errors
    /// Returns an error variant if zenoh fails, or if the stop
    /// channels is disconnected.
    pub async fn stop(
        &self,
        // stop: AbortHandle
        stop: async_std::channel::Sender<()>,
    ) -> ZFResult<()> {
        // Taking the lock to stop the workers,
        // stopping them and releasing the lock
        let mut workers = self.worker_pool.write().await;
        workers.stop().await;
        drop(workers);

        // Stop the server
        stop.send(())
            .await
            .map_err(|e| zferror!(ErrorKind::SendError, e))?;
        // stop.abort();

        // Stop the runtime
        self.runtime.stop().await?;

        Ok(())
    }
}

// Implementation of [`DaemonInterface`](`DaemonInterface`) trait for the Daemon
// This implementation does asynchronous operations, via zrpc/REST.
// The runtime implements the actual logic for each operation.

#[zserver]
impl DaemonInterface for Daemon {
    async fn create_instance(&self, flow: FlattenDataFlowDescriptor) -> DaemonResult<Uuid> {
        let instance_uuid = Uuid::new_v4();

        let res = self
            .worker_pool
            .read()
            .await
            .submit_create(&flow, &instance_uuid)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Creating instance < {} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_uuid,
        );

        Ok(instance_uuid)
    }

    async fn delete_instance(&self, instance_id: Uuid) -> DaemonResult<DataFlowRecord> {
        let record = self
            .runtime
            .store
            .get_flow_by_instance(&instance_id)
            .await?;

        let res = self
            .worker_pool
            .read()
            .await
            .submit_delete(&instance_id)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Deleting instance < {} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_id,
        );

        Ok(record)
    }

    async fn instantiate(&self, flow: FlattenDataFlowDescriptor) -> DaemonResult<Uuid> {
        let instance_uuid = Uuid::new_v4();

        let res = self
            .worker_pool
            .read()
            .await
            .submit_instantiate(&flow, &instance_uuid)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Instantiating flow < {} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_uuid,
        );

        Ok(instance_uuid)
    }

    async fn teardown(&self, instance_id: Uuid) -> DaemonResult<DataFlowRecord> {
        let record = self
            .runtime
            .store
            .get_flow_by_instance(&instance_id)
            .await?;

        let res = self
            .worker_pool
            .read()
            .await
            .submit_teardown(&instance_id)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Teardown flow < {} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_id,
        );

        Ok(record)
    }

    async fn start_instance(&self, instance_id: Uuid) -> DaemonResult<()> {
        let res = self
            .worker_pool
            .read()
            .await
            .submit_start(&instance_id)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Start instance < {} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_id,
        );

        Ok(())
    }

    async fn stop_instance(&self, instance_id: Uuid) -> DaemonResult<DataFlowRecord> {
        let record = self
            .runtime
            .store
            .get_flow_by_instance(&instance_id)
            .await?;

        let res = self
            .worker_pool
            .read()
            .await
            .submit_stop(&instance_id)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Stop instance < {} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_id,
        );

        Ok(record)
    }

    async fn start_node(&self, instance_id: Uuid, node: String) -> DaemonResult<()> {
        let res = self
            .worker_pool
            .read()
            .await
            .submit_start_node(&instance_id, &node)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Start node < {}:{} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_id,
            node,
        );

        Ok(())
    }
    async fn stop_node(&self, instance_id: Uuid, node: String) -> DaemonResult<()> {
        let res = self
            .worker_pool
            .read()
            .await
            .submit_stop_node(&instance_id, &node)
            .await?;
        log::info!(
            "[Daemon: {}][Job: {}] Stop node < {}:{} >",
            self.ctx.runtime_uuid,
            res.get_id(),
            instance_id,
            node,
        );

        Ok(())
    }

    // async fn start_record(&self, instance_id: Uuid, source_id: NodeId) -> DaemonResult<String> {
    //     Err(ErrorKind::Unimplemented)
    // }

    // async fn stop_record(&self, instance_id: Uuid, source_id: NodeId) -> DaemonResult<String> {
    //     Err(ErrorKind::Unimplemented)
    // }

    // async fn start_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     key_expr: String,
    // ) -> DaemonResult<NodeId> {
    //     Err(ErrorKind::Unimplemented)
    // }

    // async fn stop_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     replay_id: NodeId,
    // ) -> DaemonResult<NodeId> {
    //     Err(ErrorKind::Unimplemented)
    // }
}

#[zserver]
impl DaemonInterfaceInternal for Daemon {
    async fn prepare(&self, instance_id: Uuid) -> DaemonResult<DataFlowRecord> {
        self.runtime.prepare(instance_id).await
    }

    async fn clean(&self, instance_id: Uuid) -> DaemonResult<DataFlowRecord> {
        self.runtime.clean(instance_id).await
    }

    async fn start(&self, instance_id: Uuid) -> DaemonResult<()> {
        self.runtime.start_nodes(instance_id).await
    }

    async fn start_sources(&self, instance_id: Uuid) -> DaemonResult<()> {
        self.runtime.start_sources(instance_id).await
    }

    async fn stop(&self, instance_id: Uuid) -> DaemonResult<()> {
        self.runtime.stop_nodes(instance_id).await
    }

    async fn stop_sources(&self, instance_id: Uuid) -> DaemonResult<()> {
        self.runtime.stop_sources(instance_id).await
    }

    async fn notify_runtime(
        &self,
        instance_id: Uuid,
        node: String,
        message: ControlMessage,
    ) -> DaemonResult<()> {
        self.runtime
            .notify_runtime(instance_id, node, message)
            .await
    }

    async fn check_operator_compatibility(
        &self,
        operator: OperatorDescriptor,
    ) -> DaemonResult<bool> {
        self.runtime.check_operator_compatibility(operator).await
    }

    async fn check_source_compatibility(&self, source: SourceDescriptor) -> DaemonResult<bool> {
        self.runtime.check_source_compatibility(source).await
    }

    async fn check_sink_compatibility(&self, sink: SinkDescriptor) -> DaemonResult<bool> {
        self.runtime.check_sink_compatibility(sink).await
    }
}
