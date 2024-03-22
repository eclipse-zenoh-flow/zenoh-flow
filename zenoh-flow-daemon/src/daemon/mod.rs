//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

mod configuration;
mod queryables;

#[cfg(not(feature = "plugin"))]
pub use configuration::ZenohConfiguration;
pub use configuration::{ExtensionsConfiguration, ZenohFlowConfiguration};
pub use zenoh_flow_runtime::{Extension, Extensions, Runtime};

use flume::{Receiver, Sender};
use std::sync::Arc;
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{try_parse_from_file, Result, Vars};

/// A Zenoh-Flow daemon declares 2 queryables:
/// 1. `zenoh-flow/<uuid>/runtime`
/// 2. `zenoh-flow/<uuid>/instances`
const NUMBER_QUERYABLES: usize = 2;

pub struct Daemon {
    abort_tx: Sender<()>,
    abort_ack_rx: Receiver<()>,
}

impl Daemon {
    /// Spawn a new Zenoh-Flow Daemon with its [runtime] configured via a [configuration].
    ///
    /// Note that this configuration can be parsed from a file. This function is for instance leveraged by the Zenoh
    /// plugin for Zenoh-Flow: the configuration of Zenoh-Flow is parsed from Zenoh's configuration file.
    ///
    /// # Errors
    ///
    /// This function can fail for the following reasons:
    /// - the [extensions] section in the configuration points to a file and that file is not a valid declaration of
    ///   [extensions],
    /// - the [extensions] could not be added to the Runtime (see the list of potential reasons
    ///   [here](zenoh_flow_runtime::RuntimeBuilder::add_extensions())),
    /// - if the feature `plugin` is disabled (it is by default):
    ///   - the Zenoh
    ///
    /// [extensions]: Extensions
    /// [runtime]: Runtime
    /// [configuration]: ZenohFlowConfiguration
    pub async fn spawn_from_config(
        #[cfg(feature = "plugin")] zenoh_session: Arc<Session>,
        configuration: ZenohFlowConfiguration,
    ) -> Result<Self> {
        #[cfg(not(feature = "plugin"))]
        let zenoh_config = match configuration.zenoh {
            ZenohConfiguration::File(path) => {
                zenoh::prelude::Config::from_file(path).map_err(|e| anyhow::anyhow!("{e:?}"))
            }
            ZenohConfiguration::Configuration(config) => Ok(config),
        }?;

        #[cfg(not(feature = "plugin"))]
        let zenoh_session = zenoh::open(zenoh_config)
            .res()
            .await
            .map(|session| session.into_arc())
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        let extensions = if let Some(extensions) = configuration.extensions {
            match extensions {
                ExtensionsConfiguration::File(path) => {
                    try_parse_from_file::<Extensions>(path, Vars::default()).map(|(ext, _)| ext)
                }
                ExtensionsConfiguration::Extensions(extensions) => Ok(extensions),
            }?
        } else {
            Extensions::default()
        };

        let runtime = Runtime::builder(configuration.name)
            .add_extensions(extensions)?
            .session(zenoh_session)
            .build()
            .await?;

        Daemon::spawn(runtime).await
    }

    /// Starts the Zenoh-Flow daemon.
    ///
    /// - creates a Zenoh-Flow runtime,
    /// - spawns 2 queryables to serve external requests on the runtime:
    ///    1. `zenoh-flow/<uuid>/runtime`: for everything that relates to the runtime.
    ///    2. `zenoh-flow/<uuid>/instances`: for everything that relates to the data flow instances.
    pub async fn spawn(runtime: Runtime) -> Result<Self> {
        // Channels to gracefully stop the Zenoh-Flow daemon:
        // - `abort_?x` to tell the queryables that they have to stop,
        // - `abort_ack_?x` for the queryables to inform the runtime that they did stop.
        let (abort_tx, abort_rx) = flume::bounded::<()>(NUMBER_QUERYABLES);
        let (abort_ack_tx, abort_ack_rx) = flume::bounded::<()>(NUMBER_QUERYABLES);

        let runtime = Arc::new(runtime);

        let session = runtime.session();
        let abort = abort_rx.clone();
        let abort_ack = abort_ack_tx.clone();

        if let Err(e) =
            queryables::spawn_runtime_queryable(session.clone(), runtime.clone(), abort, abort_ack)
                .await
        {
            tracing::error!(
                "The Zenoh-Flow daemon encountered a fatal error:\n{:?}\nAborting",
                e
            );
            // TODO: Clean everything up before aborting.
        }

        if let Err(e) =
            queryables::spawn_instances_queryable(session, runtime.clone(), abort_rx, abort_ack_tx)
                .await
        {
            tracing::error!(
                "The Zenoh-Flow daemon encountered a fatal error:\n{:?}\nAborting",
                e
            );
            // TODO: Clean everything up before aborting.
        }

        Ok(Daemon {
            abort_tx,
            abort_ack_rx,
        })
    }

    /// Gracefully stops the Zenoh-Flow daemon.
    ///
    /// This method will first stop the queryables this daemon declared (to not process new requests) and then stop all
    /// the data flow instances that are running.
    pub async fn stop(&self) {
        for iteration in 0..NUMBER_QUERYABLES {
            tracing::trace!(
                "Sending abort signal to queryable ({}/{})",
                iteration + 1,
                NUMBER_QUERYABLES
            );
            self.abort_tx.send_async(()).await.unwrap_or_else(|e| {
                tracing::error!(
                    "Failed to send abort signal to queryable ({}/{}): {:?}",
                    iteration + 1,
                    NUMBER_QUERYABLES,
                    e
                );
            });
        }

        // TODO: Abort all the operations on the runtime.
        // self._runtime.abort();

        // TODO Introduce a timer: if, for whatever reason, a queryable fails to send an acknowledgment we should not
        // block the stopping procedure.
        //
        // Maybe wait for 60 seconds maximum?
        for iteration in 0..NUMBER_QUERYABLES {
            self.abort_ack_rx.recv_async().await.unwrap_or_else(|e| {
                tracing::error!(
                    "Failed to receive abort acknowledgment ({}/{}): {:?}",
                    iteration + 1,
                    NUMBER_QUERYABLES,
                    e
                );
            });
            tracing::trace!(
                "Received abort acknowledgment {}/{}",
                iteration + 1,
                NUMBER_QUERYABLES
            );
        }
    }
}
