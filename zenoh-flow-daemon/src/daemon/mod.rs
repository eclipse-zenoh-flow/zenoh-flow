//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

//! The Zenoh-Flow Daemon and re-exports needed to create one.
//!
//! A Zenoh-Flow Daemon wraps a Zenoh-Flow [runtime] and exposes queryables to remotely interact and manage it. This
//! module thus additionally re-exports the structures needed to create one.
//!
//! [runtime]: zenoh_flow_runtime::Runtime

mod configuration;
mod queryables;

use std::sync::Arc;

use flume::{Receiver, Sender};
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::Result;
pub use zenoh_flow_runtime::{Extension, Extensions, Runtime};

pub use self::configuration::ZenohFlowConfiguration;
use crate::queries::{instances::delete::delete_instance, Origin};

/// A Zenoh-Flow daemon declares 2 queryables:
/// 1. `zenoh-flow/<uuid>/runtime`
/// 2. `zenoh-flow/<uuid>/instances`
const NUMBER_QUERYABLES: usize = 2;

/// The Zenoh-Flow `Daemon`, a wrapper around a Zenoh-Flow [Runtime].
///
/// A Zenoh-Flow `Daemon` is able to coordinate with other Zenoh-Flow Daemon(s) to manage data flows.
pub struct Daemon {
    abort_tx: Sender<()>,
    abort_ack_rx: Receiver<()>,
    runtime: Arc<Runtime>,
}

impl Daemon {
    /// Spawn a new Zenoh-Flow Daemon with its [runtime] configured via the provided [configuration].
    ///
    /// Note that this configuration can be parsed from a file. This function is for instance leveraged by the Zenoh
    /// plugin for Zenoh-Flow: the configuration of Zenoh-Flow is parsed from Zenoh's configuration file.
    ///
    /// # Errors
    ///
    /// This function can fail for the following reasons:
    /// - if the feature `plugin` is disabled (*it is disabled by default*):
    ///   - the configuration of Zenoh is invalid,
    ///   - the configuration of Zenoh is provided in a separate file and the parsing of that file failed,
    ///   - a Zenoh Session could not be created,
    /// - the [extensions] section in the configuration points to a file and that file is not a valid declaration of
    ///   [extensions],
    /// - the [extensions] could not be added to the Runtime (see the list of potential reasons
    ///   [here](zenoh_flow_runtime::RuntimeBuilder::add_extensions())),
    /// - the Zenoh queryables -- one to manage the `instances` and another to manage the `runtime` itself -- could not
    ///   be created.
    ///
    /// [extensions]: Extensions
    /// [runtime]: Runtime
    /// [configuration]: ZenohFlowConfiguration
    pub async fn spawn_from_config(
        zenoh_session: Arc<Session>,
        configuration: ZenohFlowConfiguration,
    ) -> Result<Self> {
        let extensions = configuration.extensions.unwrap_or_default();

        let runtime = Runtime::builder(configuration.name)
            .add_extensions(extensions)?
            .session(zenoh_session)
            .build()
            .await?;

        Daemon::spawn(runtime).await
    }

    /// Spawn a new Zenoh-Flow Daemon wrapping the provided [runtime].
    ///
    /// # Errors
    ///
    /// This function will fail if the Zenoh queryables -- one to manage the `instances` and another to manage the
    /// `runtime` itself -- could not be created.
    ///
    /// [runtime]: Runtime
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
            runtime,
        })
    }

    /// Stop the Zenoh-Flow daemon.
    ///
    /// This method will first stop the queryables this daemon declared (to not process new requests) and then delete
    /// all the data flow instances it manages.
    ///
    /// ⚠️ If a data flow is spanning over multiple Daemons, stopping a single Daemon will delete the data flow instance
    /// on all the Daemons.
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

        let data_flows = self.runtime.instances_state().await;
        let delete_requests = data_flows
            .into_keys()
            .map(|instance_id| delete_instance(self.runtime.clone(), Origin::Client, instance_id));

        futures::future::join_all(delete_requests).await;

        // TODO Introduce a timer: if, for whatever reason, a queryable fails to send an acknowledgement we should not
        // block the stopping procedure.
        //
        // Maybe wait for 60 seconds maximum?
        for iteration in 0..NUMBER_QUERYABLES {
            self.abort_ack_rx.recv_async().await.unwrap_or_else(|e| {
                tracing::error!(
                    "Failed to receive abort acknowledgement ({}/{}): {:?}",
                    iteration + 1,
                    NUMBER_QUERYABLES,
                    e
                );
            });
            tracing::trace!(
                "Received abort acknowledgement {}/{}",
                iteration + 1,
                NUMBER_QUERYABLES
            );
        }
    }
}
