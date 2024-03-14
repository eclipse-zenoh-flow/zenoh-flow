//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

pub mod configuration;
mod instances;
pub mod runtime;
pub mod selectors;
#[cfg(not(feature = "plugin"))]
use configuration::ZenohConfiguration;
use configuration::ZenohFlowConfiguration;
pub use instances::{InstancesQuery, Origin};
pub use zenoh_flow_runtime::{InstanceState, InstanceStatus, Runtime};

use anyhow::{anyhow, bail};
use flume::{Receiver, Sender};
use serde::Deserialize;
use std::sync::Arc;
use zenoh::{prelude::r#async::*, queryable::Query};
use zenoh_flow_commons::{try_parse_from_file, Result, Vars};
use zenoh_flow_runtime::Extensions;

use crate::configuration::ExtensionsConfiguration;

/// A Zenoh-Flow daemon declares 2 queryables:
/// 1. `zenoh-flow/<uuid>/runtime`
/// 2. `zenoh-flow/<uuid>/instances`
const NUMBER_QUERYABLES: usize = 2;

pub struct Daemon {
    abort_tx: Sender<()>,
    abort_ack_rx: Receiver<()>,
}

impl Daemon {
    /// TODO Documentation
    pub async fn spawn_from_config(
        #[cfg(feature = "plugin")] zenoh_session: Arc<Session>,
        configuration: ZenohFlowConfiguration,
    ) -> Result<Self> {
        #[cfg(not(feature = "plugin"))]
        let zenoh_config = match configuration.zenoh {
            ZenohConfiguration::File(path) => {
                zenoh::prelude::Config::from_file(path).map_err(|e| anyhow!("{e:?}"))
            }
            ZenohConfiguration::Configuration(config) => Ok(config),
        }?;

        #[cfg(not(feature = "plugin"))]
        let zenoh_session = zenoh::open(zenoh_config)
            .res()
            .await
            .map(|session| session.into_arc())
            .map_err(|e| anyhow!("{e:?}"))?;

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
            runtime::spawn_runtime_queryable(session.clone(), runtime.clone(), abort, abort_ack)
                .await
        {
            tracing::error!(
                "The Zenoh-Flow daemon encountered a fatal error:\n{:?}\nAborting",
                e
            );
            // TODO: Clean everything up before aborting.
        }

        if let Err(e) =
            instances::spawn_instances_queryable(session, runtime.clone(), abort_rx, abort_ack_tx)
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

/// Validate a query and try to deserialize into an instance of `T`.
///
/// This function checks that the query is correct:
/// - it has a payload,
/// - the encoding is "correct",
/// - the payload can be deserialized into an instance of `T`.
///
/// If any check fails, an error message is logged and the query is dropped.
///
/// After these checks, the method `process` is called on the variant of `InstancesQuery`.
pub(crate) async fn validate_query<T: for<'a> Deserialize<'a>>(query: &Query) -> Result<T> {
    let value = match query.value() {
        Some(value) => value,
        None => {
            bail!("Received empty payload");
        }
    };

    if ![
        Encoding::APP_OCTET_STREAM,
        Encoding::APP_JSON,
        Encoding::TEXT_JSON,
    ]
    .contains(&value.encoding)
    {
        bail!("Encoding < {} > is not supported", value.encoding);
    }

    serde_json::from_slice::<T>(&value.payload.contiguous()).map_err(|e| anyhow!("{:?}", e))
}
