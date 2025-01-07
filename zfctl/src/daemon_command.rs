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

use std::{path::PathBuf, time::Duration};

use anyhow::anyhow;
use async_std::stream::StreamExt;
use clap::{ArgGroup, Subcommand};
use comfy_table::{Row, Table};
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::Signals;
use zenoh::Session;
use zenoh_flow_commons::{try_parse_from_file, Result, RuntimeId, Vars};
use zenoh_flow_daemon::{
    daemon::{Daemon, ZenohFlowConfiguration},
    queries::{selector_runtimes, RuntimeStatus, RuntimesQuery},
};
use zenoh_flow_runtime::Runtime;

use crate::{
    row,
    utils::{get_all_runtimes, get_runtime_by_name},
    ZENOH_FLOW_INTERNAL_ERROR,
};

#[derive(Subcommand)]
pub(crate) enum DaemonCommand {
    /// List all the Zenoh-Flow daemons reachable on the Zenoh network.
    List,
    /// Launch a Zenoh-Flow Daemon.
    #[command(verbatim_doc_comment)]
    #[command(group(
        ArgGroup::new("exclusive")
            .args(&["name", "configuration"])
            .required(true)
            .multiple(false)
    ))]
    Start {
        /// The human-readable name to give the Zenoh-Flow Runtime wrapped by this
        /// Daemon.
        ///
        /// To start a Zenoh-Flow Daemon, at least a name is required.
        name: Option<String>,
        /// The path of the configuration of the Zenoh-Flow Daemon.
        ///
        /// This configuration allows setting extensions supported by the Daemon
        /// and its name.
        #[arg(short, long, verbatim_doc_comment)]
        configuration: Option<PathBuf>,
    },
    /// Returns the status of the provided Zenoh-Flow daemon.
    ///
    /// The status consists of general information regarding the daemon and the
    /// machine it runs on:
    /// - the name associated with the Zenoh-Flow daemon,
    /// - the number of CPUs the machine running the Zenoh-Flow daemon has,
    /// - the total amount of RAM the machine running the Zenoh-Flow daemon has,
    /// - for each data flow the Zenoh-Flow daemon manages (partially or not):
    ///   - its unique identifier,
    ///   - its name,
    ///   - its status.
    #[command(verbatim_doc_comment)]
    #[command(group(
        ArgGroup::new("exclusive")
            .args(&["daemon_id", "daemon_name"])
            .required(true)
            .multiple(false)
    ))]
    Status {
        /// The name of the Zenoh-Flow daemon to contact.
        ///
        /// Note that if several daemons share the same name, the first to
        /// answer will be selected.
        daemon_name: Option<String>,
        /// The unique identifier of the Zenoh-Flow daemon to contact.
        #[arg(short = 'i', long = "id")]
        daemon_id: Option<RuntimeId>,
    },
}

impl DaemonCommand {
    pub async fn run(self, session: Session) -> Result<()> {
        match self {
            DaemonCommand::Start {
                name,
                configuration,
            } => {
                let daemon = match configuration {
                    Some(path) => {
                        let (zenoh_flow_configuration, _) =
                            try_parse_from_file::<ZenohFlowConfiguration>(&path, Vars::default())
                                .unwrap_or_else(|e| {
                                    panic!(
                                        "Failed to parse a Zenoh-Flow Configuration from < {} \
                                         >:\n{e:?}",
                                        path.display()
                                    )
                                });

                        Daemon::spawn_from_config(session, zenoh_flow_configuration)
                            .await
                            .expect("Failed to spawn the Zenoh-Flow Daemon")
                    }
                    None => Daemon::spawn(
                        Runtime::builder(name.unwrap())
                            .session(session)
                            .build()
                            .await
                            .expect("Failed to build the Zenoh-Flow Runtime"),
                    )
                    .await
                    .expect("Failed to spawn the Zenoh-Flow Daemon"),
                };

                async_std::task::spawn(async move {
                    let mut signals = Signals::new([SIGTERM, SIGINT, SIGQUIT])
                        .expect("Failed to create SignalsInfo for: [SIGTERM, SIGINT, SIGQUIT]");

                    while let Some(signal) = signals.next().await {
                        match signal {
                            SIGTERM | SIGINT | SIGQUIT => {
                                tracing::info!("Received termination signal, shutting down.");
                                daemon.stop().await;
                                break;
                            }

                            signal => {
                                tracing::warn!("Ignoring signal ({signal})");
                            }
                        }
                    }
                })
                .await;
            }
            DaemonCommand::List => {
                let runtimes = get_all_runtimes(&session).await;

                let mut table = Table::new();
                table.set_width(80);
                table.set_header(Row::from(vec!["Identifier", "Name"]));
                runtimes.iter().for_each(|info| {
                    table.add_row(Row::from(vec![&info.id.to_string(), info.name.as_ref()]));
                });

                println!("{table}");
            }
            DaemonCommand::Status {
                daemon_id,
                daemon_name,
            } => {
                let runtime_id = match (daemon_id, daemon_name) {
                    (Some(id), _) => id,
                    (None, Some(name)) => get_runtime_by_name(&session, &name).await,
                    (None, None) => {
                        // This code is indeed unreachable because:
                        // (1) The `group` macro has `required = true` which indicates that clap requires an entry for
                        //     any group.
                        // (2) The `group` macro has `multiple = false` which indicates that only a single entry for
                        //     any group is accepted.
                        // (3) The `daemon_id` and `daemon_name` fields belong to the same group "exclusive".
                        //
                        // => A single entry for the group "exclusive" is required (and mandatory).
                        unreachable!()
                    }
                };

                let selector = selector_runtimes(&runtime_id);

                let value = serde_json::to_vec(&RuntimesQuery::Status).map_err(|e| {
                    tracing::error!(
                        "serde_json failed to serialize `RuntimeQuery::Status`: {:?}",
                        e
                    );
                    anyhow!(ZENOH_FLOW_INTERNAL_ERROR)
                })?;

                let reply = session
                    .get(selector)
                    .payload(value)
                    .timeout(Duration::from_secs(5))
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "Failed to query Zenoh-Flow daemon < {} >: {:?}",
                            runtime_id,
                            e
                        )
                    })?;

                while let Ok(reply) = reply.recv_async().await {
                    match reply.result() {
                        Ok(sample) => {
                            match serde_json::from_slice::<RuntimeStatus>(
                                &sample.payload().to_bytes(),
                            ) {
                                Ok(runtime_status) => {
                                    let mut table = Table::new();
                                    table.set_width(80);
                                    table.add_row(row!("Identifier", runtime_id));
                                    table.add_row(row!("Name", runtime_status.name));
                                    table.add_row(row!(
                                        "Host name",
                                        runtime_status.hostname.unwrap_or_else(|| "N/A".into())
                                    ));
                                    table.add_row(row!(
                                        "Operating System",
                                        runtime_status
                                            .operating_system
                                            .unwrap_or_else(|| "N/A".into())
                                    ));
                                    table.add_row(row!(
                                        "Arch",
                                        runtime_status.architecture.unwrap_or_else(|| "N/A".into())
                                    ));
                                    table.add_row(row!("# CPUs", runtime_status.cpus));
                                    table.add_row(row!(
                                        "# RAM",
                                        bytesize::to_string(runtime_status.ram_total, true)
                                    ));
                                    println!("{table}");

                                    table = Table::new();
                                    table.set_width(80);
                                    table.set_header(row!("Instance Uuid", "Name", "Status"));
                                    runtime_status.data_flows_status.iter().for_each(
                                        |(uuid, (name, status))| {
                                            table.add_row(row!(uuid, name, status));
                                        },
                                    );
                                    println!("{table}");
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to parse reply as a `RuntimeStatus`: {:?}",
                                        e
                                    )
                                }
                            }
                        }

                        Err(e) => tracing::error!("Reply to daemon status failed with: {:?}", e),
                    }
                }
            }
        }

        Ok(())
    }
}
