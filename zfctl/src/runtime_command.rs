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

use std::time::Duration;

use anyhow::anyhow;
use clap::Subcommand;
use comfy_table::{Row, Table};
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{Result, RuntimeId};
use zenoh_flow_daemon::queries::*;

use super::row;
use crate::{
    utils::{get_all_runtimes, get_runtime_by_name},
    ZENOH_FLOW_INTERNAL_ERROR,
};

#[derive(Subcommand)]
pub(crate) enum RuntimeCommand {
    /// List all the Zenoh-Flow runtimes reachable on the Zenoh network.
    List,
    /// Returns the status of the provided Zenoh-Flow runtime.
    ///
    /// The status consists of general information regarding the runtime and the
    /// machine it runs on:
    /// - the name associated with the Zenoh-Flow runtime,
    /// - the number of CPUs the machine running the Zenoh-Flow runtime has,
    /// - the total amount of RAM the machine running the Zenoh-Flow runtime has,
    /// - for each data flow the Zenoh-Flow runtime manages (partially or not):
    ///   - its unique identifier,
    ///   - its name,
    ///   - its status.
    #[command(verbatim_doc_comment)]
    #[group(required = true, multiple = false)]
    Status {
        /// The unique identifier of the Zenoh-Flow runtime to contact.
        /// If no identifier is provided, a random Zenoh-Flow runtime is
        /// selected among those reachable.
        #[arg(short = 'i', long = "id", group = "runtime")]
        runtime_id: Option<RuntimeId>,
        /// The name of the Zenoh-Flow runtime to contact.
        ///
        /// Note that if several runtimes share the same name, the first to
        /// answer will be selected.
        #[arg(short = 'n', long = "name", group = "runtime")]
        runtime_name: Option<String>,
    },
}

impl RuntimeCommand {
    pub async fn run(self, session: &Session) -> Result<()> {
        match self {
            RuntimeCommand::List => {
                let runtimes = get_all_runtimes(session).await;

                let mut table = Table::new();
                table.set_width(80);
                table.set_header(Row::from(vec!["Identifier", "Name"]));
                runtimes.iter().for_each(|info| {
                    table.add_row(Row::from(vec![&info.id.to_string(), info.name.as_ref()]));
                });

                println!("{table}");
            }

            RuntimeCommand::Status {
                runtime_id,
                runtime_name,
            } => {
                let runtime_id = match (runtime_id, runtime_name) {
                    (Some(id), _) => id,
                    (None, Some(name)) => get_runtime_by_name(session, &name).await,
                    (None, None) => {
                        // This code is indeed unreachable because:
                        // (1) The `group` macro has `required = true` which indicates that clap requires an entry for
                        //     any group.
                        // (2) The `group` macro has `multiple = false` which indicates that only a single entry for
                        //     any group is accepted.
                        // (3) The `runtime_id` and `runtime_name` fields belong to the same group "runtime".
                        //
                        // => A single entry for the group "runtime" is required (and mandatory).
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
                    .with_value(value)
                    .timeout(Duration::from_secs(5))
                    .res()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "Failed to query Zenoh-Flow runtime < {} >: {:?}",
                            runtime_id,
                            e
                        )
                    })?;

                while let Ok(reply) = reply.recv_async().await {
                    match reply.sample {
                        Ok(sample) => {
                            match serde_json::from_slice::<RuntimeStatus>(
                                &sample.payload.contiguous(),
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

                        Err(e) => tracing::error!("Reply to runtime status failed with: {:?}", e),
                    }
                }
            }
        }

        Ok(())
    }
}
