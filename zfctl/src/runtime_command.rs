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

use super::row;

use std::time::Duration;

use anyhow::{anyhow, bail};
use clap::Subcommand;
use comfy_table::{Row, Table};
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{Result, RuntimeId};
use zenoh_flow_daemon::{
    runtime::{RuntimeInfo, RuntimeStatus, RuntimesQuery},
    selectors::selector_runtimes,
};

use crate::ZENOH_FLOW_INTERNAL_ERROR;

pub(crate) async fn get_all_runtimes(session: &Session) -> Result<Vec<RuntimeInfo>> {
    let value = serde_json::to_vec(&RuntimesQuery::List).map_err(|e| {
        tracing::error!(
            "`serde_json` failed to serialize `RuntimeQuery::List`: {:?}",
            e
        );
        anyhow!(ZENOH_FLOW_INTERNAL_ERROR)
    })?;

    let runtime_replies = session
        .get(zenoh_flow_daemon::selectors::selector_all_runtimes())
        .with_value(value)
        // We want to address all the Zenoh-Flow runtimes that are reachable on the Zenoh network.
        .consolidation(ConsolidationMode::None)
        // .target(QueryTarget::All)
        .res()
        .await
        .map_err(|e| anyhow!("Failed to query available runtimes:\n{:?}", e))?;

    let mut runtimes = Vec::new();
    while let Ok(reply) = runtime_replies.recv_async().await {
        match reply.sample {
            Ok(sample) => {
                match serde_json::from_slice::<RuntimeInfo>(&sample.value.payload.contiguous()) {
                    Ok(runtime_info) => runtimes.push(runtime_info),
                    Err(e) => {
                        tracing::error!("Failed to parse a reply as a `RuntimeId`:\n{:?}", e)
                    }
                }
            }

            Err(e) => tracing::warn!("A reply returned an error:\n{:?}", e),
        }
    }

    if runtimes.is_empty() {
        tracing::error!("No Zenoh-Flow runtime were detected. Have you checked if (i) they are up and (ii) reachable through Zenoh?");
        bail!("zfctl encountered a fatal error, aborting.");
    }

    Ok(runtimes)
}

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
    Status { runtime_id: RuntimeId },
}

impl RuntimeCommand {
    pub async fn run(self, session: &Session) -> Result<()> {
        match self {
            RuntimeCommand::List => {
                let runtimes = get_all_runtimes(session).await?;

                let mut table = Table::new();
                table.set_width(80);
                table.set_header(Row::from(vec!["Identifier", "Name"]));
                runtimes.iter().for_each(|info| {
                    table.add_row(Row::from(vec![&info.id.to_string(), info.name.as_ref()]));
                });

                println!("{table}");
            }

            RuntimeCommand::Status { runtime_id } => {
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
