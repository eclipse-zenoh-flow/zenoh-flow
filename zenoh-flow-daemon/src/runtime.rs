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

use std::{collections::HashMap, sync::Arc};

use anyhow::bail;
use flume::{Receiver, Sender};
use futures::select;
use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};
use zenoh::prelude::r#async::*;
use zenoh::queryable::Query;
use zenoh_flow_commons::{InstanceId, Result, RuntimeId};
use zenoh_flow_runtime::{InstanceState, Runtime};

use crate::{selectors::selector_runtimes, validate_query};

#[derive(Debug, Deserialize, Serialize)]
pub enum RuntimesQuery {
    List,
    Status,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RuntimeInfo {
    pub id: RuntimeId,
    pub name: Arc<str>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RuntimeStatus {
    pub name: Arc<str>,
    pub hostname: Option<String>,
    pub architecture: Option<String>,
    pub operating_system: Option<String>,
    pub cpus: usize,
    pub ram_total: u64,
    pub data_flows_status: HashMap<InstanceId, (Arc<str>, InstanceState)>,
}

impl RuntimesQuery {
    pub(crate) async fn process(self, query: Query, runtime: Arc<Runtime>) {
        let payload = match self {
            RuntimesQuery::List => {
                // TODO We could probably try to generate that structure the moment we create the daemon, I don't see
                // these values changing at runtime.
                let runtime_info = RuntimeInfo {
                    id: runtime.id().clone(),
                    name: runtime.name(),
                };

                serde_json::to_vec(&runtime_info)
            }

            RuntimesQuery::Status => {
                let data_flows_status = runtime.instances_state().await;

                // TODO For better performance, we should initialise this structure once and simply refresh it whenever
                // we want to access some data about the machine.
                let system = sysinfo::System::new_with_specifics(
                    RefreshKind::new()
                        .with_memory(MemoryRefreshKind::new().with_ram())
                        .with_cpu(CpuRefreshKind::new()),
                );

                serde_json::to_vec(&RuntimeStatus {
                    name: runtime.name(),
                    cpus: system.cpus().len(),
                    ram_total: system.total_memory(),
                    data_flows_status,
                    hostname: sysinfo::System::host_name(),
                    architecture: sysinfo::System::cpu_arch(),
                    operating_system: sysinfo::System::name(),
                })
            }
        };

        let sample = match payload {
            Ok(payload) => Ok(Sample::new(query.key_expr().clone(), payload)),
            Err(e) => Err(Value::from(e.to_string())),
        };

        if let Err(e) = query.reply(sample).res().await {
            tracing::error!(
                r#"Failed to reply to query < {} >:
Caused by:
{:?}"#,
                query,
                e
            );
        }
    }
}

pub(crate) async fn spawn_runtime_queryable(
    zenoh_session: Arc<Session>,
    runtime: Arc<Runtime>,
    abort_rx: Receiver<()>,
    abort_ack_tx: Sender<()>,
) -> Result<()> {
    let ke_runtime = selector_runtimes(runtime.id());

    let queryable = match zenoh_session
        .declare_queryable(ke_runtime.clone())
        .res()
        .await
    {
        Ok(queryable) => {
            tracing::trace!("declared queryable < {} >", ke_runtime);
            queryable
        }
        Err(e) => {
            bail!("Failed to declare Zenoh queryable 'runtimes': {:?}", e)
        }
    };

    async_std::task::spawn(async move {
        loop {
            select!(
                _ = abort_rx.recv_async() => {
                    tracing::trace!("Received abort signal");
                    break;
                }

                query = queryable.recv_async() => {
                    match query {
                        Ok(query) => {
                            let runtime_query: RuntimesQuery = match validate_query(&query).await {
                                Ok(runtime_query) => runtime_query,
                                Err(e) => {
                                    tracing::error!("Unable to parse `RuntimesQuery`: {:?}", e);
                                    return;
                                }
                            };

                            let runtime = runtime.clone();
                            async_std::task::spawn(async move {
                                runtime_query.process(query, runtime).await;
                            });
                        }
                        Err(e) => {
                            tracing::error!("Queryable 'runtimes' dropped: {:?}", e);
                            return;
                        }
                    }
                }
            )
        }

        abort_ack_tx.send_async(()).await.unwrap_or_else(|e| {
            tracing::error!("Queryable 'runtime' failed to acknowledge abort: {:?}", e);
        });
    });

    Ok(())
}
