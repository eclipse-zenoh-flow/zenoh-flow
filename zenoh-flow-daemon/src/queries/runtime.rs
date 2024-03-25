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

use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind};
use zenoh::prelude::r#async::*;
use zenoh::queryable::Query;
use zenoh_flow_commons::{InstanceId, RuntimeId};
use zenoh_flow_runtime::{InstanceState, Runtime};

/// The available interactions with Zenoh-Flow Daemon(s).
#[derive(Debug, Deserialize, Serialize)]
pub enum RuntimesQuery {
    /// To list all the reachable Zenoh-Flow Daemon(s).
    ///
    /// This query will display the name and [unique identifier](RuntimeId) of each Zenoh-Flow Daemon. See the
    /// corresponding structure, [RuntimeInfo], for usage within your code.
    List,
    /// To obtain detailed information about a Zenoh-Flow Daemon and its host.
    ///
    /// This query will display:
    /// - the name of the Zenoh-Flow Daemon,
    /// - the number of CPUs of the host,
    /// - the quantity of RAM of the host,
    /// - the hostname of the host,
    /// - the CPU architecture of the host,
    /// - the operating system of the host,
    /// - the status of all the data flows managed by the Zenoh-Flow Daemon.
    ///
    /// See the corresponding structure, [RuntimeStatus], for usage within your code.
    Status,
}

/// The answer to a [List] query.
///
/// [List]: RuntimesQuery::List
#[derive(Debug, Deserialize, Serialize)]
pub struct RuntimeInfo {
    pub id: RuntimeId,
    pub name: Arc<str>,
}

/// The answer to a [Status] query.
///
/// [Status]: RuntimesQuery::Status
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
