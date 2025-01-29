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

use itertools::Itertools;
use rand::Rng;
use zenoh::{query::ConsolidationMode, Session};
use zenoh_flow_commons::{Result, RuntimeId};
use zenoh_flow_daemon::queries::{selector_all_runtimes, RuntimeInfo, RuntimesQuery};

/// Returns the list of [RuntimeInfo] of the reachable Zenoh-Flow Daemon(s).
///
/// # Panic
///
/// This function will panic if:
/// - (internal error) the query to list the Zenoh-Flow Daemons could not be serialised by `serde_json`,
/// - the query on the Zenoh network failed,
/// - no Zenoh-Flow Daemon is reachable.
pub(crate) async fn get_all_runtimes(session: &Session) -> Result<Vec<RuntimeInfo>> {
    tracing::debug!("Fetching available Zenoh-Flow Daemon(s)");
    let value = match serde_json::to_vec(&RuntimesQuery::List) {
        Ok(value) => value,
        Err(e) => {
            anyhow::bail!("`serde_json` failed to serialize `RuntimeQuery::List`: {e:?}");
        }
    };

    let runtime_replies = match session
        .get(selector_all_runtimes())
        .payload(value)
        // We want to address all the Zenoh-Flow daemons that are reachable on the Zenoh network.
        .consolidation(ConsolidationMode::None)
        .await
    {
        Ok(replies) => replies,
        Err(e) => {
            anyhow::bail!("Failed to send Query to Zenoh-Flow Daemon(s): {:?}", e);
        }
    };

    let mut runtimes = Vec::new();
    while let Ok(reply) = runtime_replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                match serde_json::from_slice::<RuntimeInfo>(&sample.payload().to_bytes()) {
                    Ok(runtime_info) => runtimes.push(runtime_info),
                    Err(e) => {
                        tracing::error!("Failed to parse a reply as a `RuntimeId`: {:?}", e)
                    }
                }
            }
            Err(e) => tracing::warn!("A reply returned an error: {:?}", e),
        }
    }

    if runtimes.is_empty() {
        anyhow::bail!("Found no Zenoh-Flow Daemon on the network");
    }

    tracing::debug!("Found {} Zenoh-Flow Daemon(s)", runtimes.len());

    Ok(runtimes)
}

/// Returns the unique identifier of the Zenoh-Flow Daemon that has the provided `name`.
///
/// # Panic
///
/// This function will panic if:
/// - there is no Zenoh-Flow Daemon that has the provided name,
/// - there are more than 1 Zenoh-Flow Daemon with the provided name.
pub(crate) async fn get_runtime_by_name(session: &Session, name: &str) -> Result<RuntimeId> {
    let runtimes = get_all_runtimes(session).await?;
    let mut matching_runtimes = runtimes
        .iter()
        .filter(|&r_info| r_info.name.as_ref() == name)
        .collect_vec();

    if matching_runtimes.is_empty() {
        anyhow::bail!("Found no Zenoh-Flow Daemon with name < {name} >");
    } else if matching_runtimes.len() > 1 {
        tracing::error!("Found multiple Zenoh-Flow Daemon named < {name} >:");
        matching_runtimes.iter().for_each(|&r_info| {
            tracing::error!("- {} - (id) {}", r_info.name, r_info.id);
        });
        anyhow::bail!(
            "There are multiple Zenoh-Flow Daemons named < {name} >, please use their 'zid' \
             instead"
        );
    } else {
        Ok(matching_runtimes.pop().unwrap().id.clone())
    }
}

/// Returns the unique identifier of a reachable Zenoh-Flow Daemon.
///
/// # Panic
///
/// This function will panic if:
/// - (internal error) the query to list the Zenoh-Flow Daemons could not be serialised by `serde_json`,
/// - the query on the Zenoh network failed,
/// - no Zenoh-Flow Daemon is reachable.
pub(crate) async fn get_random_runtime(session: &Session) -> Result<RuntimeId> {
    let mut runtimes = get_all_runtimes(session).await?;
    let orchestrator = runtimes.remove(rand::thread_rng().gen_range(0..runtimes.len()));

    tracing::info!(
        "Selected Zenoh-Flow Daemon < {}: {} > as Orchestrator",
        orchestrator.name,
        orchestrator.id
    );

    Ok(orchestrator.id)
}
