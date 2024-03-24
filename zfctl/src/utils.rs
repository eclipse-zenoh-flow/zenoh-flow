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

use itertools::Itertools;
use rand::Rng;
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::RuntimeId;
use zenoh_flow_daemon::queries::{selector_all_runtimes, RuntimeInfo, RuntimesQuery};

/// Returns the list of [RuntimeInfo] of the reachable Zenoh-Flow Runtime(s).
///
/// # Panic
///
/// This function will panic if:
/// - (internal error) the query to list the Zenoh-Flow Runtimes could not be serialised by `serde_json`,
/// - the query on the Zenoh network failed,
/// - no Zenoh-Flow Runtime is reachable.
pub(crate) async fn get_all_runtimes(session: &Session) -> Vec<RuntimeInfo> {
    let value = serde_json::to_vec(&RuntimesQuery::List)
        .unwrap_or_else(|e| panic!("`serde_json` failed to serialize `RuntimeQuery::List`: {e:?}"));

    let runtime_replies = session
        .get(selector_all_runtimes())
        .with_value(value)
        // We want to address all the Zenoh-Flow runtimes that are reachable on the Zenoh network.
        .consolidation(ConsolidationMode::None)
        .res()
        .await
        .unwrap_or_else(|e| panic!("Failed to query available runtimes:\n{:?}", e));

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
        panic!("No Zenoh-Flow runtime were detected. Have you checked if (i) they are up and (ii) reachable through Zenoh?");
    }

    runtimes
}

/// Returns the unique identifier of the Zenoh-Flow Runtime that has the provided `name`.
///
/// # Panic
///
/// This function will panic if:
/// - there is no Zenoh-Flow Runtime that has the provided name,
/// - there are more than 1 Zenoh-Flow Runtime with the provided name.
pub(crate) async fn get_runtime_by_name(session: &Session, name: &str) -> RuntimeId {
    let runtimes = get_all_runtimes(session).await;
    let mut matching_runtimes = runtimes
        .iter()
        .filter(|&r_info| r_info.name.as_ref() == name)
        .collect_vec();

    if matching_runtimes.is_empty() {
        panic!("Found no Zenoh-Flow Runtime with name < {name} >");
    } else if matching_runtimes.len() > 1 {
        tracing::error!("Found multiple Zenoh-Flow Runtimes named < {name} >:");
        matching_runtimes.iter().for_each(|&r_info| {
            tracing::error!("- {} - (id) {}", r_info.name, r_info.id);
        });
        panic!(
            "There are multiple Zenoh-Flow Runtimes named < {name} >, please use their 'id' instead"
        );
    } else {
        matching_runtimes.pop().unwrap().id.clone()
    }
}

/// Returns the unique identifier of a reachable Zenoh-Flow Runtime.
///
/// # Panic
///
/// This function will panic if:
/// - (internal error) the query to list the Zenoh-Flow Runtimes could not be serialised by `serde_json`,
/// - the query on the Zenoh network failed,
/// - no Zenoh-Flow Runtime is reachable.
pub(crate) async fn get_random_runtime(session: &Session) -> RuntimeId {
    let mut runtimes = get_all_runtimes(session).await;
    let orchestrator = runtimes.remove(rand::thread_rng().gen_range(0..runtimes.len()));

    orchestrator.id
}
