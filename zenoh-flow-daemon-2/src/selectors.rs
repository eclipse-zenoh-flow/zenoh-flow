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

use anyhow::anyhow;
use zenoh::key_expr::OwnedKeyExpr;
use zenoh_flow_commons::Result;
use zenoh_flow_commons::RuntimeId;

const ZENOH_FLOW: &str = "zenoh-flow";
const INSTANCES: &str = "instances";
const RUNTIMES: &str = "runtimes";

fn try_autocanonize(maybe_ke: String) -> Result<OwnedKeyExpr> {
    OwnedKeyExpr::autocanonize(maybe_ke.clone()).map_err(|e| {
        anyhow!(
            "Failed to generate a valid, canonical, Zenoh key expression from < {} >:\n{:?}",
            maybe_ke,
            e
        )
    })
}

/// Helper function to generate an [OwnedKeyExpr] to query the data flow instances managed by a specific Zenoh-Flow
/// runtime.
///
/// The generated key expression has the following structure:
///
///     `zenoh-flow/{runtime id}/instances`
///
/// where `{runtime id}` corresponds to the unique identifier of the chosen runtime.
///
/// To obtain the list of available Zenoh-Flow runtimes, a query can be made on [runtime::KE_ALL].
pub fn selector_instances(runtime_id: &RuntimeId) -> Result<OwnedKeyExpr> {
    try_autocanonize(format!("{ZENOH_FLOW}/{runtime_id}/{INSTANCES}"))
}

/// Helper function to generate an [OwnedKeyExpr] to query the data flow instances managed by all the reachable
/// Zenoh-Flow runtimes.
///
/// The generated key expression has the following structure:
///
///     `zenoh-flow/*/instances`
///
/// # Performance
///
/// As this selector will attempt to reach all the Zenoh-Flow runtime, it is possible that the query will take
/// longer to finish and consume more network resources.
pub fn selector_all_instances() -> Result<OwnedKeyExpr> {
    try_autocanonize(format!("{ZENOH_FLOW}/*/{INSTANCES}"))
}

/// Helper function to generate an [OwnedKeyExpr] to query the provided runtime.
///
/// The generated key expression has the following structure:
///
///     `zenoh-flow/{runtime id}/runtime`
///
/// where `{runtime id}` corresponds to the unique identifier of the chosen runtime.
pub fn selector_runtimes(runtime_id: &RuntimeId) -> Result<OwnedKeyExpr> {
    try_autocanonize(format!("{ZENOH_FLOW}/{runtime_id}/{RUNTIMES}"))
}

/// Helper function to generate an [OwnedKeyExpr] to query all the reachable Zenoh-Flow runtimes.
///
/// The generated key expression has the following structure:
///
///     `zenoh-flow/*/runtime`
///
/// # Performance
///
/// As this selector will attempt to reach all the Zenoh-Flow runtime, it is possible that the query will take
/// longer to finish and consume more network resources.
pub fn selector_all_runtimes() -> Result<OwnedKeyExpr> {
    try_autocanonize(format!("{ZENOH_FLOW}/*/{RUNTIMES}"))
}
