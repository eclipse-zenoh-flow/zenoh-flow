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

use zenoh::key_expr::OwnedKeyExpr;
use zenoh_flow_commons::RuntimeId;

const ZENOH_FLOW: &str = "zenoh-flow";
const INSTANCES: &str = "instances";
const RUNTIMES: &str = "runtimes";

/// This function generates an [OwnedKeyExpr] from the provided String.
///
/// # Panic
///
/// This function will panic if the provided String cannot be transformed into a canonical key expression. See the
/// [documentation of Zenoh's autocanonize](OwnedKeyExpr::autocanonize()) for all the possible scenarios where this
/// could happen.
///
/// Although panicking seems like a strong posture, we believe this choice strikes a correct balance: we, the Zenoh-Flow
/// team, know the internals of Zenoh and can guarantee the validity of the key expressions we are building. Plus, all
/// the exposed API leveraging this function only accepts [RuntimeId] which is a thin wrapper over ZenohId.
fn autocanonize(maybe_ke: String) -> OwnedKeyExpr {
    OwnedKeyExpr::autocanonize(maybe_ke.clone()).unwrap_or_else(|e| {
        panic!(
            r#"Zenoh-Flow internal error: < {} > is not a valid or canonical key expression

{e:?}"#,
            maybe_ke
        )
    })
}

/// Helper function to generate an [OwnedKeyExpr] to query the data flow instances managed by a specific Zenoh-Flow
/// runtime.
///
/// The generated key expression has the following structure: `zenoh-flow/<runtime id>/instances`
///
/// where `<runtime id>` corresponds to the unique identifier of the chosen runtime.
///
/// # Panic
///
/// This function will panic in the impossible scenario (although never say never…) where the provided [RuntimeId] would
/// make the key expression not valid or not canonical.
pub fn selector_instances(runtime_id: &RuntimeId) -> OwnedKeyExpr {
    autocanonize(format!("{ZENOH_FLOW}/{runtime_id}/{INSTANCES}"))
}

/// Helper function to generate an [OwnedKeyExpr] to query the data flow instances managed by all the reachable
/// Zenoh-Flow runtimes.
///
/// The generated key expression has the following structure: `zenoh-flow/*/instances`
///
/// # Performance
///
/// As this selector will attempt to reach all the Zenoh-Flow runtime, it is possible that the query will take
/// longer to finish and consume more network resources.
///
/// # Panic
///
/// This function will panic in the impossible scenario where the key expression we internally rely on is no longer
/// valid or canonical.
pub fn selector_all_instances() -> OwnedKeyExpr {
    autocanonize(format!("{ZENOH_FLOW}/*/{INSTANCES}"))
}

/// Helper function to generate an [OwnedKeyExpr] to query the provided runtime.
///
/// The generated key expression has the following structure: " zenoh-flow/<runtime id>/runtime "
///
/// where `{runtime id}` corresponds to the unique identifier of the chosen runtime.
///
/// # Panic
///
/// This function will panic in the impossible scenario (although never say never…) where the provided [RuntimeId] would
/// make the key expression not valid or not canonical.
pub fn selector_runtimes(runtime_id: &RuntimeId) -> OwnedKeyExpr {
    autocanonize(format!("{ZENOH_FLOW}/{runtime_id}/{RUNTIMES}"))
}

/// Helper function to generate an [OwnedKeyExpr] to query all the reachable Zenoh-Flow runtimes.
///
/// The generated key expression has the following structure: " zenoh-flow/*/runtime "
///
/// # Performance
///
/// As this selector will attempt to reach all the Zenoh-Flow runtime, it is possible that the query will take
/// longer to finish and consume more network resources.
///
/// # Panic
///
/// This function will panic in the impossible scenario where the key expression we internally rely on is no longer
/// valid or canonical.
pub fn selector_all_runtimes() -> OwnedKeyExpr {
    autocanonize(format!("{ZENOH_FLOW}/*/{RUNTIMES}"))
}
