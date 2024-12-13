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

//! Queries and their response to interact with a Zenoh-Flow [Daemon].
//!
//! This module exposes the available queries to manage data flow instances and interact with other Zenoh-Flow Daemons.
//! The queries are divided into two sets:
//! - [instances](InstancesQuery)
//! - [runtime](RuntimesQuery)
//!
//! [Daemon]: crate::daemon::Daemon

pub(crate) mod instances;
pub(crate) mod runtime;
pub(crate) mod selectors;

use anyhow::{anyhow, bail};
use serde::Deserialize;
use zenoh::query::Query;
use zenoh_flow_commons::Result;
pub use zenoh_flow_runtime::InstanceStatus;

pub use self::{
    instances::{InstancesQuery, Origin},
    runtime::{RuntimeInfo, RuntimeStatus, RuntimesQuery},
    selectors::*,
};

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
    let Some(payload) = query.payload() else {
        bail!("Received Query with empty payload")
    };

    serde_json::from_slice::<T>(&payload.to_bytes()).map_err(|e| anyhow!("{:?}", e))
}
