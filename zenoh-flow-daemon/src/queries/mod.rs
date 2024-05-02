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
use zenoh::{prelude::*, queryable::Query};
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
    let value = match query.value() {
        Some(value) => value,
        None => {
            bail!("Received empty payload");
        }
    };

    if ![
        Encoding::APP_OCTET_STREAM,
        Encoding::APP_JSON,
        Encoding::TEXT_JSON,
    ]
    .contains(&value.encoding)
    {
        bail!("Encoding < {} > is not supported", value.encoding);
    }

    serde_json::from_slice::<T>(&value.payload.contiguous()).map_err(|e| anyhow!("{:?}", e))
}
