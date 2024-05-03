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

use serde::Deserialize;
use zenoh_flow_runtime::Extensions;

/// The configuration of a Zenoh-Flow Daemon.
#[derive(Deserialize, Debug)]
pub struct ZenohFlowConfiguration {
    /// A human-readable name for this Daemon and its embedded Runtime.
    pub name: String,
    /// Additionally supported [Extensions].
    pub extensions: Option<Extensions>,
}
