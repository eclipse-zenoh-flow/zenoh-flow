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

pub(crate) mod builtin;
pub(crate) mod operator;
pub(crate) mod sink;
pub(crate) mod source;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;
use zenoh_flow_commons::Configuration;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteNodeDescriptor {
    pub descriptor: Url,
    pub description: Option<Arc<str>>,
    #[serde(default)]
    pub configuration: Configuration,
}
