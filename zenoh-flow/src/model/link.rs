//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::ZFOperatorId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFLinkDescriptor {
    pub from: ZFFromEndpoint,
    pub to: ZFToEndpoint,
    pub size: Option<usize>,
    pub queueing_policy: Option<String>,
    pub priority: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFFromEndpoint {
    pub id: ZFOperatorId,
    pub output_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFToEndpoint {
    pub id: ZFOperatorId,
    pub input_id: String,
}
