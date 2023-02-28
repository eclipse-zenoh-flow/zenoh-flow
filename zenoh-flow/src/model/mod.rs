//
// Copyright (c) 2022 ZettaScale Technology
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

use std::{path::PathBuf, str::FromStr};

use crate::{bail, prelude::ErrorKind, zfresult::ZFError};

use self::registry::NodeKind;

pub mod descriptor;
pub mod record;
pub mod registry;

/// The middleware used for the builtin sources and sink.
#[derive(Debug)]
pub(crate) enum Middleware {
    Zenoh,
}

impl FromStr for Middleware {
    type Err = ZFError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.to_lowercase();
        if s == "zenoh" {
            return Ok(Self::Zenoh);
        }
        bail!(ErrorKind::ParsingError, "{s} is not a valid middleware!")
    }
}

impl ToString for Middleware {
    fn to_string(&self) -> String {
        "zenoh".to_string()
    }
}

#[derive(Debug)]
/// Zenoh-Flow's custom URI struct used for loading nodes.
pub(crate) enum URIStruct {
    File(PathBuf),
    Builtin(Middleware, NodeKind),
}
