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

use const_format::formatcp;

pub use ::zenoh_flow_derive;

pub use ::async_std;
pub use ::bincode;
pub use ::paste;
pub use ::serde;
pub use ::typetag;

pub mod model;
pub mod runtime;
pub use runtime::deadline::LocalDeadlineMiss;
pub use runtime::message::*;
pub use runtime::token::*;
pub mod types;
pub use types::*;
pub mod traits;
pub use traits::*;

pub mod macros;
pub use macros::*;

pub mod error;
pub use error::*;

pub const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const FULL_VERSION: &str = formatcp!("{}-{}", VERSION, GIT_VERSION);
