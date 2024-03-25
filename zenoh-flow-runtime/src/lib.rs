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

//! This crate exposes the structures driving the execution of a data flow: the [Runtime] and the [DataFlowInstance].
//!
//! If the feature `zenoh` is enabled (it is by default), this crate additionally re-exports the structures from
//! [Zenoh](zenoh) that allow opening a [Session](zenoh::Session) *asynchronously*.
//!
//! Users interested in exposing a Zenoh-Flow runtime should find everything in the [Runtime] and [RuntimeBuilder].
//!
//! Users interested in fetching the state of a data flow instance should look into the [DataFlowInstance],
//! [InstanceState] and [InstanceStatus] structures. These structures are leveraged by the `zfctl` command line tool.

mod instance;
pub use instance::{DataFlowInstance, InstanceState, InstanceStatus};

mod loader;
pub use loader::{Extension, Extensions};

#[cfg(feature = "shared-memory")]
mod shared_memory;

mod runners;

mod runtime;
pub use runtime::{DataFlowErr, Runtime, RuntimeBuilder};

/// A re-export of the Zenoh structures needed to open a [Session](zenoh::Session) asynchronously.
#[cfg(feature = "zenoh")]
pub mod zenoh {
    pub use zenoh::config::{client, empty, peer};
    pub use zenoh::open;
    pub use zenoh::prelude::r#async::AsyncResolve;
    pub use zenoh::prelude::{Config, Session};
}
