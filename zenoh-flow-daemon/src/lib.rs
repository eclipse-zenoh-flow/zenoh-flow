//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

//! This crate provides the Zenoh-Flow Daemon: a wrapper around a Zenoh-Flow [Runtime] that can process requests, made
//! on specific key expressions, to manage data flows.
//!
//! In particular, a Zenoh-Flow Daemon is able to coordinate with other Zenoh-Flow Daemon(s) to manage data flows ---
//! provided that they can reach each other.
//!
//! Hence, instantiating a data flow only requires a single communication with one Daemon: it will automatically request
//! the other Daemons involved in the deployment to manage their respective nodes.
//!
//! Users interested in integrating a [Daemon] in their system should look into the [Daemon::spawn()] and
//! [Daemon::spawn_from_config()] methods.
//!
//! This create defines the feature `plugin` for when the Daemon is embedded as a plugin on a Zenoh router.

pub mod daemon;
pub mod queries;
