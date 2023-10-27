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

mod configuration;
pub use configuration::Configuration;

mod deserialize;
pub use deserialize::deserialize_id;

mod identifiers;
pub use identifiers::{NodeId, PortId, RecordId, RuntimeId};

mod merge;
pub use merge::IMergeOverwrite;

mod runtime;
pub use runtime::RuntimeContext;

mod shared_memory;
pub use shared_memory::{SharedMemoryConfiguration, SharedMemoryParameters};

mod vars;
pub use vars::Vars;

/// Zenoh-Flow's result type.
pub type Result<T> = std::result::Result<T, anyhow::Error>;
