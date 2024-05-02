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

//! This crate exposes `*Record` structures. A *record* in Zenoh-Flow is a description of a data flow (or part of it)
//! that is tied to a specific infrastructure and deployment.
//!
//! In particular, a [DataFlowRecord] represents a single deployment of a
//! [FlattenedDataFlowDescriptor](zenoh_flow_descriptors::FlattenedDataFlowDescriptor) on an infrastructure: all the
//! nodes have been assigned to a Zenoh-Flow runtime. This is why to each [DataFlowRecord] is associated a unique
//! [InstanceId](zenoh_flow_commons::InstanceId) which uniquely identifies it.
//!
//! # ⚠️ Internal usage
//!
//! This crate is (mostly) intended for internal usage within the
//! [Zenoh-Flow](https://github.com/eclipse-zenoh/zenoh-flow) project.

mod connectors;
mod dataflow;

pub use self::{
    connectors::{ReceiverRecord, SenderRecord},
    dataflow::DataFlowRecord,
};
