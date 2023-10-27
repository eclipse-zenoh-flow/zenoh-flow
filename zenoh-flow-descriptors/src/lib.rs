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

pub(crate) mod dataflow;
pub(crate) mod flattened;
pub(crate) mod io;
pub(crate) mod nodes;

pub use dataflow::DataFlowDescriptor;
pub use flattened::{
    FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor,
};
pub use io::{InputDescriptor, LinkDescriptor, OutputDescriptor};
pub use nodes::builtin::zenoh::{ZenohSinkDescriptor, ZenohSourceDescriptor};
pub use nodes::operator::{composite::CompositeOperatorDescriptor, OperatorDescriptor};
pub use nodes::sink::SinkDescriptor;
pub use nodes::source::SourceDescriptor;
