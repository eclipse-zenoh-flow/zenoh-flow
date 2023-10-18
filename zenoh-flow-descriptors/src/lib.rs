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

mod composite;
pub use composite::{
    CompositeInputDescriptor, CompositeOperatorDescriptor, CompositeOutputDescriptor,
};

mod dataflow;
pub use dataflow::DataFlowDescriptor;

mod flattened;
pub use flattened::{
    FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor, IFlattenable,
};

mod io;
pub use io::{InputDescriptor, OutputDescriptor};

mod link;
pub use link::LinkDescriptor;

mod nodes;
pub use nodes::{NodeDescriptor, OperatorDescriptor, SinkDescriptor, SourceDescriptor};

mod uri;
