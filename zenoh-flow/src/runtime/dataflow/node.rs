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

use crate::model::record::{OperatorRecord, SinkRecord, SourceRecord};
use crate::prelude::{Configuration, Context, Inputs, Node, Outputs, Result};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use futures::Future;
#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// A `NodeConstructor` creates a single [`Node`](`Node`).
///
/// The `record` holds the metadata associated with the Node while the `constructor` is the function
/// defined by the user to create it (through the implementation of [`Source`](`Source`),
/// [`Operator`](`Operator`), or [`Sink`](`Sink`)).
///
/// The `_library` is a reference over the dynamically loaded shared library. It can be `None` when
/// the factory is created programmatically.
pub(crate) struct NodeConstructor<Record, C: ConstructorFn> {
    pub(crate) record: Record,
    pub(crate) constructor: C,
    _library: Option<Arc<Library>>,
}
/// `ConstructorFn` is a private trait that prevents us from associating any function to the
/// `Constructor` of [`NodeConstructor`](`NodeConstructor`) struct.
pub(crate) trait ConstructorFn {}

/// `SourceFn` is the only signature we accept to construct a [`Source`](`Source`).
pub type SourceFn = fn(
    Context,
    Option<Configuration>,
    Outputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

impl ConstructorFn for SourceFn {}

/// `OperatorFn` is the only signature we accept to construct an [`Operator`](`Operator`).
pub type OperatorFn = fn(
    Context,
    Option<Configuration>,
    Inputs,
    Outputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

impl ConstructorFn for OperatorFn {}

/// `SinkFn` is the only signature we accept to construct a [`Sink`](`Sink`).
pub type SinkFn = fn(
    Context,
    Option<Configuration>,
    Inputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

impl ConstructorFn for SinkFn {}

/// A `SourceConstructor` generates a [`Source`](`Source`).
pub(crate) type SourceConstructor = NodeConstructor<SourceRecord, SourceFn>;

/// An `OperatorConstructor` generates a [`Operator`](`Operator`).
pub(crate) type OperatorConstructor = NodeConstructor<OperatorRecord, OperatorFn>;

/// A `SinkConstructor` generates a [`Sink`](`Sink`).
pub(crate) type SinkConstructor = NodeConstructor<SinkRecord, SinkFn>;

/// Dereferencing to the record allows for an easy access to the metadata of the node.
impl<Record, C: ConstructorFn> Deref for NodeConstructor<Record, C> {
    type Target = Record;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl<Record, C: ConstructorFn> NodeConstructor<Record, C> {
    /// Creates a NodeFactory without a `library`.
    ///
    /// This function is intended for internal use in order to create a data flow programmatically.
    pub(crate) fn new_static(record: Record, constructor: C) -> Self {
        Self {
            record,
            constructor,
            _library: None,
        }
    }

    pub(crate) fn new_dynamic(record: Record, constructor: C, library: Arc<Library>) -> Self {
        Self {
            record,
            constructor,
            _library: Some(library),
        }
    }
}
