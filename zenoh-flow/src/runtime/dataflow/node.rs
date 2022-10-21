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
use crate::traits;
use std::ops::Deref;
use std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// A `NodeFactory` generates `Node`, i.e. objects that implement the `Node` trait.
///
/// The `record` holds the metadata associated with the Node. The `factory` is the object that
/// produces the Nodes. The `_library` is a reference over the dynamically loaded shared library. It
/// can be `None` when the factory is created programmatically.
pub(crate) struct NodeFactory<U, T: ?Sized> {
    pub(crate) record: U,
    pub(crate) factory: Arc<T>,
    pub(crate) _library: Option<Arc<Library>>,
}

/// Dereferencing to the record (the generic `U`) allows for an easy access to the metadata of the
/// node.
impl<U, T: ?Sized> Deref for NodeFactory<U, T> {
    type Target = U;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl<U, T: ?Sized> NodeFactory<U, T> {
    /// Creates a NodeFactory without a `library`.
    ///
    /// This function is intended for internal use in order to create a data flow programmatically.
    pub(crate) fn new_static(record: U, factory: Arc<T>) -> Self {
        Self {
            record,
            factory,
            _library: None,
        }
    }
}

/// A `SourceFactory` is a specialized `NodeFactory` generating Source.
pub(crate) type SourceFactory = NodeFactory<SourceRecord, dyn traits::SourceFactoryTrait>;

/// An `OperatorFactory` is a specialized `NodeFactory` generating Operator.
pub(crate) type OperatorFactory = NodeFactory<OperatorRecord, dyn traits::OperatorFactoryTrait>;

/// A `SinkFactory` is a specialized `NodeFactory` generating Sink.
pub(crate) type SinkFactory = NodeFactory<SinkRecord, dyn traits::SinkFactoryTrait>;
