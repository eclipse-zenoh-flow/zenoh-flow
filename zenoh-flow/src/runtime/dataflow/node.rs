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
use crate::traits::{self, Operator, Sink, Source};
use crate::types::{Configuration, NodeId};
use crate::Result;
use std::ops::Deref;
use std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// TODO(J-Loudet) Improve documentation.
///
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

/// TODO(J-Loudet) Improve documentation.
///
/// Dereferencing to the record (the generic `U`) allows us to access the metadata of the node.
impl<U, T: ?Sized> Deref for NodeFactory<U, T> {
    type Target = U;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl<U, T: ?Sized> NodeFactory<U, T> {
    /// TODO(J-Loudet) Improve documentation.
    ///
    /// This function is, for now, mostly used to generate data flows programmatically.
    pub(crate) fn new_static(record: U, factory: Arc<T>) -> Self {
        Self {
            record,
            factory,
            _library: None,
        }
    }
}

/// A `SourceFactory` is a specialized `NodeFactory` generating Source.
pub(crate) type SourceFactory = NodeFactory<SourceRecord, dyn traits::SourceFactory>;

/// An `OperatorFactory` is a specialized `NodeFactory` generating Operator.
pub(crate) type OperatorFactory = NodeFactory<OperatorRecord, dyn traits::OperatorFactory>;

/// A `SinkFactory` is a specialized `NodeFactory` generating Sink.
pub(crate) type SinkFactory = NodeFactory<SinkRecord, dyn traits::SinkFactory>;

/// A Source that was loaded, either dynamically or statically.
/// When a source is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the source.
pub struct SourceLoaded {
    pub(crate) id: NodeId,
    pub(crate) configuration: Option<Configuration>,
    // pub(crate) output: PortRecord,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SourceLoaded {
    /// Creates and initialzes a `Source`.
    /// The state is stored within the `SourceLoaded` in order to be used
    /// when calling the Source's callbacks.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to initialize
    pub fn try_new(
        record: SourceRecord,
        lib: Option<Arc<Library>>,
        source: Arc<dyn Source>,
    ) -> Result<Self> {
        Ok(Self {
            id: record.id,
            configuration: record.configuration,
            // output: record.output,
            source,
            library: lib,
        })
    }
}

/// An Operator that was loaded, either dynamically or statically.
/// When a operator is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the operator.
pub struct OperatorLoaded {
    pub(crate) id: NodeId,
    pub(crate) configuration: Option<Configuration>,
    // pub(crate) inputs: HashMap<PortId, PortType>,
    // pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) library: Option<Arc<Library>>,
}

impl OperatorLoaded {
    /// Creates and initialzes an `Operator`.
    /// The state is stored within the `OperatorLoaded` in order to be used
    /// when calling the Operators's callbacks.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to initialize
    pub fn try_new(
        record: OperatorRecord,
        lib: Option<Arc<Library>>,
        operator: Arc<dyn Operator>,
    ) -> Result<Self> {
        // let inputs: HashMap<PortId, PortType> = record
        //     .inputs
        //     .into_iter()
        //     .map(|desc| (desc.port_id, desc.port_type))
        //     .collect();

        // let outputs: HashMap<PortId, PortType> = record
        //     .outputs
        //     .into_iter()
        //     .map(|desc| (desc.port_id, desc.port_type))
        //     .collect();

        Ok(Self {
            id: record.id,
            configuration: record.configuration,
            // inputs,
            // outputs,
            operator,
            library: lib,
        })
    }
}

/// A Sink that was loaded, either dynamically or statically.
/// When a sink is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the sink.
pub struct SinkLoaded {
    pub(crate) id: NodeId,
    pub(crate) configuration: Option<Configuration>,
    // pub(crate) input: PortRecord,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SinkLoaded {
    /// Creates and initialzes a `Sink`.
    /// The state is stored within the `SinkLoaded` in order to be used
    /// when calling the Sink's callbacks.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to initialize
    pub fn try_new(
        record: SinkRecord,
        lib: Option<Arc<Library>>,
        sink: Arc<dyn Sink>,
    ) -> Result<Self> {
        Ok(Self {
            id: record.id,
            // input: record.input,
            sink,
            library: lib,
            configuration: record.configuration,
        })
    }
}
