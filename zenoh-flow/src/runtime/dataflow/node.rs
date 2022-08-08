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

use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::{Configuration, NodeId, Operator, Sink, Source, ZFResult};
use async_std::sync::Arc;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

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
    ) -> ZFResult<Self> {
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
    ) -> ZFResult<Self> {
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
    ) -> ZFResult<Self> {
        Ok(Self {
            id: record.id,
            // input: record.input,
            sink,
            library: lib,
            configuration: record.configuration,
        })
    }
}
