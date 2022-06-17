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

use crate::model::link::PortDescriptor;
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::{NodeId, Operator, PortId, PortType, Sink, Source, State, ZFResult};
use async_std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Duration;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// A Source that was loaded, either dynamically or statically.
/// When a source is loaded it is first initialized and then can be ran.
/// This struct is then used within a `Runner` to actually run the source.
pub struct SourceLoaded {
    pub(crate) id: NodeId,
    pub(crate) output: PortDescriptor,
    pub(crate) period: Option<Duration>,
    pub(crate) state: Arc<Mutex<State>>,
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
        let state = source.initialize(&record.configuration)?;

        Ok(Self {
            id: record.id,
            output: record.output,
            state: Arc::new(Mutex::new(state)),
            period: record.period.map(|dur_desc| dur_desc.to_duration()),
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
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) state: State,
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
        let state = operator.initialize(&record.configuration)?;

        let inputs: HashMap<PortId, PortType> = record
            .inputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        let outputs: HashMap<PortId, PortType> = record
            .outputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        Ok(Self {
            id: record.id,
            state,
            inputs,
            outputs,
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
    pub(crate) input: PortDescriptor,
    pub(crate) state: Arc<Mutex<State>>,
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
        let state = sink.initialize(&record.configuration)?;

        Ok(Self {
            id: record.id,
            input: record.input,
            state: Arc::new(Mutex::new(state)),
            sink,
            library: lib,
        })
    }
}
