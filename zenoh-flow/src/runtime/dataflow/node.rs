//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::model::link::PortDescriptor;
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::model::period::PeriodDescriptor;
use crate::{NodeId, Operator, PortId, PortType, Sink, Source, State, ZFResult};
use async_std::sync::{Arc, RwLock};
use libloading::Library;
use std::collections::HashMap;
use std::time::Duration;

pub struct SourceLoaded {
    pub(crate) id: NodeId,
    pub(crate) output: PortDescriptor,
    pub(crate) period: Option<PeriodDescriptor>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SourceLoaded {
    pub fn try_new(
        record: SourceRecord,
        lib: Option<Arc<Library>>,
        source: Arc<dyn Source>,
    ) -> ZFResult<Self> {
        let state = source.initialize(&record.configuration)?;

        Ok(Self {
            id: record.id,
            output: record.output,
            period: record.period,
            state: Arc::new(RwLock::new(state)),
            source,
            library: lib,
        })
    }
}

pub struct OperatorLoaded {
    pub(crate) id: NodeId,
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) deadline: Option<Duration>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) library: Option<Arc<Library>>,
}

impl OperatorLoaded {
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
            inputs,
            outputs,
            deadline: record.deadline,
            state: Arc::new(RwLock::new(state)),
            operator,
            library: lib,
        })
    }
}

pub struct SinkLoaded {
    pub(crate) id: NodeId,
    pub(crate) input: PortDescriptor,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SinkLoaded {
    pub fn try_new(
        record: SinkRecord,
        lib: Option<Arc<Library>>,
        sink: Arc<dyn Sink>,
    ) -> ZFResult<Self> {
        let state = sink.initialize(&record.configuration)?;

        Ok(Self {
            id: record.id,
            input: record.input,
            state: Arc::new(RwLock::new(state)),
            sink,
            library: lib,
        })
    }
}
