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

use std::collections::HashMap;
use std::convert::TryFrom;

use crate::model::link::PortDescriptor;
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::model::period::PeriodDescriptor;
use crate::runtime::dataflow::loader::{load_operator, load_sink, load_source};
use crate::{NodeId, Operator, PortId, PortType, Sink, Source, State, ZFError};
use async_std::sync::{Arc, RwLock};
use libloading::Library;

pub struct SourceLoaded {
    pub(crate) id: NodeId,
    pub(crate) output: PortDescriptor,
    pub(crate) period: Option<PeriodDescriptor>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) library: Option<Arc<Library>>,
}

impl TryFrom<SourceRecord> for SourceLoaded {
    type Error = ZFError;

    fn try_from(value: SourceRecord) -> Result<Self, Self::Error> {
        let uri = value.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Source < {} >.",
                value.id.clone()
            ))
        })?;
        let (library, source) = load_source(uri)?;
        let state = source.initialize(&value.configuration)?;

        Ok(Self {
            id: value.id,
            output: value.output,
            period: value.period,
            state: Arc::new(RwLock::new(state)),
            source,
            library: Some(Arc::new(library)),
        })
    }
}

pub struct OperatorLoaded {
    pub(crate) id: NodeId,
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) library: Option<Arc<Library>>,
}

impl TryFrom<OperatorRecord> for OperatorLoaded {
    type Error = ZFError;

    fn try_from(value: OperatorRecord) -> Result<Self, Self::Error> {
        let uri = value.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Operator < {} >.",
                value.id.clone()
            ))
        })?;
        let (library, operator) = load_operator(uri)?;
        let state = operator.initialize(&value.configuration)?;

        let inputs: HashMap<PortId, PortType> = value
            .inputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        let outputs: HashMap<PortId, PortType> = value
            .outputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        Ok(Self {
            id: value.id,
            inputs,
            outputs,
            state: Arc::new(RwLock::new(state)),
            operator,
            library: Some(Arc::new(library)),
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

impl TryFrom<SinkRecord> for SinkLoaded {
    type Error = ZFError;

    fn try_from(value: SinkRecord) -> Result<Self, Self::Error> {
        let uri = value.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Sink < {} >.",
                value.id.clone()
            ))
        })?;
        let (library, sink) = load_sink(uri)?;
        let state = sink.initialize(&value.configuration)?;

        Ok(Self {
            id: value.id,
            input: value.input,
            state: Arc::new(RwLock::new(state)),
            sink,
            library: Some(Arc::new(library)),
        })
    }
}
