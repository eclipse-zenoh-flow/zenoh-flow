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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, NodeId, PortId, RuntimeId};
use zenoh_flow_descriptors::{
    FlattenedOperatorDescriptor, FlattenedSinkDescriptor, FlattenedSourceDescriptor, SinkLibrary,
    SourceLibrary,
};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct OperatorRecord {
    pub id: NodeId,
    pub description: Arc<str>,
    pub library: Arc<str>,
    pub outputs: Vec<PortId>,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    #[serde(flatten)]
    pub(crate) runtime: RuntimeId,
}

impl OperatorRecord {
    pub fn new(descriptor: FlattenedOperatorDescriptor, default_runtime: RuntimeId) -> Self {
        Self {
            id: descriptor.id,
            description: descriptor.description,
            library: descriptor.library,
            outputs: descriptor.outputs,
            inputs: descriptor.inputs,
            configuration: descriptor.configuration,
            runtime: descriptor.runtime.map_or(default_runtime, |r| r),
        }
    }

    pub fn runtime(&self) -> &RuntimeId {
        &self.runtime
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkRecord {
    pub id: NodeId,
    pub description: Arc<str>,
    pub library: SinkLibrary,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    pub(crate) runtime: RuntimeId,
}

impl SinkRecord {
    pub fn new(descriptor: FlattenedSinkDescriptor, default_runtime: RuntimeId) -> Self {
        Self {
            id: descriptor.id,
            description: descriptor.description,
            library: descriptor.library,
            inputs: descriptor.inputs,
            configuration: descriptor.configuration,
            runtime: descriptor.runtime.map_or(default_runtime, |r| r),
        }
    }

    pub fn runtime(&self) -> &RuntimeId {
        &self.runtime
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceRecord {
    pub id: NodeId,
    pub description: Arc<str>,
    pub library: SourceLibrary,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    pub(crate) runtime: RuntimeId,
}

impl SourceRecord {
    pub fn new(descriptor: FlattenedSourceDescriptor, default_runtime: RuntimeId) -> Self {
        Self {
            id: descriptor.id,
            description: descriptor.description,
            library: descriptor.library,
            outputs: descriptor.outputs,
            configuration: descriptor.configuration,
            runtime: descriptor.runtime.map_or(default_runtime, |r| r),
        }
    }

    pub fn runtime(&self) -> &RuntimeId {
        &self.runtime
    }
}
