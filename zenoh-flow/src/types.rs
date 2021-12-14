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

use crate::async_std::sync::Arc;
use crate::serde::{Deserialize, Serialize};
use crate::{ControlMessage, DataMessage, Token, ZFData, ZFState};
use std::collections::HashMap;
use std::time::Duration;

pub type NodeId = Arc<str>;
pub type PortId = Arc<str>;
pub type RuntimeId = Arc<str>;
pub type FlowId = Arc<str>;
pub type PortType = Arc<str>;

pub type ZFResult<T> = Result<T, ZFError>;

pub use crate::ZFError;

/// ZFContext is a structure provided by Zenoh Flow to access the execution context directly from
/// the nodes.
#[derive(Default, Debug)]
pub struct Context {
    pub mode: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Typed data is never serialized using serde
    Typed(Arc<dyn ZFData>),
}

impl Data {
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::Bytes(Arc::new(bytes))
    }

    pub fn try_as_bytes(&self) -> ZFResult<Arc<Vec<u8>>> {
        match &self {
            Self::Bytes(bytes) => Ok(bytes.clone()),
            Self::Typed(typed) => {
                let serialized_data = typed
                    .try_serialize()
                    .map_err(|_| ZFError::SerializationError)?;
                Ok(Arc::new(serialized_data))
            }
        }
    }

    pub fn from_arc<Typed>(arc: Arc<Typed>) -> Self
    where
        Typed: ZFData + 'static,
    {
        Self::Typed(arc)
    }

    pub fn from<Typed>(typed: Typed) -> Self
    where
        Typed: ZFData + 'static,
    {
        Self::Typed(Arc::new(typed))
    }

    pub fn try_get<Typed>(&mut self) -> ZFResult<&Typed>
    where
        Typed: ZFData + crate::Deserializable + 'static,
    {
        *self = (match &self {
            Self::Bytes(bytes) => {
                let data: Arc<dyn ZFData> = Arc::new(
                    Typed::try_deserialize(bytes.as_slice())
                        .map_err(|_| crate::types::ZFError::DeseralizationError)?,
                );
                Ok(Self::Typed(data.clone()))
            }
            Self::Typed(typed) => Ok(Self::Typed(typed.clone())),
        } as ZFResult<Self>)?;

        match self {
            Self::Typed(typed) => Ok(typed
                .as_any()
                .downcast_ref::<Typed>()
                .ok_or_else(|| ZFError::InvalidData("Could not downcast.".to_string()))?),
            _ => Err(ZFError::InvalidData(
                "Should be deserialized first".to_string(),
            )),
        }
    }
}

pub struct State {
    state: Box<dyn ZFState>,
}

impl State {
    pub fn from_box<S>(boxed: Box<S>) -> Self
    where
        S: ZFState + 'static,
    {
        Self { state: boxed }
    }

    pub fn from<S>(state: S) -> Self
    where
        S: ZFState + 'static,
    {
        Self {
            state: Box::new(state),
        }
    }

    pub fn try_get<S>(&mut self) -> ZFResult<&mut S>
    where
        S: ZFState + 'static,
    {
        self.state
            .as_mut_any()
            .downcast_mut::<S>()
            .ok_or_else(|| ZFError::InvalidData("Could not downcast.".to_string()))
    }
}

#[derive(Debug, Clone)]
pub enum NodeOutput {
    Data(Data),
    // TODO Users should not have access to all control messages. When implementing the control
    // messages change this to an enum with a "limited scope".
    Control(ControlMessage),
}

#[derive(Debug, Clone)]
pub struct ZFInput(HashMap<String, DataMessage>);

impl Default for ZFInput {
    fn default() -> Self {
        Self::new()
    }
}

impl ZFInput {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, id: String, data: DataMessage) -> Option<DataMessage> {
        self.0.insert(id, data)
    }

    pub fn get(&self, id: &str) -> Option<&DataMessage> {
        self.0.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut DataMessage> {
        self.0.get_mut(id)
    }
}

impl<'a> IntoIterator for &'a ZFInput {
    type Item = (&'a String, &'a DataMessage);
    type IntoIter = std::collections::hash_map::Iter<'a, String, DataMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone)]
pub struct EmptyState;

impl ZFState for EmptyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub fn default_output_rule(
    _state: &mut State,
    outputs: HashMap<PortId, Data>,
) -> ZFResult<HashMap<PortId, NodeOutput>> {
    let mut results = HashMap::with_capacity(outputs.len());
    for (k, v) in outputs {
        results.insert(k, NodeOutput::Data(v));
    }

    Ok(results)
}

pub fn default_input_rule(
    _state: &mut State,
    tokens: &mut HashMap<PortId, Token>,
) -> ZFResult<bool> {
    for token in tokens.values() {
        match token {
            Token::Ready(_) => continue,
            Token::Pending => return Ok(false),
        }
    }

    Ok(true)
}

pub type Configuration = serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DurationUnit {
    #[serde(alias = "s")]
    #[serde(alias = "second")]
    #[serde(alias = "seconds")]
    Second,
    #[serde(alias = "ms")]
    #[serde(alias = "millisecond")]
    #[serde(alias = "milliseconds")]
    Millisecond,
    #[serde(alias = "us")]
    #[serde(alias = "µs")]
    #[serde(alias = "microsecond")]
    #[serde(alias = "microseconds")]
    Microsecond,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DurationDescriptor {
    #[serde(alias = "duration")]
    pub(crate) length: u64,
    pub(crate) unit: DurationUnit,
}

impl DurationDescriptor {
    pub fn to_duration(&self) -> Duration {
        match self.unit {
            DurationUnit::Second => Duration::from_secs(self.length),
            DurationUnit::Millisecond => Duration::from_millis(self.length),
            DurationUnit::Microsecond => Duration::from_micros(self.length),
        }
    }
}
