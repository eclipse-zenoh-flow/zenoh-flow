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

use crate::async_std::sync::Arc;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::serde::{Deserialize, Serialize};
use crate::{ControlMessage, InputToken, ZFData, ZFState};
use std::collections::HashMap;
use std::time::Duration;
/// A NodeId identifies a node inside a Zenoh Flow graph
pub type NodeId = Arc<str>;
/// A PortId identifies a port within an node.
pub type PortId = Arc<str>;
/// A RuntimeId identifies a runtime within the Zenoh Flow infrastructure.
pub type RuntimeId = Arc<str>;
/// A FlowId identifies a Zenoh Flow graph within Zenoh Flow
pub type FlowId = Arc<str>;
/// The PortType identifies the type of the data expected in a port.
pub type PortType = Arc<str>;

/// The Zenoh Flow result type.
pub type ZFResult<T> = Result<T, ZFError>;

pub use crate::ZFError;

/// Context is a structure provided by Zenoh Flow to access
/// the execution context directly from the nodes.
/// It contains the `mode` as usize.
#[derive(Default, Debug)]
pub struct Context {
    pub mode: usize,
}

/// The Zenoh Flow data.
/// It is an `enum` that can contain both the serialized data (if received from
/// the network, or from nodes not written in Rust),
/// or the actual `Typed` data as [`ZFData`](`ZFData`).
/// The `Typed` data is never serialized directly when sending over Zenoh
/// or to an operator not written in Rust.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    /// Serialized data, coming either from Zenoh of from non-rust node.
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    /// Actual data as instance of 'ZFData` coming from a Rust node.
    /// This is never serialized directly.
    Typed(Arc<dyn ZFData>),
}

impl Data {
    /// Creates a new `Data` from a `Vec<u8>`,
    /// In order to avoid copies it puts the data inside an `Arc`.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::Bytes(Arc::new(bytes))
    }

    /// Tries to return a serialized representation of the data.
    /// It does not actually change the internal representation.
    /// The serialized representation in stored inside an `Arc`
    /// to avoid copies.
    ///
    /// # Errors
    /// If it fails to serialize an error
    /// variant will be returned.
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

    /// Creates a Data from an `Arc` of  typed data.
    /// The typed data has to be an instance of `ZFData`.
    pub fn from_arc<Typed>(arc: Arc<Typed>) -> Self
    where
        Typed: ZFData + 'static,
    {
        Self::Typed(arc)
    }

    /// Creates a Data from  typed data.
    /// The typed data has to be an instance of `ZFData`.
    /// The data is then stored inside an `Arc` to avoid copies.
    pub fn from<Typed>(typed: Typed) -> Self
    where
        Typed: ZFData + 'static,
    {
        Self::Typed(Arc::new(typed))
    }

    /// Tries to cast the data to the given type.
    /// If the data is represented as serialized, this will try to deserialize
    /// the bytes and change the internal representation of the data.
    /// If the data is already represented with as `Typed` then it will
    /// return an *immutable* reference to the internal data.
    /// This reference is *immutable* because one Output can send data to
    /// multiple Inputs, therefore to avoid copies the same `Arc` is sent
    /// to multiple operators, thus it is multiple-owned and the data inside
    /// cannot be modified.
    ///
    /// # Errors
    /// If fails to cast an error
    /// variant will be returned.
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

/// This structs stores a node state in the heap.
pub struct State {
    state: Box<dyn ZFState>,
}

impl State {
    /// Creates a new `State`, from an already boxed state.
    /// The state has to be an instance of [`ZFState`]`ZFState`
    pub fn from_box<S>(boxed: Box<S>) -> Self
    where
        S: ZFState + 'static,
    {
        Self { state: boxed }
    }

    /// Creates a new `State` from the provided state.
    /// The state has to be an instance of [`ZFState`]`ZFState`
    pub fn from<S>(state: S) -> Self
    where
        S: ZFState + 'static,
    {
        Self {
            state: Box::new(state),
        }
    }

    /// Tries to cast the state to the given type.
    /// It returns a mutable reference to the internal state, so user can
    /// modify it.
    ///
    /// # Errors
    /// If it fails to cast an error
    /// variant will be returned.
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

/// Represents the output of a node.
/// A node can either send `Data` or `Control`
/// Where the first is a [`Data`](`Data`) and the latter
/// a [`ControlMessage`](`ControlMessage`).
///
///
/// *NOTE:* Handling of control messages is not yet implemented.
#[derive(Debug, Clone)]
pub enum NodeOutput {
    Data(Data),
    // TODO Users should not have access to all control messages. When implementing the control
    // messages change this to an enum with a "limited scope".
    Control(ControlMessage),
}

/// The empty state is a commodity struct provided to user that do not
/// need any state for they operators.
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

/// Commodity function for users that do not need output
/// rules in their operators.
/// The data is simply converted to the
/// expected [`NodeOutput`](`NodeOutput`) type.
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

/// Commodity function for users that need their operators to behave
/// in a KPN manner:
/// all inputs must be present before a computation can be triggered.
pub fn default_input_rule(
    _state: &mut State,
    tokens: &mut HashMap<PortId, InputToken>,
) -> ZFResult<bool> {
    for token in tokens.values() {
        match token {
            InputToken::Ready(_) => continue,
            InputToken::Pending => return Ok(false),
        }
    }

    Ok(true)
}

/// The generic configuration of a graph node.
/// It is a re-export of `serde_json::Value`
pub type Configuration = serde_json::Value;

/// Merges two configurations, keeping the values of the `local` Configuration in case of duplicated
/// keys.
///
/// This function was created with the idea of merging (and overwriting) a `global` Configuration,
/// common to all nodes of a graph, with a `local` one, i.e. specific to a node.
pub(crate) fn merge_configurations(
    global: Option<Configuration>,
    local: Option<Configuration>,
) -> Option<Configuration> {
    match (global, local) {
        (None, None) => None,
        (None, Some(local)) => Some(local),
        (Some(global), None) => Some(global),
        (Some(mut global), Some(mut local)) => {
            global
                .as_object_mut()
                .unwrap()
                .append(local.as_object_mut().unwrap());
            Some(global)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::merge_configurations;
    use serde_json::json;

    #[test]
    fn test_merge_configurations() {
        let global = json!({ "a": { "nested": true }, "b": ["an", "array"] });
        let local = json!({ "a": { "not-nested": false }, "c": 1 });

        assert_eq!(
            merge_configurations(Some(global.clone()), Some(local.clone())),
            Some(json!({ "a": { "not-nested": false }, "b": ["an", "array"], "c": 1 }))
        );

        assert_eq!(merge_configurations(None, Some(local.clone())), Some(local));
        assert_eq!(
            merge_configurations(Some(global.clone()), None),
            Some(global)
        );
    }
}

/// The unit of duration used in different descriptors.
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

/// The descriptor for a duration.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DurationDescriptor {
    #[serde(alias = "duration")]
    pub(crate) length: u64,
    pub(crate) unit: DurationUnit,
}

impl DurationDescriptor {
    /// Converts the [`DurationDescriptor`](`DurationDescriptor`) to a [`Duration`](`Duration`).
    pub fn to_duration(&self) -> Duration {
        match self.unit {
            DurationUnit::Second => Duration::from_secs(self.length),
            DurationUnit::Millisecond => Duration::from_millis(self.length),
            DurationUnit::Microsecond => Duration::from_micros(self.length),
        }
    }
}

// TODO Implement iterator?
#[derive(Clone, Debug)]
pub struct Inputs {
    pub(crate) hmap: HashMap<PortId, Vec<LinkReceiver>>,
}

impl Inputs {
    pub(crate) fn new() -> Self {
        Self {
            hmap: HashMap::new(),
        }
    }

    pub fn get(&self, port_id: &str) -> Option<&Vec<LinkReceiver>> {
        self.hmap.get(port_id)
    }

    pub fn remove(&mut self, port_id: &str) -> Option<Vec<LinkReceiver>> {
        self.hmap.remove(port_id)
    }

    pub(crate) fn add(&mut self, rx: LinkReceiver) {
        let port_id = rx.id();
        if let Some(receivers) = self.hmap.get_mut(&port_id) {
            receivers.push(rx);
        } else {
            self.hmap.insert(port_id, vec![rx]);
        }
    }
}

// TODO Implement iterator?
#[derive(Clone)]
pub struct Outputs {
    pub(crate) hmap: HashMap<PortId, Vec<LinkSender>>,
}

impl Outputs {
    pub(crate) fn new() -> Self {
        Self {
            hmap: HashMap::new(),
        }
    }

    pub fn get(&self, port_id: &str) -> Option<&Vec<LinkSender>> {
        self.hmap.get(port_id)
    }

    pub fn remove(&mut self, port_id: &str) -> Option<Vec<LinkSender>> {
        self.hmap.remove(port_id)
    }

    pub(crate) fn add(&mut self, tx: LinkSender) {
        let port_id = tx.id();
        if let Some(senders) = self.hmap.get_mut(&port_id) {
            senders.push(tx);
        } else {
            self.hmap.insert(port_id, vec![tx]);
        }
    }
}
