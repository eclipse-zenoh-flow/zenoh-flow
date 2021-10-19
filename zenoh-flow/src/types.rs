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
use crate::{ControlMessage, Data, DataMessage, State, Token};
use std::collections::HashMap;

pub type OperatorId = Arc<str>;
pub type PortId = Arc<str>;
pub type RuntimeId = Arc<str>;

pub type ZFResult<T> = Result<T, ZFError>;

pub use crate::ZFError;

/// ZFContext is a structure provided by Zenoh Flow to access the execution context directly from
/// the nodes.
pub struct Context {
    pub mode: usize,
}

impl Default for Context {
    fn default() -> Self {
        Self { mode: 0 }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SerDeData {
    Serialized(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Deserialized data is never serialized directly
    Deserialized(Arc<dyn Data>),
}

#[derive(Debug, Clone)]
pub enum NodeOutput {
    Data(SerDeData),
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

impl State for EmptyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub fn default_output_rule(
    _state: &mut Box<dyn State>,
    outputs: HashMap<PortId, SerDeData>,
) -> ZFResult<HashMap<PortId, NodeOutput>> {
    let mut results = HashMap::with_capacity(outputs.len());
    for (k, v) in outputs {
        results.insert(k, NodeOutput::Data(v));
    }

    Ok(results)
}

pub fn default_input_rule(
    _state: &mut Box<dyn State>,
    tokens: &mut HashMap<PortId, Token>,
) -> ZFResult<bool> {
    for token in tokens.values() {
        match token {
            Token::Ready(_) => continue,
            Token::NotReady => return Ok(false),
        }
    }

    Ok(true)
}
