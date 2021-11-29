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

use async_trait::async_trait;
use flume::{Receiver, Sender};
use std::convert::TryInto;
use zenoh_flow::{Data, Deserializable, Node, Sink, Source, State, ZFData, ZFError, ZFResult};
use zenoh_flow_derive::{ZFData, ZFState};

#[derive(Debug, Clone, ZFData)]
pub struct ZFUsize(pub usize);

impl ZFData for ZFUsize {
    fn try_serialize(&self) -> ZFResult<Vec<u8>> {
        Ok(self.0.to_ne_bytes().to_vec())
    }
}

impl Deserializable for ZFUsize {
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized,
    {
        let value =
            usize::from_ne_bytes(bytes.try_into().map_err(|_| ZFError::DeseralizationError)?);
        Ok(ZFUsize(value))
    }
}

#[derive(ZFState, Debug)]
pub struct CounterState {
    counter: usize,
}

#[allow(dead_code)]
impl CounterState {
    pub fn new_as_state() -> State {
        State::from(Self { counter: 0 })
    }

    pub fn add_fetch(&mut self) -> usize {
        self.counter += 1;
        self.counter
    }
}

#[derive(Debug, ZFState)]
pub struct VecState {
    values: Vec<usize>,
}

pub struct VecSource {
    values: Vec<usize>,
    unused_rx: Receiver<()>,
    _unused_tx: Sender<()>,
}

#[allow(dead_code)]
impl VecSource {
    pub fn new(values: Vec<usize>) -> Self {
        let (_unused_tx, unused_rx) = flume::bounded::<()>(1);
        Self {
            values,
            unused_rx,
            _unused_tx,
        }
    }
}

impl Node for VecSource {
    fn initialize(&self, _configuration: &Option<zenoh_flow::Configuration>) -> ZFResult<State> {
        Ok(State::from(VecState {
            values: self.values.clone(),
        }))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Source for VecSource {
    async fn run(&self, _context: &mut zenoh_flow::Context, state: &mut State) -> ZFResult<Data> {
        let state = state.try_get::<VecState>()?;
        let value = match state.values.pop() {
            Some(value) => value,
            None => {
                // No more value, wait indefinitely.
                self.unused_rx
                    .recv_async()
                    .await
                    .map_err(|e| ZFError::IOError(e.to_string()))?;
                return Err(ZFError::Disconnected);
            }
        };

        Ok(Data::from::<ZFUsize>(ZFUsize(value)))
    }
}

pub struct VecSink {
    values: Vec<usize>,
    tx: Sender<()>,
}

#[allow(dead_code)]
impl VecSink {
    pub fn new(tx: Sender<()>, values: Vec<usize>) -> Self {
        Self { values, tx }
    }
}

impl Node for VecSink {
    fn initialize(&self, _configuration: &Option<zenoh_flow::Configuration>) -> ZFResult<State> {
        Ok(State::from(VecState {
            values: self.values.clone(),
        }))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Sink for VecSink {
    async fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        mut input: zenoh_flow::DataMessage,
    ) -> ZFResult<()> {
        let data = input.get_inner_data().try_get::<ZFUsize>()?;
        let state = state.try_get::<VecState>()?;

        let value = match state.values.pop() {
            Some(value) => value,
            None => {
                return Ok(());
            }
        };

        assert_eq!(value, data.0);

        if state.values.is_empty() {
            // No more value, inform via the Sender `tx` and wait indefinitely.
            self.tx
                .send_async(())
                .await
                .map_err(|e| ZFError::IOError(e.to_string()))?;
        }

        Ok(())
    }
}
