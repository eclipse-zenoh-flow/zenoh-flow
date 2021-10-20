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

use crate::async_std::sync::{Arc, RwLock};
use crate::model::node::OperatorRecord;
use crate::runtime::graph::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::{Context, DataMessage, Operator, PortId, Token, ZFResult, State};
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use std::mem;
use uhlc::HLC;

pub type OperatorRegisterFn = fn() -> ZFResult<Arc<dyn Operator>>;

pub struct OperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: OperatorRegisterFn,
}

pub struct OperatorIO {
    inputs: HashMap<PortId, LinkReceiver<Message>>,
    outputs: HashMap<PortId, Vec<LinkSender<Message>>>,
}

impl OperatorIO {
    pub fn new(record: &OperatorRecord) -> Self {
        Self {
            inputs: HashMap::with_capacity(record.inputs.len()),
            outputs: HashMap::with_capacity(record.outputs.len()),
        }
    }
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the operator/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct OperatorRunner {
    pub record: Arc<OperatorRecord>,
    pub io: Arc<RwLock<OperatorIO>>,
    pub state: Arc<RwLock<State>>,
    pub hlc: Arc<HLC>,
    pub operator: Arc<dyn Operator>,
    pub lib: Arc<Option<Library>>,
}

impl OperatorRunner {
    pub fn new(
        record: OperatorRecord,
        hlc: Arc<HLC>,
        operator: Arc<dyn Operator>,
        lib: Option<Library>,
    ) -> Self {
        let state = operator.initialize(&record.configuration);
        Self {
            hlc,
            io: Arc::new(RwLock::new(OperatorIO::new(&record))),
            state: Arc::new(RwLock::new(state)),
            operator,
            lib: Arc::new(lib),
            record: Arc::new(record),
        }
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>) {
        let mut guard = self.io.write().await;
        let key = input.id();
        guard.inputs.insert(key, input);
    }

    pub async fn add_output(&self, output: LinkSender<Message>) {
        let mut guard = self.io.write().await;
        let key = output.id();
        if let Some(links) = guard.outputs.get_mut(key.as_ref()) {
            links.push(output);
        } else {
            guard.outputs.insert(key, vec![output]);
        }
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.operator.finalize(&mut state)
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();
        let mut tokens: HashMap<PortId, Token> = self
            .record
            .inputs
            .iter()
            .map(|port_desc| (port_desc.port_id.clone().into(), Token::NotReady))
            .collect();
        let mut data: HashMap<PortId, DataMessage> =
            HashMap::with_capacity(self.record.inputs.len());

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let io = self.io.read().await;
            let mut state = self.state.write().await;

            let mut links: Vec<_> = io.inputs.values().map(|rx| rx.recv()).collect();

            // Input Rules
            crate::run_input_rules!(self.operator, tokens, links, &mut state, &mut context);

            let mut max_token_timestamp = None;

            for (id, token) in tokens.iter_mut() {
                // TODO: Input Rules — Take action into consideration, only replace when needed.
                let old_token = mem::replace(token, Token::NotReady);
                match old_token {
                    Token::NotReady => {
                        data.remove(id);
                        log::debug!("Removing < {} > from tokens for next iteration.", id);
                    }

                    Token::Ready(ready_token) => {
                        max_token_timestamp = match max_token_timestamp {
                            None => Some(ready_token.data.timestamp),
                            Some(timestamp) => {
                                if ready_token.data.timestamp > timestamp {
                                    Some(ready_token.data.timestamp)
                                } else {
                                    Some(timestamp)
                                }
                            }
                        };

                        data.insert(id.clone(), ready_token.data);
                    }
                };
            }

            let timestamp = {
                match max_token_timestamp {
                    Some(max_timestamp) => {
                        if let Err(error) = self.hlc.update_with_timestamp(&max_timestamp) {
                            log::warn!(
                                "[HLC] Could not update HLC with timestamp {:?}: {:?}",
                                max_timestamp,
                                error
                            );
                        }

                        max_timestamp
                    }
                    None => self.hlc.new_timestamp(),
                }
            };

            // Running
            let run_outputs = self.operator.run(&mut context, &mut state, &mut data)?;

            // Output rules
            let outputs = self
                .operator
                .output_rule(&mut context, &mut state, run_outputs)?;

            // Send to Links
            for (id, output) in outputs {
                // getting link
                log::debug!("id: {:?}, message: {:?}", id, output);
                if let Some(links) = io.outputs.get(&id) {
                    let zf_message = Arc::new(Message::from_node_output(output, timestamp));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }

            // This depends on the Tokens...
            for rx in io.inputs.values() {
                rx.discard().await?;
            }
        }
    }
}
