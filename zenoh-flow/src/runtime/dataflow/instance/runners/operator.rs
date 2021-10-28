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
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::node::OperatorLoaded;
use crate::runtime::message::Message;
use crate::runtime::RuntimeContext;
use crate::{Context, DataMessage, NodeId, Operator, PortId, State, Token, ZFError, ZFResult};
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use std::mem;

#[derive(Default)]
pub struct OperatorIO {
    inputs: HashMap<PortId, LinkReceiver<Message>>,
    outputs: HashMap<PortId, Vec<LinkSender<Message>>>,
}

pub type InputsLink = HashMap<PortId, LinkReceiver<Message>>;
pub type OutputsLinks = HashMap<PortId, Vec<LinkSender<Message>>>;

impl OperatorIO {
    pub fn new(record: &OperatorRecord) -> Self {
        Self {
            inputs: HashMap::with_capacity(record.inputs.len()),
            outputs: HashMap::with_capacity(record.outputs.len()),
        }
    }

    pub fn try_add_input(&mut self, rx: LinkReceiver<Message>) -> ZFResult<()> {
        if self.inputs.contains_key(&rx.id()) {
            return Err(ZFError::DuplicatedInputPort((rx.id(), rx.id())));
        }

        self.inputs.insert(rx.id(), rx);

        Ok(())
    }

    pub fn add_output(&mut self, tx: LinkSender<Message>) {
        if let Some(vec_senders) = self.outputs.get_mut(&tx.id()) {
            vec_senders.push(tx);
        } else {
            self.outputs.insert(tx.id(), vec![tx]);
        }
    }

    pub fn take(self) -> (InputsLink, OutputsLinks) {
        (self.inputs, self.outputs)
    }
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the operator/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct OperatorRunner {
    pub(crate) id: NodeId,
    pub(crate) runtime_context: RuntimeContext,
    pub(crate) io: Arc<RwLock<OperatorIO>>,
    pub(crate) inputs: HashMap<PortId, String>,
    pub(crate) outputs: HashMap<PortId, String>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) library: Option<Arc<Library>>,
}

impl OperatorRunner {
    pub fn try_new(
        context: RuntimeContext,
        operator: OperatorLoaded,
        operator_io: OperatorIO,
    ) -> ZFResult<Self> {
        // TODO Check that all ports are used.
        Ok(Self {
            id: operator.id,
            runtime_context: context,
            io: Arc::new(RwLock::new(operator_io)),
            inputs: operator.inputs,
            outputs: operator.outputs,
            state: operator.state,
            operator: operator.operator,
            library: operator.library,
        })
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
            .inputs
            .keys()
            .map(|input_id| (input_id.clone(), Token::NotReady))
            .collect();
        let mut data: HashMap<PortId, DataMessage> = HashMap::with_capacity(tokens.len());

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let io = self.io.read().await;
            let mut state = self.state.write().await;

            let mut links: Vec<_> = io.inputs.values().map(|rx| rx.recv()).collect();

            // Input Rules
            crate::run_input_rules!(self.operator, tokens, links, &mut state, &mut context);

            let mut earliest_source_timestamp = None;

            for (id, token) in tokens.iter_mut() {
                // TODO: Input Rules — Take action into consideration, only replace when needed.
                let old_token = mem::replace(token, Token::NotReady);
                match old_token {
                    Token::NotReady => {
                        data.remove(id);
                        log::debug!("Removing < {} > from tokens for next iteration.", id);
                    }

                    Token::Ready(ready_token) => {
                        earliest_source_timestamp = match earliest_source_timestamp {
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
                match earliest_source_timestamp {
                    Some(max_timestamp) => {
                        if let Err(error) = self
                            .runtime_context
                            .hlc
                            .update_with_timestamp(&max_timestamp)
                        {
                            log::warn!(
                                "[HLC] Could not update HLC with timestamp {:?}: {:?}",
                                max_timestamp,
                                error
                            );
                        }

                        max_timestamp
                    }
                    None => self.runtime_context.hlc.new_timestamp(),
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
        }
    }
}
