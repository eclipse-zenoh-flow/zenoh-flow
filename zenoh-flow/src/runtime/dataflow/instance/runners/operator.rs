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
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::OperatorLoaded;
use crate::runtime::message::Message;
use crate::runtime::RuntimeContext;
use crate::{
    Context, DataMessage, DeadlineMiss, NodeId, Operator, PortId, PortType, State, Token,
    TokenAction, ZFError, ZFResult,
};
use async_trait::async_trait;
use futures::{future, Future};
use libloading::Library;
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Default)]
pub struct OperatorIO {
    inputs: HashMap<PortId, LinkReceiver<Message>>,
    outputs: HashMap<PortId, Vec<LinkSender<Message>>>,
}

type LinkRecvFut<'a> = std::pin::Pin<
    Box<dyn Future<Output = Result<(Arc<str>, Arc<Message>), ZFError>> + Send + Sync + 'a>,
>;

impl OperatorIO {
    fn poll_input(&self, node_id: &NodeId, port_id: &PortId) -> ZFResult<LinkRecvFut> {
        let rx = self.inputs.get(port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "[Operator: {}] Link < {} > no longer exists.",
                node_id, port_id
            ))
        })?;
        Ok(rx.recv())
    }
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
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) deadline: Option<Duration>,
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
            deadline: operator.deadline,
        })
    }
}

#[async_trait]
impl Runner for OperatorRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Operator
    }

    async fn add_input(&self, input: LinkReceiver<Message>) -> ZFResult<()> {
        let mut guard = self.io.write().await;
        let key = input.id();
        guard.inputs.insert(key, input);
        Ok(())
    }

    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()> {
        let mut guard = self.io.write().await;
        let key = output.id();
        if let Some(links) = guard.outputs.get_mut(key.as_ref()) {
            links.push(output);
        } else {
            guard.outputs.insert(key, vec![output]);
        }
        Ok(())
    }

    async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.operator.finalize(&mut state)
    }

    async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();
        let mut tokens: HashMap<PortId, Token> = self
            .inputs
            .keys()
            .map(|input_id| (input_id.clone(), Token::Pending))
            .collect();
        let mut data: HashMap<PortId, DataMessage> = HashMap::with_capacity(tokens.len());

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let io = self.io.read().await;
            let mut state = self.state.write().await;

            let mut links = Vec::with_capacity(tokens.len());

            // Only call `recv` on links where the corresponding Token is `Pending`. If a
            // `ReadyToken` has its action set to `Keep` then it will stay as a `ReadyToken` (i.e.
            // it won’t be resetted later on) and we should not poll data.
            for (port_id, token) in tokens.iter() {
                if let Token::Pending = token {
                    links.push(io.poll_input(&self.id, port_id)?);
                }
            }

            'input_rule: loop {
                if !links.is_empty() {
                    match future::select_all(links).await {
                        (Ok((id, message)), _index, remaining) => {
                            match message.as_ref() {
                                Message::Data(_) => {
                                    tokens.insert(id, Token::from(message));
                                }
                                Message::Control(_) => {
                                    return Err(ZFError::Unimplemented);
                                }
                            }

                            links = remaining;
                        }
                        (Err(e), _index, _remaining) => {
                            let err_msg =
                                format!("[Operator: {}] Link returned an error: {:?}", self.id, e);
                            log::error!("{}", &err_msg);
                            return Err(ZFError::IOError(err_msg));
                        }
                    }
                }

                match self
                    .operator
                    .input_rule(&mut context, &mut state, &mut tokens)
                {
                    Ok(true) => {
                        log::debug!("[Operator: {}] Input Rule returned < true >.", self.id);
                        break 'input_rule;
                    }
                    Ok(false) => {
                        log::debug!("[Operator: {}] Input Rule returned < false >.", self.id);
                    }
                    Err(e) => {
                        log::error!(
                            "[Operator: {}] Input Rule returned an error: {:?}",
                            self.id,
                            e
                        );
                        return Err(ZFError::IOError(e.to_string()));
                    }
                }

                // Poll on the links where the action of the `Token` was set to `drop`.
                for (port_id, token) in tokens.iter_mut() {
                    if token.should_drop() {
                        *token = Token::Pending;
                        links.push(io.poll_input(&self.id, port_id)?);
                    }
                }
            } // end < 'input_rule: loop >

            let mut earliest_source_timestamp = None;

            for (port_id, token) in tokens.iter_mut() {
                match token {
                    Token::Pending => {
                        log::debug!(
                            "[Operator: {}] Removing < {} > from Data transmitted to `run`.",
                            self.id,
                            port_id
                        );
                        data.remove(port_id);
                        continue;
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

                        match ready_token.action {
                            TokenAction::Consume => {
                                log::debug!("[Operator: {}] Consuming < {} >.", self.id, port_id);
                                data.insert(port_id.clone(), ready_token.data.clone());
                                *token = Token::Pending;
                            }
                            TokenAction::Keep => {
                                log::debug!("[Operator: {}] Keeping < {} >.", self.id, port_id);
                                data.insert(port_id.clone(), ready_token.data.clone());
                            }
                            TokenAction::Drop => {
                                log::debug!("[Operator: {}] Dropping < {} >.", self.id, port_id);
                                data.remove(port_id);
                                *token = Token::Pending;
                            }
                        }
                    }
                }
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
            let start = Instant::now();
            let run_outputs = self.operator.run(&mut context, &mut state, &mut data)?;
            let elapsed = start.elapsed();

            log::debug!(
                "[Operator: {}] `run` executed in {} ms",
                self.id,
                elapsed.as_micros()
            );

            let mut deadline_miss = None;

            if let Some(deadline) = self.deadline {
                if elapsed > deadline {
                    log::warn!(
                        "[Operator: {}] Deadline miss detected for `run`: {} ms (expected < {} ms)",
                        self.id,
                        elapsed.as_micros(),
                        deadline.as_micros()
                    );
                    deadline_miss = Some(DeadlineMiss {
                        start,
                        deadline,
                        elapsed,
                    });
                }
            }

            // Output rules
            let mut outputs =
                self.operator
                    .output_rule(&mut context, &mut state, run_outputs, deadline_miss)?;

            // Send to Links
            for port_id in self.outputs.keys() {
                let output = match outputs.remove(port_id) {
                    Some(output) => output,
                    None => continue,
                };

                if let Some(link_senders) = io.outputs.get(port_id) {
                    let zf_message = Arc::new(Message::from_node_output(output, timestamp));
                    for link_sender in link_senders {
                        let res = link_sender.send(zf_message.clone()).await;

                        // TODO: Maybe we want to process somehow the error, not simply log it.
                        if let Err(e) = res {
                            log::error!(
                                "[Operator: {}] Could not send output < {} > on link < {} >: {:?}",
                                self.id,
                                port_id,
                                link_sender.id,
                                e
                            );
                        }
                    }
                }
            }
        }
    }
}
