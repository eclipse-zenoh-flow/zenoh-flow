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

use crate::async_std::sync::{Arc, Mutex};
use crate::model::deadline::E2EDeadlineRecord;
use crate::model::loops::LoopDescriptor;
use crate::model::node::OperatorRecord;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::OperatorLoaded;
use crate::runtime::deadline::E2EDeadline;
use crate::runtime::loops::LoopContext;
use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::{
    Context, DataMessage, InputToken, LocalDeadlineMiss, NodeId, Operator, PortId, PortType, State,
    TokenAction, ZFError, ZFResult,
};
use async_trait::async_trait;
use futures::{future, Future};
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// A struct that wraps the inputs and outputs
/// of a node.
#[derive(Default)]
pub struct OperatorIO {
    inputs: HashMap<PortId, LinkReceiver>,
    outputs: HashMap<PortId, Vec<LinkSender>>,
}

/// Future of the `Receiver`
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

/// Type of Inputs
pub type InputsLink = HashMap<PortId, LinkReceiver>;
/// Type of Outputs
pub type OutputsLinks = HashMap<PortId, Vec<LinkSender>>;

impl OperatorIO {
    /// Creates the `OperatorIO` from an [`OperatorRecord`](`OperatorRecord`)
    pub fn new(record: &OperatorRecord) -> Self {
        Self {
            inputs: HashMap::with_capacity(record.inputs.len()),
            outputs: HashMap::with_capacity(record.outputs.len()),
        }
    }

    /// Tries to add the given `LinkReceiver`
    ///
    /// # Errors
    /// It fails if the `PortId` is duplicated.
    pub fn try_add_input(&mut self, rx: LinkReceiver) -> ZFResult<()> {
        if self.inputs.contains_key(&rx.id()) {
            return Err(ZFError::DuplicatedPort((rx.id(), rx.id())));
        }

        self.inputs.insert(rx.id(), rx);

        Ok(())
    }

    /// Adds the given `LinkSender`
    pub fn add_output(&mut self, tx: LinkSender) {
        if let Some(vec_senders) = self.outputs.get_mut(&tx.id()) {
            vec_senders.push(tx);
        } else {
            self.outputs.insert(tx.id(), vec![tx]);
        }
    }

    /// Destroys and returns a tuple with the internal inputs and outputs.
    pub fn take(self) -> (InputsLink, OutputsLinks) {
        (self.inputs, self.outputs)
    }

    /// Returns a copy of the inputs.
    pub fn get_inputs(&self) -> InputsLink {
        self.inputs.clone()
    }

    /// Returns a copy of the outputs.
    pub fn get_outputs(&self) -> OutputsLinks {
        self.outputs.clone()
    }
}

/// The `OperatorRunner` is the component in charge of executing the operator.
/// It contains all the runtime information for the operator, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the operator/lib, otherwise we
/// will have a SIGSEV.
#[derive(Clone)]
pub struct OperatorRunner {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) io: Arc<Mutex<OperatorIO>>,
    pub(crate) inputs: HashMap<PortId, PortType>,
    pub(crate) outputs: HashMap<PortId, PortType>,
    pub(crate) local_deadline: Option<Duration>,
    pub(crate) end_to_end_deadlines: Vec<E2EDeadlineRecord>,
    pub(crate) is_running: Arc<Mutex<bool>>,
    // Ciclo is the italian word for "loop" — we cannot use "loop" as it’s a reserved keyword.
    pub(crate) ciclo: Option<LoopDescriptor>,
    pub(crate) state: Arc<Mutex<State>>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) _library: Option<Arc<Library>>,
}

impl OperatorRunner {
    /// Tries to create a new `OperatorRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`OperatorLoaded`](`OperatorLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the output is not connected.
    pub fn try_new(
        context: InstanceContext,
        operator: OperatorLoaded,
        operator_io: OperatorIO,
    ) -> ZFResult<Self> {
        // TODO Check that all ports are used.
        Ok(Self {
            id: operator.id,
            context,
            io: Arc::new(Mutex::new(operator_io)),
            inputs: operator.inputs,
            outputs: operator.outputs,
            state: operator.state,
            is_running: Arc::new(Mutex::new(false)),
            operator: operator.operator,
            _library: operator.library,
            local_deadline: operator.local_deadline,
            end_to_end_deadlines: operator.end_to_end_deadlines,
            ciclo: operator.ciclo,
        })
    }

    /// Starts the operator.
    async fn start(&self) {
        *self.is_running.lock().await = true;
    }

    /// A single iteration of the run loop.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// -  user returns an error
    /// - link recv fails
    /// - link send fails
    ///
    async fn iteration(
        &self,
        mut context: Context,
        mut tokens: HashMap<PortId, InputToken>,
        mut data: HashMap<PortId, DataMessage>,
    ) -> ZFResult<(
        Context,
        HashMap<PortId, InputToken>,
        HashMap<PortId, DataMessage>,
    )> {
        // Guards are taken at the beginning of each iteration to allow interleaving.
        let io = self.io.lock().await;
        let mut state = self.state.lock().await;

        let mut links = Vec::with_capacity(tokens.len());

        // Only call `recv` on links where the corresponding Token is `Pending`. If a
        // `ReadyToken` has its action set to `Keep` then it will stay as a `ReadyToken` (i.e.
        // it won’t be resetted later on) and we should not poll data.
        for (port_id, token) in tokens.iter() {
            if let InputToken::Pending = token {
                links.push(io.poll_input(&self.id, port_id)?);
            }
        }

        'input_rule: loop {
            if !links.is_empty() {
                match future::select_all(links).await {
                    (Ok((port_id, message)), _index, remaining) => {
                        match message.as_ref() {
                            Message::Data(data_message) => {
                                // In order to check for E2EDeadlines we first have to update
                                // the HLC. There is indeed a possibility that the timestamp
                                // associated with the data is "ahead" of the hlc on this
                                // runtime — which would cause problems when computing the time
                                // difference.
                                if let Err(error) = self
                                    .context
                                    .runtime
                                    .hlc
                                    .update_with_timestamp(&data_message.timestamp)
                                {
                                    log::error!(
                                        "[Operator: {}][HLC] Could not update HLC with \
                                             timestamp {:?}: {:?}",
                                        self.id,
                                        data_message.timestamp,
                                        error
                                    );
                                }

                                // We clone the `data_message` because the end to end deadlines
                                // are specific to each Operator. Suppose we have the following
                                // dataflow, running on **the same daemon**:
                                //
                                //              ┌───┐
                                //         ┌───►│ 2 │
                                //         │    └───┘
                                //       ┌─┴─┐
                                //       │ 1 │
                                //       └─┬─┘
                                //         │    ┌───┐
                                //         └───►│ 3 │
                                //              └───┘
                                //
                                // Operators 2 and 3 receive the same output from Operator 1.
                                //
                                // They will thus both receive an `Arc`. I.e. they both share a
                                // pointer on the **same** message. Operator 2 cannot add
                                // information about an end-to-end deadline that applies only on
                                // itself as Operator 3 would have access to it — which, in the
                                // end, would be very incorrect (and confusing).
                                let mut data_msg = data_message.clone();

                                let now = self.context.runtime.hlc.new_timestamp();
                                data_message
                                    .end_to_end_deadlines
                                    .iter()
                                    .for_each(|deadline| {
                                        if let Some(miss) = deadline.check(&self.id, &port_id, &now)
                                        {
                                            data_msg.missed_end_to_end_deadlines.push(miss)
                                        }
                                    });

                                tokens.insert(port_id, InputToken::from(data_msg));
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
                    log::trace!("[Operator: {}] Input Rule returned < true >.", self.id);
                    break 'input_rule;
                }
                Ok(false) => {
                    log::trace!("[Operator: {}] Input Rule returned < false >.", self.id);
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
                    *token = InputToken::Pending;
                    links.push(io.poll_input(&self.id, port_id)?);
                }
            }
        } // end < 'input_rule: loop >

        let mut earliest_source_timestamp = None;
        let mut e2e_deadlines_to_propagate: Vec<E2EDeadline> = vec![];
        let mut loop_feedback = false;
        let mut loop_contexts_to_propagate: Vec<LoopContext> = vec![];

        for (port_id, token) in tokens.iter_mut() {
            match token {
                InputToken::Pending => {
                    log::trace!(
                        "[Operator: {}] Removing < {} > from Data transmitted to `run`.",
                        self.id,
                        port_id
                    );
                    data.remove(port_id);
                    continue;
                }
                InputToken::Ready(data_token) => {
                    earliest_source_timestamp = match earliest_source_timestamp {
                        None => Some(data_token.data.timestamp),
                        Some(timestamp) => {
                            if data_token.data.timestamp > timestamp {
                                Some(data_token.data.timestamp)
                            } else {
                                Some(timestamp)
                            }
                        }
                    };

                    // TODO: Refactor this code to:
                    // 1) Avoid considering the source_timestamp of a token that is dropped.
                    match data_token.action {
                        TokenAction::Consume | TokenAction::Keep => {
                            log::trace!("[Operator: {}] Consuming < {} >.", self.id, port_id);
                            e2e_deadlines_to_propagate.extend(
                                data_token
                                    .data
                                    .end_to_end_deadlines
                                    .iter()
                                    .filter(|e2e_deadline| e2e_deadline.to.node != self.id)
                                    .cloned(),
                            );

                            loop_contexts_to_propagate
                                .extend(data_token.data.loop_contexts.iter().cloned());

                            if let Some(ciclo) = &self.ciclo {
                                if ciclo.ingress == self.id && *port_id == ciclo.feedback_port {
                                    loop_feedback = true;
                                }
                            }

                            data.insert(port_id.clone(), data_token.data.clone());
                            if data_token.action == TokenAction::Consume {
                                *token = InputToken::Pending;
                            }
                        }
                        TokenAction::Drop => {
                            log::trace!("[Operator: {}] Dropping < {} >.", self.id, port_id);
                            data.remove(port_id);
                            *token = InputToken::Pending;
                        }
                    }
                }
            }
        }

        // Loop management: Ingress.
        //
        // Two, mutually exclusive, cases can happen:
        // 1. No message coming from the feedback link was processed. This means a new Loop is
        //    starting and we need to associate a context to this message.
        // 2. A message from the feedback link was processed: a LoopContext is already
        //    associated. We need to update it.
        if let Some(ciclo) = &self.ciclo {
            if ciclo.ingress == self.id {
                let now = self.context.runtime.hlc.new_timestamp();
                if !loop_feedback {
                    log::trace!("[Operator: {}] Ingress, new loop detected", self.id);
                    loop_contexts_to_propagate.push(LoopContext::new(ciclo, now));
                } else {
                    log::trace!("[Operator: {}] Ingress, updating LoopContext", self.id);
                    let loop_ctx = loop_contexts_to_propagate
                        .iter_mut()
                        .find(|loop_ctx| loop_ctx.ingress == self.id);
                    if let Some(ctx) = loop_ctx {
                        ctx.update_ingress(now);
                    }
                }
            }
        }

        let timestamp = {
            match earliest_source_timestamp {
                Some(max_timestamp) => max_timestamp,
                None => self.context.runtime.hlc.new_timestamp(),
            }
        };

        // Running
        let start = Instant::now();
        let run_outputs = self.operator.run(&mut context, &mut state, &mut data)?;
        let elapsed = start.elapsed();

        log::trace!(
            "[Operator: {}] `run` executed in {} ms",
            self.id,
            elapsed.as_micros()
        );

        let mut deadline_miss = None;

        if let Some(deadline) = self.local_deadline {
            if elapsed > deadline {
                log::warn!(
                    "[Operator: {}] Deadline miss detected for `run`: {} ms (expected < {} ms)",
                    self.id,
                    elapsed.as_micros(),
                    deadline.as_micros()
                );
                deadline_miss = Some(LocalDeadlineMiss {
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

        // E2EDeadlines management: add deadlines that start at that operator.
        let now = self.context.runtime.hlc.new_timestamp();
        e2e_deadlines_to_propagate.extend(
            self.end_to_end_deadlines
                .iter()
                .filter(|e2e_deadline| e2e_deadline.from.node == self.id)
                .map(|e2e_deadline| E2EDeadline::new(e2e_deadline.clone(), now)),
        );

        // Send to Links
        for port_id in self.outputs.keys() {
            let output = match outputs.remove(port_id) {
                Some(output) => output,
                None => continue,
            };

            log::trace!("Sending on port < {} >…", port_id);

            if let Some(link_senders) = io.outputs.get(port_id) {
                // Loop management: the node is an Egress, depending on which output the message
                // is sent, we need to either update the LoopContext or remove it.
                //
                // CAVEAT: both options are possible at the same time! A message can be sent on
                // the feedback link and on another output. We thus need to clone.
                let mut loop_contexts = loop_contexts_to_propagate.clone();

                if let Some(ciclo) = &self.ciclo {
                    if ciclo.egress == self.id {
                        if *port_id != ciclo.feedback_port {
                            // Output is not sent on the feedback link, remove the LoopContext.
                            loop_contexts = loop_contexts
                                .into_iter()
                                .filter(|loop_ctx| loop_ctx.egress != self.id)
                                .collect::<Vec<LoopContext>>();
                        } else {
                            // Output is sent on the feedback link, we updade the context before
                            // sending it.
                            let loop_context = loop_contexts
                                .iter_mut()
                                .find(|loop_ctx| loop_ctx.egress == self.id);
                            if let Some(loop_ctx) = loop_context {
                                loop_ctx.update_egress(now);
                            }
                        }
                    }
                }

                let zf_message = Arc::new(Message::from_node_output(
                    output,
                    timestamp,
                    e2e_deadlines_to_propagate.clone(),
                    loop_contexts,
                ));

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
        Ok((context, tokens, data))
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

    async fn add_input(&self, input: LinkReceiver) -> ZFResult<()> {
        let mut guard = self.io.lock().await;
        let key = input.id();
        guard.inputs.insert(key, input);
        Ok(())
    }

    async fn add_output(&self, output: LinkSender) -> ZFResult<()> {
        let mut guard = self.io.lock().await;
        let key = output.id();
        if let Some(links) = guard.outputs.get_mut(key.as_ref()) {
            links.push(output);
        } else {
            guard.outputs.insert(key, vec![output]);
        }
        Ok(())
    }

    fn get_inputs(&self) -> HashMap<PortId, PortType> {
        self.inputs.clone()
    }

    fn get_outputs(&self) -> HashMap<PortId, PortType> {
        self.outputs.clone()
    }

    async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
        self.io.lock().await.get_outputs()
    }

    async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
        let inputs = HashMap::new();
        let mut io_guard = self.io.lock().await;
        let current_inputs = io_guard.get_inputs();
        io_guard.inputs = inputs;
        current_inputs
    }

    async fn start_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unsupported)
    }

    async fn stop_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unsupported)
    }

    async fn is_recording(&self) -> bool {
        false
    }

    async fn stop(&self) {
        *self.is_running.lock().await = false;
    }

    async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }

    async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.lock().await;
        self.operator.finalize(&mut state)
    }

    async fn run(&self) -> ZFResult<()> {
        self.start().await;

        let mut context = Context::default();
        let mut tokens: HashMap<PortId, InputToken> = self
            .inputs
            .keys()
            .map(|input_id| (input_id.clone(), InputToken::Pending))
            .collect();
        let mut data: HashMap<PortId, DataMessage> = HashMap::with_capacity(tokens.len());

        // Looping on iteration, each iteration is a single
        // run of the source, as a run can fail in case of error it
        // stops and returns the error to the caller (the RunnerManager)
        loop {
            match self.iteration(context, tokens, data).await {
                Ok((ctx, tkn, d)) => {
                    log::trace!(
                        "[Operator: {}] iteration ok with new context {:?}",
                        self.id,
                        ctx
                    );
                    context = ctx;
                    tokens = tkn;
                    data = d;
                    // As async_std scheduler is run to completion,
                    // if the iteration is always ready there is a possibility
                    // that other tasks are not scheduled (e.g. the stopping
                    // task), therefore after the iteration we give back
                    // the control to the scheduler, if no other tasks are
                    // ready, then this one is scheduled again.
                    async_std::task::yield_now().await;
                    continue;
                }
                Err(e) => {
                    log::error!("[Operator: {}] iteration failed with error: {}", self.id, e);
                    self.stop().await;
                    break Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "./tests/operator_test.rs"]
mod tests;

#[cfg(test)]
#[path = "./tests/operator_e2e_deadline_tests.rs"]
mod e2e_deadline_tests;
