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

use crate::runtime::connectors::{ZFZenohReceiver, ZFZenohSender};
use crate::runtime::graph::link::{ZFLinkReceiver, ZFLinkSender};
use crate::runtime::message::ZFMessage;
use crate::types::{Token, ZFContext, ZFInput, ZFResult};
use crate::utils::hlc::PeriodicHLC;
use crate::{OperatorTrait, SinkTrait, SourceTrait};
use async_std::sync::Arc;
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use uhlc::HLC;
use zenoh::net::Session;

pub enum Runner {
    Operator(ZFOperatorRunner),
    Source(ZFSourceRunner),
    Sink(ZFSinkRunner),
    Sender(ZFZenohSender),
    Receiver(ZFZenohReceiver),
}

impl Runner {
    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            Runner::Operator(runner) => runner.run().await,
            Runner::Source(runner) => runner.run().await,
            Runner::Sink(runner) => runner.run().await,
            Runner::Sender(runner) => runner.run().await,
            Runner::Receiver(runner) => runner.run().await,
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        log::trace!("add_input({:?})", input);
        match self {
            Runner::Operator(runner) => runner.add_input(input),
            Runner::Source(_) => panic!("Sources does not have inputs!"), // TODO this should return a ZFResult<()>
            Runner::Sink(runner) => runner.add_input(input),
            Runner::Sender(runner) => runner.add_input(input),
            Runner::Receiver(_) => panic!("Receiver does not have inputs!"), // TODO this should return a ZFResult<()>
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        log::trace!("add_output({:?})", output);
        match self {
            Runner::Operator(runner) => runner.add_output(output),
            Runner::Source(runner) => runner.add_output(output),
            Runner::Sink(_) => panic!("Sinks does not have output!"), // TODO this should return a ZFResult<()>
            Runner::Sender(_) => panic!("Senders does not have output!"), // TODO this should return a ZFResult<()>
            Runner::Receiver(runner) => runner.add_output(output),
        }
    }
}

#[repr(C)]
pub struct ZFOperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        Option<HashMap<String, String>>,
    ) -> ZFResult<Box<dyn OperatorTrait + Send>>,
}

pub struct ZFOperatorRunner {
    pub hlc: Arc<HLC>,
    pub operator: Box<dyn OperatorTrait + Send>,
    pub lib: Option<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub outputs: HashMap<String, Vec<ZFLinkSender<ZFMessage>>>,
}

impl ZFOperatorRunner {
    pub fn new(
        hlc: Arc<HLC>,
        operator: Box<dyn OperatorTrait + Send>,
        lib: Option<Library>,
    ) -> Self {
        Self {
            hlc,
            operator,
            lib,
            inputs: vec![],
            outputs: HashMap::new(),
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        let key = output.id();
        if let Some(links) = self.outputs.get_mut(&key) {
            links.push(output);
        } else {
            self.outputs.insert(key, vec![output]);
        }
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all PortId and not ready tokens
            let mut msgs: HashMap<String, Token> = HashMap::new();

            for i in &self.inputs {
                msgs.insert(i.id(), Token::NotReady);
            }

            let ir_fn = self.operator.get_input_rule(ctx.clone());

            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input Rules
            crate::run_input_rules!(ir_fn, msgs, futs, ctx);
            let mut data = ZFInput::new();
            let mut max_token_timestamp = None;

            for (id, token) in msgs {
                // Keep the biggest timestamp (i.e. most recent) associated to the inputs. This
                // timestamp will be reported to all outputs and used to update the HLC.
                let token_timestamp = token.get_timestamp();
                max_token_timestamp = match (&max_token_timestamp, &token_timestamp) {
                    (None, _) => token_timestamp,
                    (Some(_), None) => max_token_timestamp,
                    (Some(max_time), Some(time)) => {
                        if max_time < time {
                            token_timestamp
                        } else {
                            max_token_timestamp
                        }
                    }
                };

                let (d, _) = token.split();
                data.insert(id, d.unwrap());
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
            let run_fn = self.operator.get_run(ctx.clone());
            let run_outputs = run_fn(ctx.clone(), data)?;

            // Output rules
            let out_fn = self.operator.get_output_rule(ctx.clone());
            let outputs = out_fn(ctx.clone(), run_outputs)?;

            // Send to Links
            for (id, output) in outputs {
                // getting link
                log::debug!("id: {:?}, message: {:?}", id, output);
                if let Some(links) = self.outputs.get(&id) {
                    let zf_message =
                        Arc::new(ZFMessage::from_component_output(output, timestamp.clone()));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }

            // This depends on the Tokens...
            for rx in self.inputs.iter_mut() {
                rx.drop()?;
            }
        }
    }
}

pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        Option<HashMap<String, String>>,
    ) -> ZFResult<Box<dyn SourceTrait + Send>>,
}

pub struct ZFZenohReceiverDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        Arc<Session>,
        Option<HashMap<String, String>>,
    ) -> ZFResult<Box<dyn SourceTrait + Send>>,
}

// TODO to be removed
pub trait ZFSourceRegistrarTrait {
    fn register_zfsource(&mut self, name: &str, operator: Box<dyn SourceTrait + Send>);
}

pub struct ZFSourceRunner {
    pub hlc: PeriodicHLC,
    pub operator: Box<dyn SourceTrait + Send>,
    pub lib: Option<Library>,
    pub outputs: HashMap<String, Vec<ZFLinkSender<ZFMessage>>>,
}

impl ZFSourceRunner {
    pub fn new(
        hlc: PeriodicHLC,
        operator: Box<dyn SourceTrait + Send>,
        lib: Option<Library>,
    ) -> Self {
        Self {
            hlc,
            operator,
            lib,
            outputs: HashMap::new(),
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        let key = output.id();
        if let Some(links) = self.outputs.get_mut(&key) {
            links.push(output);
        } else {
            self.outputs.insert(key, vec![output]);
        }
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let run_outputs = run_fn(ctx.clone()).await?;

            // Output
            let out_fn = self.operator.get_output_rule(ctx.clone());

            let mut outputs = out_fn(ctx.clone(), run_outputs)?;
            log::debug!("Outputs: {:?}", self.outputs);

            let timestamp = self.hlc.new_timestamp();

            // Send to Links
            for (id, output) in outputs.drain() {
                log::debug!("Sending on {:?} data: {:?}", id, output);

                if let Some(links) = self.outputs.get(&id) {
                    let zf_message =
                        Arc::new(ZFMessage::from_component_output(output, timestamp.clone()));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }
        }
    }
}

pub struct ZFZenohSenderDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        Arc<Session>,
        Option<HashMap<String, String>>,
    ) -> ZFResult<Box<dyn SinkTrait + Send>>,
}

pub struct ZFSinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        Option<HashMap<String, String>>,
    ) -> ZFResult<Box<dyn SinkTrait + Send>>,
}

pub trait ZFSinkRegistrarTrait {
    fn register_zfsink(&mut self, name: &str, operator: Box<dyn SinkTrait + Send>);
}

pub struct ZFSinkRunner {
    pub operator: Box<dyn SinkTrait + Send>,
    pub lib: Option<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
}

impl ZFSinkRunner {
    pub fn new(operator: Box<dyn SinkTrait + Send>, lib: Option<Library>) -> Self {
        Self {
            operator,
            lib,
            inputs: vec![],
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all PortId and not ready tokens
            let mut msgs: HashMap<String, Token> = HashMap::new();

            for i in self.inputs.iter() {
                msgs.insert(i.id(), Token::NotReady);
            }

            let ir_fn = self.operator.get_input_rule(ctx.clone());

            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            crate::run_input_rules!(ir_fn, msgs, futs, ctx);

            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let mut data = ZFInput::new();

            for (id, v) in msgs {
                log::debug!("[SINK] Sending data to run: {:?}", v);
                let (d, _) = v.split();
                if d.is_none() {
                    continue;
                }
                data.insert(id, d.unwrap());
            }

            run_fn(ctx.clone(), data).await?;

            //This depends on the Tokens...
            for rx in self.inputs.iter_mut() {
                rx.drop()?;
            }
        }
    }
}

#[macro_export]
macro_rules! run_input_rules {
    ($ir : expr, $tokens : expr, $links : expr, $ctx: expr) => {
        while !$links.is_empty() {
            match future::select_all($links).await {
                // this could be "slow" as suggested by LC
                (Ok((id, message)), _i, remaining) => {
                    match message.as_ref() {
                        ZFMessage::Data(_) => {
                            $tokens.insert(id, Token::from(message));

                            match $ir($ctx.clone(), &mut $tokens) {
                                Ok(true) => {
                                    // we can run
                                    log::debug!("IR: OK");
                                    $links = vec![]; // this makes the while loop to end
                                }
                                Ok(false) => {
                                    //we cannot run, we should update the list of futures
                                    log::debug!("IR: Not OK");
                                    $links = remaining;
                                }
                                Err(_) => {
                                    // we got an error on the input rules, we should recover/update list of futures
                                    log::debug!("IR: received an error");
                                    $links = remaining;
                                }
                            }
                        }
                        ZFMessage::Control(_) => {
                            //control message receiver, we should handle it
                            $links = remaining;
                        }
                    };
                }
                (Err(e), i, remaining) => {
                    log::debug!("Link index {:?} has got error {:?}", i, e);
                    $links = remaining;
                }
            }
        };
        drop($links);
    };
}
