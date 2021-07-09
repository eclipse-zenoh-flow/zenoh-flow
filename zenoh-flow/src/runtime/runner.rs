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
use crate::runtime::connectors::{ZFZenohReceiver, ZFZenohSender};
use crate::runtime::graph::link::{ZFLinkReceiver, ZFLinkSender};
use crate::runtime::message::{Message, ZFMessage};
use crate::types::{Token, ZFContext, ZFData, ZFInput, ZFLinkId, ZFResult};
use crate::{OperatorTrait, SinkTrait, SourceTrait};
use futures::future;
use libloading::Library;
use std::collections::HashMap;
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
        match self {
            Runner::Operator(runner) => runner.add_input(input),
            Runner::Source(_) => panic!("Sources does not have inputs!"), // TODO this should return a ZFResult<()>
            Runner::Sink(runner) => runner.add_input(input),
            Runner::Sender(runner) => runner.add_input(input),
            Runner::Receiver(_) => panic!("Receiver does not have inputs!"), // TODO this should return a ZFResult<()>
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
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
    pub operator: Box<dyn OperatorTrait + Send>,
    pub lib: Option<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
}

impl ZFOperatorRunner {
    pub fn new(operator: Box<dyn OperatorTrait + Send>, lib: Option<Library>) -> Self {
        Self {
            operator,
            lib,
            inputs: vec![],
            outputs: vec![],
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.outputs.push(output);
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in &self.inputs {
                msgs.insert(i.id(), Token::new_not_ready(0));
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
                let (d, _) = v.split();
                data.insert(id, ZFData::from(d.unwrap()));
            }

            let outputs = run_fn(ctx.clone(), data)?;

            //Output
            let out_fn = self.operator.get_output_rule(ctx.clone());

            let out_msgs = out_fn(ctx.clone(), outputs)?;

            // Send to Links
            for (id, zf_msg) in out_msgs {
                //getting link
                log::debug!("id: {:?}, zf_msg: {:?}", id, zf_msg);
                let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
                log::debug!("Sending on: {:?}", tx);
                //println!("Tx: {:?} Receivers: {:?}", tx.inner.tx, tx.inner.tx.receiver_count());
                match zf_msg.msg {
                    Message::Data(_) => {
                        tx.send(zf_msg).await?;
                    }
                    Message::Ctrl(_) => {
                        // here we process should process control messages (eg. change mode)
                        //tx.send(zf_msg);
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
    pub operator: Box<dyn SourceTrait + Send>,
    pub lib: Option<Library>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
}

impl ZFSourceRunner {
    pub fn new(operator: Box<dyn SourceTrait + Send>, lib: Option<Library>) -> Self {
        Self {
            operator,
            lib,
            outputs: vec![],
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.outputs.push(output);
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let outputs = run_fn(ctx.clone()).await?;

            //Output
            let out_fn = self.operator.get_output_rule(ctx.clone());

            let out_msgs = out_fn(ctx.clone(), outputs)?;
            log::debug!("Outputs: {:?}", self.outputs);

            // Send to Links
            for (id, zf_msg) in out_msgs {
                log::debug!("Sending on {:?} data: {:?}", id, zf_msg);
                //getting link
                let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
                //println!("Tx: {:?} Receivers: {:?}", tx.inner.tx, tx.inner.tx.receiver_count());
                match zf_msg.msg {
                    Message::Data(_) => {
                        tx.send(zf_msg).await?;
                    }
                    Message::Ctrl(_) => {
                        // here we process should process control messages (eg. change mode)
                        //tx.send(zf_msg);
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
            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in self.inputs.iter() {
                msgs.insert(i.id(), Token::new_not_ready(0));
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
                data.insert(id, ZFData::from(d.unwrap()));
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
                (Ok((id, msg)), _i, remaining) => {
                    match &msg.msg {
                        Message::Data(_) => {
                            $tokens.insert(id, Token::from(msg));

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
                        Message::Ctrl(_) => {
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
