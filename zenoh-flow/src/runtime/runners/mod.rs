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

pub mod connector;
pub mod operator;
pub mod sink;
pub mod source;

use crate::runtime::graph::link::{ZFLinkReceiver, ZFLinkSender};
use crate::runtime::message::ZFMessage;
use crate::runtime::runners::connector::{ZFZenohReceiver, ZFZenohSender};
use crate::runtime::runners::operator::ZFOperatorRunner;
use crate::runtime::runners::sink::ZFSinkRunner;
use crate::runtime::runners::source::ZFSourceRunner;
use crate::types::ZFResult;
use crate::ZFError;

use crate::async_std::prelude::*;
use crate::async_std::{
    channel::{bounded, Receiver, Sender},
    sync::{Arc, Mutex},
    task::JoinHandle,
};

#[derive(Clone)]
pub struct Runner(Arc<Mutex<RunnerInner>>);

impl Runner {
    pub fn new(inner: RunnerInner) -> Self {
        Self(Arc::new(Mutex::new(inner)))
    }

    pub async fn run(&self) -> ZFResult<()> {
        self.0.lock().await.run().await
    }

    pub async fn run_stoppable(&self, stop: Receiver<()>) -> ZFResult<()> {
        let run = async { self.0.lock().await.run().await };
        let stopper = async {
            stop.recv()
                .await
                .map_err(|e| ZFError::IOError(format!("{:?}", e)))
        };

        match run.race(stopper).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub fn start(&self) -> (Sender<()>, JoinHandle<ZFResult<()>>) {
        let (s, r) = bounded::<()>(1);
        let inner = self.clone();

        let h = async_std::task::spawn(async move { inner.run_stoppable(r).await });
        (s, h)
    }

    pub async fn add_input(&self, input: ZFLinkReceiver<ZFMessage>) {
        self.0.lock().await.add_input(input)
    }

    pub async fn add_output(&self, output: ZFLinkSender<ZFMessage>) {
        self.0.lock().await.add_output(output)
    }
}

pub enum RunnerInner {
    Operator(ZFOperatorRunner),
    Source(ZFSourceRunner),
    Sink(ZFSinkRunner),
    Sender(ZFZenohSender),
    Receiver(ZFZenohReceiver),
}

impl RunnerInner {
    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            RunnerInner::Operator(runner) => runner.run().await,
            RunnerInner::Source(runner) => runner.run().await,
            RunnerInner::Sink(runner) => runner.run().await,
            RunnerInner::Sender(runner) => runner.run().await,
            RunnerInner::Receiver(runner) => runner.run().await,
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        log::trace!("add_input({:?})", input);
        match self {
            RunnerInner::Operator(runner) => runner.add_input(input),
            RunnerInner::Source(_) => panic!("Sources does not have inputs!"), // TODO this should return a ZFResult<()>
            RunnerInner::Sink(runner) => runner.add_input(input),
            RunnerInner::Sender(runner) => runner.add_input(input),
            RunnerInner::Receiver(_) => panic!("Receiver does not have inputs!"), // TODO this should return a ZFResult<()>
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        log::trace!("add_output({:?})", output);
        match self {
            RunnerInner::Operator(runner) => runner.add_output(output),
            RunnerInner::Source(runner) => runner.add_output(output),
            RunnerInner::Sink(_) => panic!("Sinks does not have output!"), // TODO this should return a ZFResult<()>
            RunnerInner::Sender(_) => panic!("Senders does not have output!"), // TODO this should return a ZFResult<()>
            RunnerInner::Receiver(runner) => runner.add_output(output),
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
