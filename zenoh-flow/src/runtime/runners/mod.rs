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
    channel::{bounded, Receiver, RecvError, Sender},
    sync::{Arc, Mutex},
    task::JoinHandle,
};

use futures_lite::future::FutureExt;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub enum RunnerKind {
    Source,
    Operator,
    Sink,
    Connector,
}

pub struct RunnerManager {
    stopper: Sender<()>,
    handler: JoinHandle<ZFResult<()>>,
    kind: RunnerKind,
}

impl RunnerManager {
    pub fn new(stopper: Sender<()>, handler: JoinHandle<ZFResult<()>>, kind: RunnerKind) -> Self {
        Self {
            stopper,
            handler,
            kind,
        }
    }

    pub async fn kill(&self) -> ZFResult<()> {
        Ok(self.stopper.send(()).await?)
    }

    pub fn get_handler(&self) -> &JoinHandle<ZFResult<()>> {
        &self.handler
    }

    pub fn get_kind(&self) -> &RunnerKind {
        &self.kind
    }
}

impl Future for RunnerManager {
    type Output = ZFResult<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.handler.poll(ctx)
    }
}

pub enum RunAction {
    RestartRun(Option<ZFError>),
    Stop,
    StopError(RecvError),
}

#[derive(Clone)]
pub struct Runner {
    inner: Arc<Mutex<RunnerInner>>,
    kind: RunnerKind,
}

impl Runner {
    pub fn new(inner: RunnerInner) -> Self {
        let kind = match &inner {
            RunnerInner::Operator(_) => RunnerKind::Operator,
            RunnerInner::Source(_) => RunnerKind::Source,
            RunnerInner::Sink(_) => RunnerKind::Sink,
            RunnerInner::Sender(_) | RunnerInner::Receiver(_) => RunnerKind::Connector,
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
            kind,
        }
    }

    pub async fn run(&self) -> ZFResult<()> {
        self.inner.lock().await.run().await
    }

    pub async fn run_stoppable(&self, stop: Receiver<()>) -> ZFResult<()> {
        loop {
            let run = async {
                match self.inner.lock().await.run().await {
                    Ok(_) => RunAction::RestartRun(None),
                    Err(e) => RunAction::RestartRun(Some(e)),
                }
            };
            let stopper = async {
                match stop.recv().await {
                    Ok(_) => RunAction::Stop,
                    Err(e) => RunAction::StopError(e),
                }
            };

            match run.race(stopper).await {
                RunAction::RestartRun(e) => {
                    log::error!("The run loop exited with {:?}, restarting...", e);
                    continue;
                }
                RunAction::Stop => {
                    log::trace!("Received kill command, killing runner");
                    break Ok(());
                }
                RunAction::StopError(e) => {
                    log::error!("The stopper recv got an error: {}, exiting...", e);
                    break Err(e.into());
                }
            }
        }
    }

    pub fn start(&self) -> RunnerManager {
        let (s, r) = bounded::<()>(1);
        let _self = self.clone();

        let h = async_std::task::spawn(async move { _self.run_stoppable(r).await });
        RunnerManager::new(s, h, self.kind.clone())
    }

    pub async fn add_input(&self, input: ZFLinkReceiver<ZFMessage>) {
        self.inner.lock().await.add_input(input)
    }

    pub async fn add_output(&self, output: ZFLinkSender<ZFMessage>) {
        self.inner.lock().await.add_output(output)
    }

    pub fn get_kind(&self) -> &RunnerKind {
        &self.kind
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
    ($component: expr, $tokens : expr, $links : expr, $state: expr, $context: expr) => {
        while !$links.is_empty() {
            match future::select_all($links).await {
                // this could be "slow" as suggested by LC
                (Ok((id, message)), _i, remaining) => {
                    match message.as_ref() {
                        ZFMessage::Data(_) => {
                            $tokens.insert(id, Token::from(message));

                            match $component.input_rule($context, $state, &mut $tokens) {
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
