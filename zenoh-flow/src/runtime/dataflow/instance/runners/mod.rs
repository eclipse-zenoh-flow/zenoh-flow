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

use crate::async_std::prelude::*;
use crate::async_std::sync::Arc;
use crate::async_std::{
    channel::{RecvError, Sender},
    task::JoinHandle,
};
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::types::{NodeId, ZFResult};
use crate::ZFError;
use async_trait::async_trait;
use futures_lite::future::FutureExt;
use std::ops::Deref;
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

#[async_trait]
pub trait Runner: Send + Sync {
    async fn run(&self) -> ZFResult<()>;
    async fn add_input(&self, input: LinkReceiver<Message>) -> ZFResult<()>;

    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()>;

    async fn clean(&self) -> ZFResult<()>;

    fn get_kind(&self) -> RunnerKind;

    fn get_id(&self) -> NodeId;
}

#[derive(Clone)]
pub struct NodeRunner {
    inner: Arc<dyn Runner>,
}

impl NodeRunner {
    pub fn new(inner: Arc<dyn Runner>) -> Self {
        Self { inner }
    }

    async fn run_stoppable(&self, stop: crate::async_std::channel::Receiver<()>) -> ZFResult<()> {
        loop {
            let run = async {
                match self.run().await {
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
                    log::error!(
                        "[Node: {}] The run loop exited with {:?}, restarting…",
                        self.get_id(),
                        e
                    );
                    continue;
                }
                RunAction::Stop => {
                    log::trace!(
                        "[Node: {}] Received kill command, killing runner",
                        self.get_id()
                    );
                    break Ok(());
                }
                RunAction::StopError(e) => {
                    log::error!(
                        "[Node {}] The stopper recv got an error: {}, exiting...",
                        self.get_id(),
                        e
                    );
                    break Err(e.into());
                }
            }
        }
    }
    pub fn start(&self) -> RunnerManager {
        let (s, r) = crate::async_std::channel::bounded::<()>(1);
        let cloned_self = self.clone();

        let h = async_std::task::spawn(async move { cloned_self.run_stoppable(r).await });
        RunnerManager::new(s, h, self.get_kind())
    }
}

impl Deref for NodeRunner {
    type Target = Arc<dyn Runner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[macro_export]
macro_rules! run_input_rules {
    ($node: expr, $tokens : expr, $links : expr, $state: expr, $context: expr) => {
        while !$links.is_empty() {
            match future::select_all($links).await {
                // this could be "slow" as suggested by LC
                (Ok((id, message)), _i, remaining) => {
                    match message.as_ref() {
                        Message::Data(_) => {
                            $tokens.insert(id, Token::from(message));

                            match $node.input_rule($context, $state, &mut $tokens) {
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
                        Message::Control(_) => {
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
