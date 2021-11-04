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
use crate::async_std::{
    channel::{RecvError, Sender},
    task::JoinHandle,
};
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::types::ZFResult;
use crate::ZFError;
use async_trait::async_trait;
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

#[async_trait]
pub trait Runner: Send + Sync {
    async fn run(&self) -> ZFResult<()>;
    async fn add_input(&self, input: LinkReceiver<Message>) -> ZFResult<()>;

    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()>;

    async fn clean(&self) -> ZFResult<()>;

    fn start(&self) -> RunnerManager;

    fn get_kind(&self) -> RunnerKind;
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
