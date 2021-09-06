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
use crate::model::operator::ZFSinkRecord;
use crate::runtime::graph::link::ZFLinkReceiver;
use crate::runtime::message::ZFMessage;
use crate::types::{Token, ZFResult};
use crate::{ZFContext, ZFSinkTrait, ZFStateTrait};
use futures::future;
use libloading::Library;
use std::collections::HashMap;

pub type ZFSinkRegisterFn = fn() -> ZFResult<Arc<dyn ZFSinkTrait>>;

pub struct ZFSinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: ZFSinkRegisterFn,
}

pub struct ZFSinkRunnerInner {
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub state: Box<dyn ZFStateTrait>,
}

impl ZFSinkRunnerInner {
    pub fn new(state: Box<dyn ZFStateTrait>) -> Self {
        Self {
            inputs: vec![],
            state,
        }
    }
}

#[derive(Clone)]
pub struct ZFSinkRunner {
    pub record: Arc<ZFSinkRecord>,
    pub sink: Arc<dyn ZFSinkTrait>,
    pub lib: Arc<Option<Library>>,
    pub inner: Arc<RwLock<ZFSinkRunnerInner>>,
}

impl ZFSinkRunner {
    pub fn new(record: ZFSinkRecord, sink: Arc<dyn ZFSinkTrait>, lib: Option<Library>) -> Self {
        Self {
            record: Arc::new(record),
            sink,
            lib: Arc::new(lib),
            inner: Arc::new(RwLock::new(ZFSinkRunnerInner::new(Box::new(
                crate::EmptyState {},
            )))), //place holder
        }
    }

    pub async fn add_input(&self, input: ZFLinkReceiver<ZFMessage>) {
        self.inner.write().await.inputs.push(input);
    }

    pub fn clean(&self) -> ZFResult<()> {
        // self.sink.clean(&mut self.state)
        Ok(())
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = ZFContext::default();
        let mut state = self.sink.initialize(&self.record.configuration);

        loop {
            // we should start from an HashMap with all PortId and not ready tokens
            let mut msgs: HashMap<String, Token> = HashMap::new();

            let guard = self.inner.read().await;

            for i in guard.inputs.iter() {
                msgs.insert(i.id(), Token::NotReady);
            }

            // let ir_fn = self.operator.get_input_rule(context.clone());

            let mut futs = vec![];
            for rx in guard.inputs.iter() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            crate::run_input_rules!(self.sink, msgs, futs, &mut state, &mut context);

            // Running
            let mut data = HashMap::with_capacity(msgs.len());

            for (id, v) in msgs {
                log::debug!("[SINK] Sending data to run: {:?}", v);
                let (d, _) = v.split();
                if d.is_none() {
                    continue;
                }
                data.insert(id, d.unwrap());
            }

            self.sink.run(&mut context, &mut state, &mut data).await?;

            //This depends on the Tokens...
            for rx in guard.inputs.iter() {
                rx.discard().await?;
            }

            drop(guard);
        }
    }
}
