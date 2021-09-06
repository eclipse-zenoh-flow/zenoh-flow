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
use crate::model::operator::ZFOperatorRecord;
use crate::runtime::graph::link::{ZFLinkReceiver, ZFLinkSender};
use crate::runtime::message::ZFMessage;
use crate::types::{Token, ZFResult};
use crate::{ZFContext, ZFOperatorTrait, ZFStateTrait};
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use uhlc::HLC;

pub type ZFOperatorRegisterFn = fn() -> ZFResult<Arc<dyn ZFOperatorTrait>>;

pub struct ZFOperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: ZFOperatorRegisterFn,
}

pub struct ZFOperatorRunnerInner {
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub outputs: HashMap<String, Vec<ZFLinkSender<ZFMessage>>>,
    pub state: Box<dyn ZFStateTrait>,
}

impl ZFOperatorRunnerInner {
    pub fn new(state: Box<dyn ZFStateTrait>) -> Self {
        Self {
            inputs: vec![],
            outputs: HashMap::new(),
            state,
        }
    }
}

#[derive(Clone)]
pub struct ZFOperatorRunner {
    pub record: Arc<ZFOperatorRecord>,
    pub hlc: Arc<HLC>,
    pub operator: Arc<dyn ZFOperatorTrait>,
    pub lib: Arc<Option<Library>>,
    pub inner: Arc<RwLock<ZFOperatorRunnerInner>>,
}

impl ZFOperatorRunner {
    pub fn new(
        record: ZFOperatorRecord,
        hlc: Arc<HLC>,
        operator: Arc<dyn ZFOperatorTrait>,
        lib: Option<Library>,
    ) -> Self {
        Self {
            record: Arc::new(record),
            hlc,
            operator,
            lib: Arc::new(lib),
            inner: Arc::new(RwLock::new(ZFOperatorRunnerInner::new(Box::new(
                crate::EmptyState {},
            )))), //place holder
        }
    }

    pub async fn add_input(&self, input: ZFLinkReceiver<ZFMessage>) {
        self.inner.write().await.inputs.push(input);
    }

    pub async fn add_output(&self, output: ZFLinkSender<ZFMessage>) {
        let mut guard = self.inner.write().await;
        let key = output.id();
        if let Some(links) = guard.outputs.get_mut(&key) {
            links.push(output);
        } else {
            guard.outputs.insert(key, vec![output]);
        }
        drop(guard);
    }

    pub fn clean(&self) -> ZFResult<()> {
        Ok(())
        // self.operator.clean(&mut self.state)
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = ZFContext::default();
        let mut state = self.operator.initialize(&self.record.configuration);

        loop {
            let guard = self.inner.read().await;

            // we should start from an HashMap with all PortId and not ready tokens
            let mut msgs: HashMap<String, Token> = HashMap::new();

            for i in &guard.inputs {
                msgs.insert(i.id(), Token::NotReady);
            }

            let mut futs = vec![];
            for rx in guard.inputs.iter() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input Rules
            crate::run_input_rules!(self.operator, msgs, futs, &mut state, &mut context);

            let mut data = HashMap::with_capacity(msgs.len());
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
            let run_outputs = self.operator.run(&mut context, &mut state, &mut data)?;

            // Output rules
            let outputs = self
                .operator
                .output_rule(&mut context, &mut state, &run_outputs)?;

            // Send to Links
            for (id, output) in outputs {
                // getting link
                log::debug!("id: {:?}, message: {:?}", id, output);
                if let Some(links) = guard.outputs.get(&id) {
                    let zf_message = Arc::new(ZFMessage::from_component_output(output, timestamp));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }

            // This depends on the Tokens...
            for rx in guard.inputs.iter() {
                rx.discard().await?;
            }

            drop(guard);
        }
    }
}
