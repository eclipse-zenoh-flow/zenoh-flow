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
use crate::runtime::graph::link::{ZFLinkReceiver, ZFLinkSender};
use crate::runtime::message::ZFMessage;
use crate::types::{Token, ZFContext, ZFInput, ZFResult};
use crate::OperatorTrait;
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use uhlc::HLC;

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
            for rx in self.inputs.iter() {
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
                    let zf_message = Arc::new(ZFMessage::from_component_output(output, timestamp));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }

            // This depends on the Tokens...
            for rx in self.inputs.iter() {
                rx.discard().await?;
            }
        }
    }
}
