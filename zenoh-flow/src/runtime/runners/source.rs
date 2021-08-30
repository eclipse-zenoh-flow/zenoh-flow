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
use crate::runtime::graph::link::ZFLinkSender;
use crate::runtime::message::ZFMessage;
use crate::types::{ZFContext, ZFResult};
use crate::utils::hlc::PeriodicHLC;
use crate::SourceTrait;
use libloading::Library;
use std::collections::HashMap;

#[repr(C)]
pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        Option<HashMap<String, String>>,
    ) -> ZFResult<Box<dyn SourceTrait + Send>>,
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
                    let zf_message = Arc::new(ZFMessage::from_component_output(output, timestamp));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }
        }
    }
}
