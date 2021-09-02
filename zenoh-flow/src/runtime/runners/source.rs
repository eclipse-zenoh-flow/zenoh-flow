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
use crate::model::operator::ZFSourceRecord;
use crate::runtime::graph::link::ZFLinkSender;
use crate::runtime::message::ZFMessage;
use crate::types::ZFResult;
use crate::utils::hlc::PeriodicHLC;
use crate::{ZFSourceTrait, ZFStateTrait};
use libloading::Library;
use std::collections::HashMap;

pub type ZFSourceRegisterFn = fn() -> ZFResult<Box<dyn ZFSourceTrait + Send>>;

pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: ZFSourceRegisterFn,
}

pub struct ZFSourceRunner {
    pub record: ZFSourceRecord,
    pub hlc: PeriodicHLC,
    pub source: Box<dyn ZFSourceTrait + Send>,
    pub lib: Option<Library>,
    pub outputs: HashMap<String, Vec<ZFLinkSender<ZFMessage>>>,
    pub state: Box<dyn ZFStateTrait>,
}

impl ZFSourceRunner {
    pub fn new(
        record: ZFSourceRecord,
        hlc: PeriodicHLC,
        source: Box<dyn ZFSourceTrait + Send>,
        lib: Option<Library>,
    ) -> Self {
        Self {
            state: source.initial_state(&record.configuration),
            record,
            hlc,
            source,
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
        loop {
            // Running
            let run_outputs = self.source.run(&mut self.state).await?;

            // Output
            let mut outputs = self.source.output_rule(&mut self.state, &run_outputs)?;
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
