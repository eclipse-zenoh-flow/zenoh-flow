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
use crate::model::operator::ZFSourceRecord;
use crate::runtime::graph::link::ZFLinkSender;
use crate::runtime::message::ZFMessage;
use crate::types::ZFResult;
use crate::utils::hlc::PeriodicHLC;
use crate::{Context, PortId, State, ZFSourceTrait};
use libloading::Library;
use std::collections::HashMap;

pub type ZFSourceRegisterFn = fn() -> ZFResult<Arc<dyn ZFSourceTrait>>;

pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: ZFSourceRegisterFn,
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the source/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct ZFSourceRunner {
    pub record: Arc<ZFSourceRecord>,
    pub hlc: Arc<PeriodicHLC>,
    pub state: Arc<RwLock<Box<dyn State>>>,
    pub outputs: Arc<RwLock<HashMap<PortId, Vec<ZFLinkSender<ZFMessage>>>>>,
    pub source: Arc<dyn ZFSourceTrait>,
    pub lib: Arc<Option<Library>>,
}

impl ZFSourceRunner {
    pub fn new(
        record: ZFSourceRecord,
        hlc: PeriodicHLC,
        source: Arc<dyn ZFSourceTrait>,
        lib: Option<Library>,
    ) -> Self {
        let state = source.initialize(&record.configuration);
        Self {
            record: Arc::new(record),
            hlc: Arc::new(hlc),
            state: Arc::new(RwLock::new(state)),
            outputs: Arc::new(RwLock::new(HashMap::new())),
            source,
            lib: Arc::new(lib),
        }
    }

    pub async fn add_output(&self, output: ZFLinkSender<ZFMessage>) {
        let mut outputs = self.outputs.write().await;
        let key = output.id();
        if let Some(links) = outputs.get_mut(key.as_ref()) {
            links.push(output);
        } else {
            outputs.insert(key, vec![output]);
        }
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.source.clean(&mut state)
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow
            // interleaving.
            let outputs_links = self.outputs.read().await;
            let mut state = self.state.write().await;

            // Running
            let run_outputs = self.source.run(&mut context, &mut state).await?;

            // Output
            let mut outputs = self
                .source
                .output_rule(&mut context, &mut state, &run_outputs)?;

            log::debug!("Outputs: {:?}", outputs);

            let timestamp = self.hlc.new_timestamp();

            // Send to Links
            for (id, output) in outputs.drain() {
                log::debug!("Sending on {:?} data: {:?}", id, output);

                if let Some(links) = outputs_links.get(&id) {
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
