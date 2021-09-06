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
use crate::{ZFContext, ZFSourceTrait, ZFStateTrait};
use libloading::Library;
use std::collections::HashMap;

pub type ZFSourceRegisterFn = fn() -> ZFResult<Arc<dyn ZFSourceTrait>>;

pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: ZFSourceRegisterFn,
}

pub struct ZFSourceRunnerInner {
    pub outputs: HashMap<String, Vec<ZFLinkSender<ZFMessage>>>,
    pub state: Box<dyn ZFStateTrait>,
}

impl ZFSourceRunnerInner {
    pub fn new(state: Box<dyn ZFStateTrait>) -> Self {
        Self {
            outputs: HashMap::new(),
            state,
        }
    }
}

#[derive(Clone)]
pub struct ZFSourceRunner {
    pub record: Arc<ZFSourceRecord>,
    pub hlc: Arc<PeriodicHLC>,
    pub source: Arc<dyn ZFSourceTrait>,
    pub lib: Arc<Option<Library>>,
    pub inner: Arc<RwLock<ZFSourceRunnerInner>>,
}

impl ZFSourceRunner {
    pub fn new(
        record: ZFSourceRecord,
        hlc: PeriodicHLC,
        source: Arc<dyn ZFSourceTrait>,
        lib: Option<Library>,
    ) -> Self {
        Self {
            record: Arc::new(record),
            hlc: Arc::new(hlc),
            source,
            lib: Arc::new(lib),
            inner: Arc::new(RwLock::new(ZFSourceRunnerInner::new(Box::new(
                crate::EmptyState {},
            )))), //place holder
        }
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
        // self.source.clean(&mut self.state)
        Ok(())
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = ZFContext::default();
        let mut state = self.source.initialize(&self.record.configuration);

        loop {
            let guard = self.inner.read().await;
            // Running
            let run_outputs = self.source.run(&mut context, &mut state).await?;

            // Output
            let mut outputs = self
                .source
                .output_rule(&mut context, &mut state, &run_outputs)?;

            log::debug!("Outputs: {:?}", guard.outputs);

            let timestamp = self.hlc.new_timestamp();

            // Send to Links
            for (id, output) in outputs.drain() {
                log::debug!("Sending on {:?} data: {:?}", id, output);

                if let Some(links) = guard.outputs.get(&id) {
                    let zf_message = Arc::new(ZFMessage::from_component_output(output, timestamp));

                    for tx in links {
                        log::debug!("Sending on: {:?}", tx);
                        tx.send(zf_message.clone()).await?;
                    }
                }
            }

            drop(guard);
        }
    }
}
