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
use crate::model::node::SinkRecord;
use crate::runtime::graph::link::LinkReceiver;
use crate::runtime::message::{DataMessage, Message};
use crate::types::ZFResult;
use crate::{Context, Sink, ZFState};
use futures::future;
use libloading::Library;

pub type SinkRegisterFn = fn() -> ZFResult<Arc<dyn Sink>>;

pub struct SinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: SinkRegisterFn,
}

pub struct SinkRunnerInner {
    pub inputs: Vec<LinkReceiver<Message>>,
    pub state: Box<dyn ZFState>,
}

impl SinkRunnerInner {
    pub fn new(state: Box<dyn ZFState>) -> Self {
        Self {
            inputs: vec![],
            state,
        }
    }
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the sink/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct SinkRunner {
    pub record: Arc<SinkRecord>,
    pub state: Arc<RwLock<Box<dyn ZFState>>>,
    pub inputs: Arc<RwLock<Vec<LinkReceiver<Message>>>>,
    pub sink: Arc<dyn Sink>,
    pub lib: Arc<Option<Library>>,
}

impl SinkRunner {
    pub fn new(record: SinkRecord, sink: Arc<dyn Sink>, lib: Option<Library>) -> Self {
        let state = sink.initialize(&record.configuration);
        Self {
            record: Arc::new(record),
            state: Arc::new(RwLock::new(state)),
            inputs: Arc::new(RwLock::new(vec![])),
            sink,
            lib: Arc::new(lib),
        }
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>) {
        self.inputs.write().await.push(input);
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.sink.finalize(&mut state)
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let inputs = self.inputs.read().await;
            let mut state = self.state.write().await;

            let links: Vec<_> = inputs.iter().map(|rx| rx.recv()).collect();

            // FEAT. With the introduction of deadline, a DeadlineMissToken could be sent.
            let input: DataMessage;

            match future::select_all(links).await {
                (Ok((_, message)), _, _) => match message.as_ref() {
                    Message::Data(d) => input = d.clone(),
                    Message::Control(_) => todo!(),
                },
                (Err(e), index, _) => {
                    log::error!("[Link] Received an error on < {} >: {:?}.", index, e);
                    continue;
                }
            }

            self.sink.run(&mut context, &mut state, input).await?;
        }
    }
}
