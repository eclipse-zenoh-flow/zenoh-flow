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
use crate::model::link::PortDescriptor;
use crate::runtime::dataflow::instance::link::LinkReceiver;
use crate::runtime::dataflow::instance::runners::operator::OperatorIO;
use crate::runtime::dataflow::node::SinkLoaded;
use crate::runtime::message::Message;
use crate::runtime::RuntimeContext;
use crate::types::ZFResult;
use crate::{Context, NodeId, Sink, State, ZFError};
use libloading::Library;

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the sink/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct SinkRunner {
    pub(crate) id: NodeId,
    pub(crate) runtime_context: RuntimeContext,
    pub(crate) input: PortDescriptor,
    pub(crate) link: Arc<RwLock<LinkReceiver<Message>>>,
    pub(crate) state: Arc<RwLock<State>>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SinkRunner {
    pub fn try_new(context: RuntimeContext, sink: SinkLoaded, io: OperatorIO) -> ZFResult<Self> {
        let (mut inputs, _) = io.take();
        let port_id: Arc<str> = sink.input.port_id.clone().into();
        let link = inputs.remove(&port_id).ok_or_else(|| {
            ZFError::MissingOutput(format!(
                "Missing link for port < {} > for Sink: < {} >.",
                &port_id, &sink.id
            ))
        })?;

        Ok(Self {
            id: sink.id,
            runtime_context: context,
            input: sink.input,
            link: Arc::new(RwLock::new(link)),
            state: sink.state,
            sink: sink.sink,
            library: sink.library,
        })
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>) {
        (*self.link.write().await) = input;
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.sink.finalize(&mut state)
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let link = self.link.read().await;
            let mut state = self.state.write().await;

            // FEAT. With the introduction of deadline, a DeadlineMissToken could be sent.
            let (_, message) = link.recv().await?;
            let input = match message.as_ref() {
                Message::Data(d) => d.clone(),
                Message::Control(_) => return Err(ZFError::Unimplemented),
            };

            self.sink.run(&mut context, &mut state, input).await?;
        }
    }
}
