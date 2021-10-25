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
use crate::model::node::SourceRecord;
use crate::runtime::graph::link::LinkSender;
use crate::runtime::loader::load_source;
use crate::runtime::message::Message;
use crate::runtime::runners::operator::OperatorIO;
use crate::runtime::RuntimeContext;
use crate::types::ZFResult;
use crate::utils::hlc::PeriodicHLC;
use crate::{Context, Source, State, ZFError};
use libloading::Library;

pub type SourceRegisterFn = fn() -> ZFResult<Arc<dyn Source>>;

pub struct SourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: SourceRegisterFn,
}

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the source/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct SourceRunner {
    hlc: PeriodicHLC,
    state: Arc<RwLock<State>>,
    record: SourceRecord,
    links: Arc<RwLock<Vec<LinkSender<Message>>>>,
    source: Arc<dyn Source>,
    library: Arc<Option<Library>>,
}

impl SourceRunner {
    pub fn try_new(
        context: &RuntimeContext,
        record: SourceRecord,
        io: Option<OperatorIO>,
    ) -> ZFResult<Self> {
        let io = io.ok_or_else(|| {
            ZFError::IOError(format!(
                "Links for Source < {} > were not created.",
                &record.id
            ))
        })?;

        let port_id: Arc<str> = record.output.port_id.clone().into();
        let (_, mut outputs) = io.take();
        let links = outputs.remove(&port_id).ok_or_else(|| {
            ZFError::MissingOutput(format!(
                "Missing links for port < {} > for Source: < {} >.",
                &port_id, &record.id
            ))
        })?;

        let uri = record.uri.as_ref().ok_or_else(|| {
            ZFError::LoadingError(format!(
                "Missing URI for dynamically loaded Source < {} >.",
                record.id.clone()
            ))
        })?;
        let (library, source) = load_source(uri)?;

        let state = source.initialize(&record.configuration);

        Ok(Self {
            hlc: PeriodicHLC::new(context.hlc.clone(), &record.period),
            state: Arc::new(RwLock::new(state)),
            record,
            links: Arc::new(RwLock::new(links)),
            source,
            library: Arc::new(Some(library)),
        })
    }

    pub async fn add_output(&self, output: LinkSender<Message>) {
        // let mut links = self.links.write().await;
        // links.push(output);
        (*self.links.write().await).push(output)
    }

    pub async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.write().await;
        self.source.finalize(&mut state)
    }

    pub async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let links = self.links.read().await;
            let mut state = self.state.write().await;

            // Running
            let output = self.source.run(&mut context, &mut state).await?;

            let timestamp = self.hlc.new_timestamp();

            // Send to Links
            log::debug!(
                "Sending on {:?} data: {:?}",
                self.record.output.port_id,
                output
            );

            let zf_message = Arc::new(Message::from_serdedata(output, timestamp));
            for link in links.iter() {
                log::debug!("\tSending on: {:?}", link);
                link.send(zf_message.clone()).await?;
            }
        }
    }
}
