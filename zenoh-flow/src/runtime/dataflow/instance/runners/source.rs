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

use super::operator::OperatorIO;
use crate::async_std::sync::{Arc, Mutex};
use crate::model::link::PortDescriptor;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::SourceLoaded;
use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::types::ZFResult;
use crate::{Context, NodeId, PortId, PortType, Source, State, ZFError};
use async_trait::async_trait;
use libloading::Library;
use std::collections::HashMap;
use std::time::Duration;
use uhlc::{Timestamp, NTP64};

// Do not reorder the fields in this struct.
// Rust drops fields in a struct in the same order they are declared.
// Ref: https://doc.rust-lang.org/reference/destructors.html
// We need the state to be dropped before the source/lib, otherwise we
// will have a SIGSEV.
#[derive(Clone)]
pub struct SourceRunner {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) period: Option<Duration>,
    pub(crate) output: PortDescriptor,
    pub(crate) links: Arc<Mutex<Vec<LinkSender<Message>>>>,
    pub(crate) state: Arc<Mutex<State>>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) library: Option<Arc<Library>>,
}

impl SourceRunner {
    pub fn try_new(
        context: InstanceContext,
        source: SourceLoaded,
        io: OperatorIO,
    ) -> ZFResult<Self> {
        let port_id = source.output.port_id.clone();
        let (_, mut outputs) = io.take();
        let links = outputs.remove(&port_id).ok_or_else(|| {
            ZFError::MissingOutput(format!(
                "Missing links for port < {} > for Source: < {} >.",
                &port_id, &source.id
            ))
        })?;

        Ok(Self {
            id: source.id,
            context,
            period: source.period.map(|period| period.to_duration()),
            state: source.state,
            output: source.output,
            links: Arc::new(Mutex::new(links)),
            source: source.source,
            library: source.library,
        })
    }

    fn new_maybe_periodic_timestamp(&self) -> Timestamp {
        let mut timestamp = self.context.runtime.hlc.new_timestamp();
        log::debug!("Timestamp generated: {:?}", timestamp);

        if let Some(period) = &self.period {
            let period_us = period.as_secs_f64();
            let orig_timestamp_us = timestamp.get_time().to_duration().as_secs_f64();

            let nb_period_floored = f64::floor(orig_timestamp_us / period_us);
            let periodic_timestamp_us = Duration::from_secs_f64(period_us * nb_period_floored);

            timestamp = Timestamp::new(
                NTP64::from(periodic_timestamp_us),
                timestamp.get_id().to_owned(),
            );
            log::debug!(
                "Periodic timestamp: {:?} — period = {:?} — original = {:?}",
                periodic_timestamp_us,
                period_us,
                orig_timestamp_us,
            );
        }

        timestamp
    }
}

#[async_trait]
impl Runner for SourceRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Source
    }
    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()> {
        (*self.links.lock().await).push(output);
        println!("Added output");
        Ok(())
    }

    async fn add_input(&self, _input: LinkReceiver<Message>) -> ZFResult<()> {
        Err(ZFError::SourceDoNotHaveInputs)
    }

    async fn clean(&self) -> ZFResult<()> {
        let mut state = self.state.lock().await;
        self.source.finalize(&mut state)
    }

    fn get_outputs(&self) -> HashMap<PortId, PortType> {
        let mut outputs = HashMap::with_capacity(1);
        outputs.insert(self.output.port_id.clone(), self.output.port_type.clone());
        outputs
    }

    fn get_inputs(&self) -> HashMap<PortId, PortType> {
        HashMap::with_capacity(0)
    }

    async fn run(&self) -> ZFResult<()> {
        let mut context = Context::default();

        loop {
            // Guards are taken at the beginning of each iteration to allow interleaving.
            let links = self.links.lock().await;
            let mut state = self.state.lock().await;

            // Running
            let output = self.source.run(&mut context, &mut state).await?;

            let timestamp = self.new_maybe_periodic_timestamp();

            // Send to Links
            log::debug!("Sending on {:?} data: {:?}", self.output.port_id, output);

            let zf_message = Arc::new(Message::from_serdedata(output, timestamp));
            for link in links.iter() {
                log::debug!("\tSending on: {:?}", link);
                link.send(zf_message.clone()).await?;
            }
        }
    }
}
