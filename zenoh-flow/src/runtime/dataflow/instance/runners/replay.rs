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

use super::{Runner, RunnerKind};
use crate::async_std::sync::{Arc, Mutex};
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::{ControlMessage, NodeId, PortId, PortType, ZFError, ZFResult};
use async_std::task;
use async_trait::async_trait;
use futures::prelude::*;
use std::collections::HashMap;
use zenoh::net::*;

#[derive(Clone)]
pub struct ZenohReplay {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) node_id: NodeId,
    pub(crate) port_id: PortId,
    pub(crate) port_type: PortType,
    pub(crate) resource_name: String,
    pub(crate) links: Arc<Mutex<Vec<LinkSender<Message>>>>,
}

impl ZenohReplay {
    pub fn try_new(
        id: NodeId,
        context: InstanceContext,
        node_id: NodeId,
        port_id: PortId,
        port_type: PortType,
        links: Vec<LinkSender<Message>>,
        resource_name: String,
    ) -> ZFResult<Self> {
        if links.len() != 1 {
            return Err(ZFError::IOError(format!(
                "Expected exactly one link for port < {} > for Replay < {} >, found: {}",
                &port_id,
                &id,
                links.len()
            )));
        }

        Ok(Self {
            id,
            context,
            node_id,
            port_id,
            port_type,
            resource_name,
            links: Arc::new(Mutex::new(links)),
        })
    }

    async fn send_data(&self, msg: Message) -> ZFResult<()> {
        let links = self.links.lock().await;
        let msg = Arc::new(msg);
        for link in links.iter() {
            log::debug!("ZenohReplay - OUT =>{:?} ", msg);
            link.send(msg.clone()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Runner for ZenohReplay {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Source
    }
    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()> {
        (*self.links.lock().await).push(output);
        Ok(())
    }

    async fn add_input(&self, _input: LinkReceiver<Message>) -> ZFResult<()> {
        Err(ZFError::SourceDoNotHaveInputs)
    }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }

    fn get_outputs(&self) -> HashMap<PortId, PortType> {
        let mut outputs = HashMap::with_capacity(1);
        outputs.insert(self.port_id.clone(), self.port_type.clone());
        outputs
    }

    fn get_inputs(&self) -> HashMap<PortId, PortType> {
        HashMap::with_capacity(0)
    }
    async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohReplay - {} - Started", self.resource_name);
        let query_target = QueryTarget {
            kind: queryable::STORAGE,
            target: Target::default(),
        };

        let mut replies = self
            .context
            .runtime
            .session
            .query(
                &self.resource_name.clone().into(),
                "",
                query_target,
                QueryConsolidation::default(),
            )
            .await?;

        // Placeholder
        let mut last_ts = self.context.runtime.hlc.new_timestamp();

        while let Some(msg) = replies.next().await {
            log::debug!("ZenohReplay - {}<={:?} ", self.resource_name, msg);
            let de: Message = bincode::deserialize(&msg.data.payload.contiguous())
                .map_err(|_| ZFError::DeseralizationError)?;
            match &de {
                Message::Control(ref ctrl_msg) => match &ctrl_msg {
                    ControlMessage::RecordingStart(ref ts) => {
                        last_ts = ts.timestamp;
                    }
                    ControlMessage::RecordingStop(_) => return Ok(()),
                    _ => {
                        self.send_data(de).await?;
                    }
                },
                Message::Data(ref data_msg) => {
                    let data_ts = data_msg.timestamp;
                    let wait_time = data_ts.get_diff_duration(&last_ts);

                    log::debug!("ZenohReplay - Wait for {:?} ", wait_time);
                    task::sleep(wait_time).await;

                    self.send_data(de).await?;

                    // Updating last sent timestamp
                    last_ts = data_ts;
                }
            }
        }

        Err(ZFError::Disconnected)
    }
}
