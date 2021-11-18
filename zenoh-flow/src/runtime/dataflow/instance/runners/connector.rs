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

use std::collections::HashMap;

use crate::async_std::sync::{Arc, Mutex};
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::operator::OperatorIO;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::{NodeId, PortId, PortType, ZFError, ZFResult};
use async_trait::async_trait;
use futures::prelude::*;
use zenoh::net::{Reliability, SubInfo, SubMode};

#[derive(Clone)]
pub struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) link: Arc<Mutex<LinkReceiver<Message>>>,
}

impl ZenohSender {
    pub fn try_new(
        context: InstanceContext,
        record: ZFConnectorRecord,
        io: OperatorIO,
    ) -> ZFResult<Self> {
        let (mut inputs, _) = io.take();
        let port_id = record.link_id.port_id.clone();
        let link = inputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        Ok(Self {
            id: record.id.clone(),
            context,
            record,
            link: Arc::new(Mutex::new(link)),
        })
    }
}
#[async_trait]
impl Runner for ZenohSender {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Connector
    }
    async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohSender - {} - Started", self.record.resource);
        let guard = self.link.lock().await;
        while let Ok((_, message)) = (*guard).recv().await {
            log::debug!("ZenohSender IN <= {:?} ", message);

            let serialized = message.serialize_bincode()?;
            log::debug!("ZenohSender - {}=>{:?} ", self.record.resource, serialized);
            self.context
                .runtime
                .session
                .write(&self.record.resource.clone().into(), serialized.into())
                .await?;
        }

        Err(ZFError::Disconnected)
    }

    fn get_outputs(&self) -> HashMap<PortId, PortType> {
        let mut outputs = HashMap::with_capacity(1);
        outputs.insert(
            self.record.link_id.port_id.clone(),
            self.record.link_id.port_type.clone(),
        );
        outputs
    }

    fn get_inputs(&self) -> HashMap<PortId, PortType> {
        HashMap::with_capacity(0)
    }

    async fn add_input(&self, input: LinkReceiver<Message>) -> ZFResult<()> {
        *(self.link.lock().await) = input;
        Ok(())
    }

    async fn add_output(&self, _output: LinkSender<Message>) -> ZFResult<()> {
        Err(ZFError::SenderDoNotHaveOutputs)
    }

    async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender<Message>>> {
        HashMap::with_capacity(0)
    }

    async fn get_input_links(&self) -> HashMap<PortId, LinkReceiver<Message>> {
        let mut inputs = HashMap::with_capacity(1);
        inputs.insert(
            self.record.link_id.port_id.clone(),
            self.link.lock().await.clone(),
        );
        inputs
    }

    async fn start_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn stop_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn is_recording(&self) -> bool {
        false
    }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) link: Arc<Mutex<LinkSender<Message>>>,
}

impl ZenohReceiver {
    pub fn try_new(
        context: InstanceContext,
        record: ZFConnectorRecord,
        io: OperatorIO,
    ) -> ZFResult<Self> {
        let (_, mut outputs) = io.take();
        let port_id = record.link_id.port_id.clone();
        let mut links = outputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        if links.len() != 1 {
            return Err(ZFError::IOError(format!(
                "Expected exactly one link for port < {} > for Connector < {} >, found: {}",
                &port_id,
                &record.id,
                links.len()
            )));
        }

        let link = links.remove(0);

        Ok(Self {
            id: record.id.clone(),
            context,
            record,
            link: Arc::new(Mutex::new(link)),
        })
    }
}

#[async_trait]
impl Runner for ZenohReceiver {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Connector
    }

    async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohReceiver - {} - Started", self.record.resource);
        let guard = self.link.lock().await;
        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None,
        };

        let mut subscriber = self
            .context
            .runtime
            .session
            .declare_subscriber(&self.record.resource.clone().into(), &sub_info)
            .await?;

        while let Some(msg) = subscriber.receiver().next().await {
            log::debug!("ZenohSender - {}<={:?} ", self.record.resource, msg);
            let de: Message = bincode::deserialize(&msg.payload.contiguous())
                .map_err(|_| ZFError::DeseralizationError)?;
            log::debug!("ZenohSender - OUT =>{:?} ", de);
            (*guard).send(Arc::new(de)).await?;
        }

        Err(ZFError::Disconnected)
    }

    fn get_inputs(&self) -> HashMap<PortId, PortType> {
        HashMap::with_capacity(0)
    }

    fn get_outputs(&self) -> HashMap<PortId, PortType> {
        let mut inputs = HashMap::with_capacity(1);
        inputs.insert(
            self.record.link_id.port_id.clone(),
            self.record.link_id.port_type.clone(),
        );
        inputs
    }
    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()> {
        (*self.link.lock().await) = output;
        Ok(())
    }

    async fn add_input(&self, _input: LinkReceiver<Message>) -> ZFResult<()> {
        Err(ZFError::ReceiverDoNotHaveInputs)
    }

    async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender<Message>>> {
        let mut outputs = HashMap::with_capacity(1);
        outputs.insert(
            self.record.link_id.port_id.clone(),
            vec![self.link.lock().await.clone()],
        );
        outputs
    }

    async fn get_input_links(&self) -> HashMap<PortId, LinkReceiver<Message>> {
        HashMap::with_capacity(0)
    }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }

    async fn start_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn stop_recording(&self) -> ZFResult<String> {
        Err(ZFError::Unimplemented)
    }

    async fn is_recording(&self) -> bool {
        false
    }
}
