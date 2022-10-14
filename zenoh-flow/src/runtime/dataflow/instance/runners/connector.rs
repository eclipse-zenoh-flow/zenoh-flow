//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use crate::model::record::ZFConnectorRecord;
use crate::prelude::Streams;
use crate::traits::Node;
use crate::types::{Input, Inputs, Message, NodeId, Output, Outputs};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result as ZFResult;
use async_trait::async_trait;
use flume::Receiver;
use std::sync::Arc;
use zenoh::prelude::r#async::*;
use zenoh::subscriber::Subscriber;
use zenoh_util::core::AsyncResolve;

pub(crate) struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) input: Input,
    pub(crate) z_session: Arc<zenoh::Session>,
    pub(crate) key_expr: KeyExpr<'static>,
}

impl ZenohSender {
    pub(crate) async fn new(
        record: &ZFConnectorRecord,
        session: Arc<Session>,
        mut inputs: Inputs,
    ) -> ZFResult<Self> {
        let input = inputs
            .take(Arc::clone(&record.link_id.port_id))
            .ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Link < {} > was not created for Connector < {} >.",
                    record.link_id.port_id,
                    record.id
                )
            })?;

        let key_expr = session
            .declare_keyexpr(record.resource.clone())
            .res()
            .await?
            .into_owned();

        Ok(Self {
            id: Arc::clone(&record.id),
            input,
            z_session: Arc::clone(&session),
            key_expr,
        })
    }
}

#[async_trait]
impl Node for ZenohSender {
    async fn iteration(&self) -> ZFResult<()> {
        if let Ok(message) = self.input.recv_async().await {
            log::trace!("[ZenohSender: {}] recv_async: OK", self.id);

            let serialized = message.serialize_bincode()?;

            self.z_session
                .put(self.key_expr.clone(), serialized)
                // .put(key_expr, &(**buffer)[0..size])
                .congestion_control(CongestionControl::Block)
                .res()
                .await
        } else {
            Err(zferror!(ErrorKind::Disconnected).into())
        }
    }
}

pub(crate) struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) output: Output,
    pub(crate) subscriber: Subscriber<'static, Receiver<Sample>>,
}

impl ZenohReceiver {
    pub(crate) async fn new(
        record: &ZFConnectorRecord,
        session: Arc<Session>,
        mut outputs: Outputs,
    ) -> ZFResult<Self> {
        let key_expr = session
            .declare_keyexpr(record.resource.clone())
            .res()
            .await?
            .into_owned();
        let subscriber = session.declare_subscriber(key_expr.clone()).res().await?;
        let output = outputs
            .take(Arc::clone(&record.link_id.port_id))
            .ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Link < {} > was not created for Connector < {} >.",
                    record.link_id.port_id,
                    record.id
                )
            })?;

        Ok(Self {
            id: Arc::clone(&record.id),
            output,
            subscriber,
        })
    }
}

#[async_trait]
impl Node for ZenohReceiver {
    async fn iteration(&self) -> ZFResult<()> {
        if let Ok(msg) = self.subscriber.recv_async().await {
            let de: Message = bincode::deserialize(&msg.value.payload.contiguous())
                .map_err(|e| zferror!(ErrorKind::DeseralizationError, e))?;
            self.output.send_to_all_async(de).await?;
            log::trace!("[ZenohReceiver: {}] send_async: OK", self.id);
            Ok(())
        } else {
            Err(zferror!(ErrorKind::Disconnected).into())
        }
    }
}
