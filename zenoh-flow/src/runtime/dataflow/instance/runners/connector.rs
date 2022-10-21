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

/// The `ZenohSender` is the connector that sends the data to Zenoh when nodes are running on
/// different runtimes.
pub(crate) struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) input: Input,
    pub(crate) z_session: Arc<zenoh::Session>,
    pub(crate) key_expr: KeyExpr<'static>,
}

impl ZenohSender {
    /// Creates a new `ZenohSender`.
    ///
    /// We first take the flume channel on which we receive the data to publish and then declare, on
    /// Zenoh, the key expression on which we are going to publish.
    ///
    /// # Errors
    ///
    /// An error variant is returned if:
    /// - no link was created for this sender,
    /// - the declaration of the key expression failed.
    pub(crate) async fn new(
        record: &ZFConnectorRecord,
        session: Arc<Session>,
        mut inputs: Inputs,
    ) -> ZFResult<Self> {
        let input = inputs.take(record.link_id.port_id.clone()).ok_or_else(|| {
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
            id: record.id.clone(),
            input,
            z_session: session.clone(),
            key_expr,
        })
    }
}

#[async_trait]
impl Node for ZenohSender {
    /// An iteration of a ZenohSender: wait for some data to publish, serialize it using `bincode`
    /// and publish it on Zenoh.
    ///
    /// # Errors
    ///
    /// An error variant is returned if:
    /// - serialization fails
    /// - zenoh put fails
    /// - link recv fails
    async fn iteration(&self) -> ZFResult<()> {
        if let Ok(message) = self.input.recv_async().await {
            log::trace!("[ZenohSender: {}] recv_async: OK", self.id);

            let serialized = message.serialize_bincode()?;

            self.z_session
                .put(self.key_expr.clone(), serialized)
                .congestion_control(CongestionControl::Block)
                .res()
                .await
        } else {
            Err(zferror!(ErrorKind::Disconnected).into())
        }
    }
}

/// A `ZenohReceiver` receives the messages from Zenoh when nodes are running on different runtimes.
pub(crate) struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) output: Output,
    pub(crate) subscriber: Subscriber<'static, Receiver<Sample>>,
}

impl ZenohReceiver {
    /// Creates a new `ZenohReceiver`.
    ///
    /// We first declare, on Zenoh, the key expression on which the `ZenohReceiver` will subscribe.
    /// We then declare the subscriber and finally take the output on which the `ZenohReceiver` will
    /// forward the reiceved messages.
    ///
    /// # Errors
    ///
    /// An error variant is returned if:
    /// - the declaration of the key expression failed,
    /// - the declaration of the subscriber failed,
    /// - the link for this connector was not created.
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
            .take(record.link_id.port_id.clone())
            .ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Link < {} > was not created for Connector < {} >.",
                    record.link_id.port_id,
                    record.id
                )
            })?;

        Ok(Self {
            id: record.id.clone(),
            output,
            subscriber,
        })
    }
}

#[async_trait]
impl Node for ZenohReceiver {
    /// An iteration of a `ZenohReceiver`: wait on the subscriber for some message, deserialize it
    /// using `bincode` and send it on the flume channel(s) to the downstream node(s).
    ///
    /// # Errors
    ///
    /// An error variant is returned if:
    /// - the subscriber fails
    /// - the deserialization fails
    /// - sending on the flume channels fails
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
