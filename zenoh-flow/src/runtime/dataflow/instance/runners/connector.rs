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

use crate::io::{Inputs, Outputs};
use crate::model::record::ZFConnectorRecord;
use crate::prelude::{InputRaw, OutputRaw};
use crate::traits::Node;
use crate::types::{LinkMessage, NodeId};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result as ZFResult;
use async_trait::async_trait;
use flume::Receiver;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use uhlc::HLC;
use zenoh::prelude::r#async::*;
use zenoh::subscriber::Subscriber;
use zenoh_util::core::AsyncResolve;

/// The `ZenohSender` is the connector that sends the data to Zenoh when nodes are running on
/// different runtimes.
pub(crate) struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) input_raw: InputRaw,
    pub(crate) z_session: Arc<zenoh::Session>,
    pub(crate) key_expr: KeyExpr<'static>,
}

impl ZenohSender {
    /// Creates a new `ZenohSender`.
    ///
    /// We first take the flume channel on which we receive the data to publish and then declare, on
    /// Zenoh, the key expression on which we are going to publish.
    ///
    /// ## Errors
    ///
    /// An error variant is returned if:
    /// - no link was created for this sender,
    /// - the declaration of the key expression failed.
    pub(crate) async fn new(
        record: &ZFConnectorRecord,
        session: Arc<Session>,
        mut inputs: Inputs,
    ) -> ZFResult<Self> {
        let receivers = inputs.hmap.remove(&record.link_id.port_id).ok_or_else(|| {
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
            input_raw: InputRaw {
                port_id: record.link_id.port_id.clone(),
                receivers,
            },
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
    /// ## Errors
    ///
    /// An error variant is returned if:
    /// - serialization fails
    /// - zenoh put fails
    /// - link recv fails
    async fn iteration(&self) -> ZFResult<()> {
        match self.input_raw.recv().await {
            Ok(message) => {
                self.z_session
                    .put(self.key_expr.clone(), message.serialize_bincode()?)
                    .congestion_control(CongestionControl::Block)
                    .res()
                    .await
            }
            Err(e) => Err(zferror!(
                ErrorKind::Disconnected,
                "[ZenohSender: {}] {:?}",
                self.id,
                e
            )
            .into()),
        }
    }
}

/// A `ZenohReceiver` receives the messages from Zenoh when nodes are running on different runtimes.
pub(crate) struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) output_raw: OutputRaw,
    pub(crate) subscriber: Subscriber<'static, Receiver<Sample>>,
}

impl ZenohReceiver {
    /// Creates a new `ZenohReceiver`.
    ///
    /// We first declare, on Zenoh, the key expression on which the `ZenohReceiver` will subscribe.
    /// We then declare the subscriber and finally take the output on which the `ZenohReceiver` will
    /// forward the reiceved messages.
    ///
    /// ## Errors
    ///
    /// An error variant is returned if:
    /// - the declaration of the key expression failed,
    /// - the declaration of the subscriber failed,
    /// - the link for this connector was not created.
    pub(crate) async fn new(
        record: &ZFConnectorRecord,
        session: Arc<Session>,
        hlc: Arc<HLC>,
        mut outputs: Outputs,
    ) -> ZFResult<Self> {
        let key_expr = session
            .declare_keyexpr(record.resource.clone())
            .res()
            .await?
            .into_owned();
        let subscriber = session.declare_subscriber(key_expr.clone()).res().await?;
        let senders = outputs
            .hmap
            .remove(&record.link_id.port_id)
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
            output_raw: OutputRaw {
                port_id: record.link_id.port_id.clone(),
                senders,
                hlc: hlc.clone(),
                last_watermark: Arc::new(AtomicU64::new(hlc.new_timestamp().get_time().as_u64())),
            },
            subscriber,
        })
    }
}

#[async_trait]
impl Node for ZenohReceiver {
    /// An iteration of a `ZenohReceiver`: wait on the subscriber for some message, deserialize it
    /// using `bincode` and send it on the flume channel(s) to the downstream node(s).
    ///
    /// ## Errors
    ///
    /// An error variant is returned if:
    /// - the subscriber fails
    /// - the deserialization fails
    /// - sending on the channels fails
    async fn iteration(&self) -> ZFResult<()> {
        match self.subscriber.recv_async().await {
            Ok(message) => {
                let de: LinkMessage = bincode::deserialize(&message.value.payload.contiguous())
                    .map_err(|e| {
                        zferror!(
                            ErrorKind::DeserializationError,
                            "[ZenohReceiver: {}] {:?}",
                            self.id,
                            e
                        )
                    })?;

                self.output_raw.forward(de).await?;

                Ok(())
            }

            Err(e) => Err(zferror!(ErrorKind::Disconnected, e).into()),
        }
    }
}
