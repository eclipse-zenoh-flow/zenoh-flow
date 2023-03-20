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
use async_std::sync::Mutex;
use async_trait::async_trait;
use flume::Receiver;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use uhlc::HLC;
use zenoh::buffers::SharedMemoryManager;
use zenoh::prelude::r#async::*;
use zenoh::subscriber::Subscriber;
use zenoh_util::core::AsyncResolve;

/// Default Shared Memory size (10MiB)
static DEFAULT_SHM_SIZE: usize = 10_485_760;

/// Default Shared allocation backoff time (100ms)
static SHM_ALLOCATION_BACKOFF_MS: u64 = 100;

/// The `ZenohSender` is the connector that sends the data to Zenoh when nodes are running on
/// different runtimes.
pub(crate) struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) input_raw: InputRaw,
    pub(crate) z_session: Arc<zenoh::Session>,
    pub(crate) key_expr: KeyExpr<'static>,
    pub(crate) shm: Arc<Mutex<SharedMemoryManager>>,
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
            shm: Arc::new(Mutex::new(
                SharedMemoryManager::make(record.resource.clone(), DEFAULT_SHM_SIZE).map_err(
                    |_| {
                        zferror!(
                            ErrorKind::ConfigurationError,
                            "Unable to allocate {DEFAULT_SHM_SIZE} bytes of shared memory"
                        )
                    },
                )?,
            )),
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
        // Getting shared memory manager
        let mut shm = self.shm.lock().await;
        match self.input_raw.recv().await {
            Ok(message) => {
                // Getting the shared memory buffer
                let mut buff = match shm.alloc(DEFAULT_SHM_SIZE) {
                    Ok(buf) => buf,
                    Err(_) => {
                        async_std::task::sleep(std::time::Duration::from_millis(
                            SHM_ALLOCATION_BACKOFF_MS,
                        ))
                        .await;
                        log::trace!(
                            "After failing allocation the GC collected: {} bytes -- retrying",
                            shm.garbage_collect()
                        );
                        log::trace!(
                            "Trying to de-fragment memory... De-fragmented {} bytes",
                            shm.defragment()
                        );
                        shm.alloc(DEFAULT_SHM_SIZE).map_err(|_| {
                            zferror!(
                                ErrorKind::ConfigurationError,
                                "Unable to allocated {DEFAULT_SHM_SIZE} in the shared memory buffer!"
                            )
                        })?
                    }
                };

                // Getting the underlying slice in the shared memory
                let slice = unsafe { buff.as_mut_slice() };

                // WARNING ACHTUNG ATTENTION
                // This may fail as the message could be bigger than
                // the shared memory buffer that was allocated.
                message.serialize_bincode_into(slice)?;

                self.z_session
                    .put(self.key_expr.clone(), buff)
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
