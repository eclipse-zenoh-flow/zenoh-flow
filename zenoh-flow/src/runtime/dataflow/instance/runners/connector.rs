//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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
use crate::runtime::InstanceContext;
use crate::traits::Node;
use crate::types::{LinkMessage, NodeId};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result as ZFResult;
use async_std::sync::Mutex;
use async_trait::async_trait;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use zenoh::prelude::r#async::*;
use zenoh::shm::SharedMemoryManager;
use zenoh::subscriber::FlumeSubscriber;
use zenoh_util::core::AsyncResolve;

/// The `ZenohSender` is the connector that sends the data to Zenoh when nodes are running on
/// different runtimes.
pub(crate) struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) input_raw: InputRaw,
    pub(crate) z_session: Arc<zenoh::Session>,
    pub(crate) key_expr: KeyExpr<'static>,
    pub(crate) state: Arc<Mutex<ZenohSenderState>>,
    pub(crate) shm_element_size: usize,
    pub(crate) shm_backoff: u64,
}

/// The `ZenohSenderState` stores in a single structure all the fields protected by a lock.
///
/// The fields are:
/// - `shm` holds the [SharedMemoryManager] used to send data through Zenoh's shared memory;
/// - `message_buffer` holds a growable vector of bytes in which the result of the serialization of
///   the [LinkMessage] is stored.
/// - `payload_buffer` holds a growable vector of bytes in which the result of the serialization of
///   the [Payload] contained inside the [LinkMessage] is stored.
pub(crate) struct ZenohSenderState {
    pub(crate) shm: Option<SharedMemoryManager>,
    pub(crate) message_buffer: Vec<u8>,
    pub(crate) payload_buffer: Vec<u8>,
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
        ctx: Arc<InstanceContext>,
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

        let key_expr = ctx
            .runtime
            .session
            .declare_keyexpr(record.resource.clone())
            .res()
            .await?
            .into_owned();

        let mut shm_element_size = 0;
        let mut shm_backoff = 0;
        let mut shm_manager = None;

        if ctx.runtime.use_shm {
            let shm_size = record
                .shared_memory_element_size
                .unwrap_or(ctx.runtime.shared_memory_element_size)
                * record
                    .shared_memory_elements
                    .unwrap_or(ctx.runtime.shared_memory_elements);
            shm_backoff = record
                .shared_memory_backoff
                .unwrap_or(ctx.runtime.shared_memory_backoff);

            shm_manager = Some(
                SharedMemoryManager::make(record.resource.clone(), shm_size).map_err(|_| {
                    zferror!(
                        ErrorKind::ConfigurationError,
                        "Unable to allocate {shm_size} bytes of shared memory"
                    )
                })?,
            );

            shm_element_size = record
                .shared_memory_element_size
                .unwrap_or(ctx.runtime.shared_memory_element_size);
        }

        Ok(Self {
            id: record.id.clone(),
            input_raw: InputRaw {
                port_id: record.link_id.port_id.clone(),
                receivers,
            },
            z_session: ctx.runtime.session.clone(),
            key_expr,
            shm_element_size,
            shm_backoff,
            state: Arc::new(Mutex::new(ZenohSenderState {
                shm: shm_manager,
                message_buffer: Vec::default(),
                payload_buffer: Vec::default(),
            })),
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
        match self.input_raw.recv().await {
            Ok(message) => {
                let mut state = self.state.lock().await;

                // NOTE: as per the documentation of Vec::default, which is what the
                // `std::mem::take` will call, no allocation is performed until elements are pushed
                // in the vector.
                let mut message_buffer = std::mem::take(&mut state.message_buffer);
                let mut payload_buffer = std::mem::take(&mut state.payload_buffer);

                match state.shm {
                    Some(ref mut shm) => {
                        // Getting the shared memory buffer
                        let mut buff = match shm.alloc(self.shm_element_size) {
                            Ok(buf) => buf,
                            Err(_) => {
                                async_std::task::sleep(std::time::Duration::from_millis(
                                    self.shm_backoff,
                                ))
                                .await;
                                log::trace!(
                                    "[ZenohSender: {}] After failing allocation the GC collected: {} bytes -- retrying",
                                    self.id,
                                    shm.garbage_collect()
                                );
                                log::trace!(
                                    "[ZenohSender: {}] Trying to de-fragment memory... De-fragmented {} bytes",
                                    self.id,
                                    shm.defragment()
                                );
                                shm.alloc(self.shm_element_size).map_err(|_| {
                                    zferror!(
                                        ErrorKind::ConfigurationError,
                                        "Unable to allocated {} in the shared memory buffer!",
                                        self.shm_element_size
                                    )
                                })?
                            }
                        };

                        // Getting the underlying slice in the shared memory
                        let slice = unsafe { buff.as_mut_slice() };

                        // WARNING ACHTUNG ATTENTION
                        // This may fail as the message could be bigger than
                        // the shared memory buffer that was allocated.
                        match message.serialize_bincode_into_shm(slice, &mut payload_buffer) {
                            Ok(_) => {
                                // If the serialization succeeded then we send the shared memory
                                // buffer.
                                self.z_session
                                    .put(self.key_expr.clone(), buff)
                                    .congestion_control(CongestionControl::Block)
                                    .res()
                                    .await?;
                            }
                            Err(e) => {
                                // Otherwise we log a warn and we serialize on a normal
                                // Vec<u8>
                                message.serialize_bincode_into(
                                    &mut message_buffer,
                                    &mut payload_buffer,
                                )?;
                                log::warn!(
                                    "[ZenohSender: {}] Unable to serialize into shared memory: {}, serialized size {}, shared memory size {}",
                                    self.id,
                                    e,
                                    state.message_buffer.len(),
                                    self.shm_element_size,
                                );

                                self.z_session
                                    .put(self.key_expr.clone(), message_buffer.clone())
                                    .congestion_control(CongestionControl::Block)
                                    .res()
                                    .await?;
                            }
                        }
                    }
                    None => {
                        message.serialize_bincode_into(&mut message_buffer, &mut payload_buffer)?;
                        self.z_session
                            .put(self.key_expr.clone(), message_buffer.clone())
                            .congestion_control(CongestionControl::Block)
                            .res()
                            .await?;
                    }
                }

                // NOTE: set back the buffers such that we don't have to allocate memory again.
                state.message_buffer = message_buffer;
                state.payload_buffer = payload_buffer;
                Ok(())
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
    pub(crate) subscriber: FlumeSubscriber<'static>,
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
        ctx: Arc<InstanceContext>,
        mut outputs: Outputs,
    ) -> ZFResult<Self> {
        let key_expr = ctx
            .runtime
            .session
            .declare_keyexpr(record.resource.clone())
            .res()
            .await?
            .into_owned();
        let subscriber = ctx
            .runtime
            .session
            .declare_subscriber(key_expr.clone())
            .res()
            .await?;
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
                hlc: ctx.runtime.hlc.clone(),
                last_watermark: Arc::new(AtomicU64::new(
                    ctx.runtime.hlc.new_timestamp().get_time().as_u64(),
                )),
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
