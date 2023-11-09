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

use anyhow::{anyhow, bail};
use async_std::sync::Mutex;
use std::sync::Arc;
use zenoh::{prelude::r#async::*, subscriber::FlumeSubscriber};
use zenoh_flow_commons::{NodeId, Result};
use zenoh_flow_nodes::prelude::{InputRaw, Inputs, LinkMessage, Node, OutputRaw, Outputs};
use zenoh_flow_records::{ReceiverRecord, SenderRecord};

#[cfg(feature = "shared-memory")]
use crate::shared_memory::SharedMemory;
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;

pub(crate) struct ZenohConnectorSender {
    id: NodeId,
    input: InputRaw,
    key_expr: OwnedKeyExpr,
    session: Arc<Session>,
    state: Arc<Mutex<State>>,
}

struct State {
    pub(crate) payload_buffer: Vec<u8>,
    pub(crate) message_buffer: Vec<u8>,
    #[cfg(feature = "shared-memory")]
    pub(crate) shm: SharedMemory,
}

impl ZenohConnectorSender {
    pub(crate) fn try_new(
        session: Arc<Session>,
        #[cfg(feature = "shared-memory")] shm_config: &SharedMemoryConfiguration,
        record: SenderRecord,
        mut inputs: Inputs,
    ) -> Result<Self> {
        let input = inputs
            .take(&record.resource)
            // TODO@J-Loudet
            .ok_or_else(|| anyhow!(""))?
            .raw();

        Ok(Self {
            input,
            key_expr: record.resource,
            state: Arc::new(Mutex::new(State {
                payload_buffer: Vec::new(),
                message_buffer: Vec::new(),
                #[cfg(feature = "shared-memory")]
                shm: SharedMemory::new(&record.id, session.clone(), shm_config),
            })),
            id: record.id,
            session,
        })
    }
}

#[async_trait::async_trait]
impl Node for ZenohConnectorSender {
    async fn iteration(&self) -> Result<()> {
        match self.input.recv().await {
            Ok(message) => {
                let mut state = self.state.lock().await;

                let mut message_buffer = std::mem::take(&mut state.message_buffer);
                let mut payload_buffer = std::mem::take(&mut state.payload_buffer);

                #[cfg(feature = "shared-memory")]
                {
                    if let Err(e) = state
                        .shm
                        .try_send_message(
                            &self.key_expr,
                            message,
                            &mut message_buffer,
                            &mut payload_buffer,
                        )
                        .await
                    {
                        tracing::warn!(
                            r#"
[connector sender (zenoh): {}][key expr: {}] Failed to send the message via Zenoh's shared memory.

Caused by:
{:?}
"#,
                            self.id,
                            self.key_expr,
                            e
                        );
                        tracing::warn!(
                        "[connector sender (zenoh): {}][key expr: {}] Attempting to send via a non-shared memory channel.",
                        self.id,
                        self.key_expr
                    );

                        self.session
                            .put(&self.key_expr, message_buffer)
                            .res()
                            .await
                            .map_err(|e| {
                                anyhow!(
                                    r#"
[connector sender (zenoh): {}][key expr: {}] Failed to send the message via a Zenoh publication.

Caused by:
{:?}
"#,
                                    self.id,
                                    self.key_expr,
                                    e
                                )
                            })?;
                    }
                }

                #[cfg(not(feature = "shared-memory"))]
                {
                    message.serialize_bincode_into(&mut message_buffer, &mut payload_buffer)?;

                    self.session
                        .put(&self.key_expr, message_buffer)
                        .res()
                        .await
                        .map_err(|e| {
                            anyhow!(
                                r#"
[connector sender (zenoh): {}][key expr: {}] Failed to send the message via a Zenoh publication.

Caused by:
{:?}
"#,
                                self.id,
                                self.key_expr,
                                e
                            )
                        })?;
                }

                Ok(())
            }

            Err(e) => {
                tracing::error!(
                    r#"
[connector sender (zenoh): {}][key expr: {}] Internal channel returned the following error:
{:?}
"#,
                    self.id,
                    self.key_expr,
                    e
                );
                Err(e)
            }
        }
    }
}

pub(crate) struct ZenohConnectorReceiver {
    pub(crate) id: NodeId,
    pub(crate) key_expr: OwnedKeyExpr,
    pub(crate) output_raw: OutputRaw,
    pub(crate) subscriber: FlumeSubscriber<'static>,
}

impl ZenohConnectorReceiver {
    pub(crate) async fn try_new(
        session: Arc<Session>,
        record: ReceiverRecord,
        mut outputs: Outputs,
    ) -> Result<Self> {
        let ke = session
            .declare_keyexpr(record.resource.clone())
            .res()
            .await
            // TODO@J-Loudet
            .map_err(|e| anyhow!("{:?}", e))?;

        let subscriber = session
            .declare_subscriber(ke)
            .res()
            .await
            // TODO@J-Loudet
            .map_err(|e| anyhow!("{:?}", e))?;

        let output_raw = outputs
            .take(&record.resource)
            // TODO@J-Loudet
            .ok_or_else(|| anyhow!(""))?
            .raw();

        Ok(Self {
            id: record.id,
            key_expr: record.resource,
            output_raw,
            subscriber,
        })
    }
}

#[async_trait::async_trait]
impl Node for ZenohConnectorReceiver {
    async fn iteration(&self) -> Result<()> {
        match self.subscriber.recv_async().await {
            Ok(message) => {
                let de: LinkMessage = bincode::deserialize(&message.value.payload.contiguous())?;

                self.output_raw.forward(de).await
            }

            Err(e) => {
                tracing::error!(
                    r#"
[connector receiver (zenoh): {}][key expr: {}] Zenoh subscriber returned the following error:
{:?}
"#,
                    self.id,
                    self.key_expr,
                    e
                );
                bail!("{:?}", e)
            }
        }
    }
}
