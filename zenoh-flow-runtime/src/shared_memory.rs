//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use anyhow::anyhow;
use std::sync::Arc;
use zenoh::shm::SharedMemoryManager;
use zenoh::{prelude::r#async::*, shm::SharedMemoryBuf};
use zenoh_flow_commons::{NodeId, Result, SharedMemoryConfiguration};
use zenoh_flow_nodes::prelude::{DataMessage, LinkMessage};

pub(crate) struct SharedMemory {
    session: Arc<Session>,
    manager: SharedMemoryManager,
    configuration: SharedMemoryConfiguration,
}

impl SharedMemory {
    pub(crate) fn new(
        id: &NodeId,
        session: Arc<Session>,
        shm_configuration: &SharedMemoryConfiguration,
    ) -> Self {
        Self {
            session,
            manager: SharedMemoryManager::make(
                format!("{}>shared-memory-manager", id),
                shm_configuration.size,
            )
            .unwrap(),
            configuration: *shm_configuration,
        }
    }

    /// This method tries to send the [LinkMessage] via Zenoh's shared memory.
    ///
    /// # Errors
    ///
    /// This method can fail for multiple reasons:
    /// 1. Zenoh's [SharedMemoryManager] did not manage to allocate a buffer.
    /// 2. The serialization of the message into the buffer failed.
    /// 3. Zenoh failed to send the message via shared memory.
    pub(crate) async fn try_send_message(
        &mut self,
        key_expr: &str,
        message: LinkMessage,
        message_buffer: &mut Vec<u8>,
        payload_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        message.serialize_bincode_into(message_buffer, payload_buffer)?;
        self.try_put_buffer(key_expr, message_buffer).await
    }

    pub(crate) async fn try_send_payload(
        &mut self,
        key_expr: &str,
        data: DataMessage,
        payload_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        data.try_as_bytes_into(payload_buffer)?;
        self.try_put_buffer(key_expr, payload_buffer).await
    }

    async fn try_put_buffer(&mut self, key_expr: &str, buffer: &mut Vec<u8>) -> Result<()> {
        let mut shm_buffer = self.try_allocate_buffer(buffer.len()).await?;
        let slice = unsafe { shm_buffer.as_mut_slice() };
        slice.clone_from_slice(buffer.as_mut_slice());

        self.session
            .put(key_expr, shm_buffer)
            .congestion_control(CongestionControl::Block)
            .res()
            .await
            .map_err(|e| {
                anyhow!(
                    r#"shared memory: Put on < {:?} > failed

Caused by:

{:?}"#,
                    &key_expr,
                    e
                )
            })
    }

    /// This methods attempts, twice, to allocate memory leveraging Zenoh's [SharedMemoryManager].
    ///
    /// # Errors
    ///
    /// If the first call fails, we wait for `backoff` nanoseconds (as configured) and then perform (i) a garbage
    /// collection followed by (ii) a defragmentation. Once these two operations have finished, we try once more to
    /// allocate memory.
    ///
    /// If it fails again, we return the error.
    pub(crate) async fn try_allocate_buffer(&mut self, size: usize) -> Result<SharedMemoryBuf> {
        if let Ok(buffer) = self.try_alloc(size) {
            return Ok(buffer);
        }
        tracing::trace!(
            "shared memory: backing off for {} nanoseconds",
            self.configuration.backoff
        );
        async_std::task::sleep(std::time::Duration::from_nanos(self.configuration.backoff)).await;

        tracing::trace!(
            "shared memory: garbage collect recovered {} bytes",
            self.manager.garbage_collect()
        );
        tracing::trace!(
            "shared memory: defragmented {} bytes",
            self.manager.defragment()
        );

        self.try_alloc(size)
    }

    // Utility method that logs the error if the shared memory manager failed to allocate and converts the error
    // into one that is "compatible" with `anyhow`.
    fn try_alloc(&mut self, size: usize) -> Result<SharedMemoryBuf> {
        let buffer = self.manager.alloc(size);

        buffer.map_err(|e| {
            tracing::trace!(
                r#"
shared memory: allocation of {} bytes failed

Caused by:
{:?}
"#,
                size,
                e
            );
            anyhow!("{:?}", e)
        })
    }
}
