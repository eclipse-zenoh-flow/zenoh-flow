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

#[cfg(feature = "shared-memory")]
use crate::shared_memory::SharedMemory;
use anyhow::{anyhow, Context};
use async_std::sync::Mutex;
use futures::{future::select_all, Future};
use std::{collections::HashMap, pin::Pin, sync::Arc};
use zenoh::{prelude::r#async::*, publication::Publisher};
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;
use zenoh_flow_commons::{NodeId, PortId, Result};
use zenoh_flow_nodes::prelude::{InputRaw, Inputs, LinkMessage, Node};

/// Internal type of pending futures for the ZenohSink
type ZFInputFut = Pin<Box<dyn Future<Output = (PortId, Result<LinkMessage>)> + Send + Sync>>;

fn wait_flow_input(id: PortId, input: &InputRaw) -> ZFInputFut {
    let input = input.clone();
    Box::pin(async move { (id, input.recv().await) })
}

/// TODO
pub(crate) struct ZenohSink<'a> {
    id: NodeId,
    inputs: HashMap<PortId, InputRaw>,
    publishers: HashMap<PortId, Publisher<'a>>,
    key_exprs: HashMap<PortId, OwnedKeyExpr>,
    state: Arc<Mutex<State>>,
}

/// Structure grouping the fields that need interior mutability.
struct State {
    pub(crate) futs: Vec<ZFInputFut>,
    pub(crate) payload_buffer: Vec<u8>,
    #[cfg(feature = "shared-memory")]
    pub(crate) shm: SharedMemory,
}

impl<'a> ZenohSink<'a> {
    pub(crate) fn get(&self, port: &PortId) -> (&OwnedKeyExpr, &InputRaw, &Publisher<'a>) {
        let key_expr = self.key_exprs.get(port).unwrap();
        let input_raw = self.inputs.get(port).unwrap();
        let publisher = self.publishers.get(port).unwrap();

        (key_expr, input_raw, publisher)
    }

    pub(crate) async fn try_new(
        id: NodeId,
        session: Arc<Session>,
        key_exprs: &HashMap<PortId, OwnedKeyExpr>,
        #[cfg(feature = "shared-memory")] shm_configuration: &SharedMemoryConfiguration,
        mut inputs: Inputs,
    ) -> Result<ZenohSink<'a>> {
        let mut raw_inputs = HashMap::with_capacity(key_exprs.len());
        let mut publishers = HashMap::with_capacity(key_exprs.len());

        for (port, key_expr) in key_exprs.clone().into_iter() {
            raw_inputs.insert(
                port.clone(),
                inputs
                    .take(port.as_ref())
                    .context(format!(
                        r#"
[built-in zenoh sink: {}][port: {}] Zenoh-Flow encountered a fatal internal error.
No Input was created for port: < {1} > (key expression: {}).
"#,
                        id, port, key_expr,
                    ))?
                    .raw(),
            );

            publishers.insert(
                port.clone(),
                session
                    .declare_publisher(key_expr.clone())
                    .res()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            r#"
[built-in zenoh sink: {}][port: {}] Zenoh-Flow encountered a fatal internal error.
Zenoh failed to declare a publisher on < {} >.
Caused by:

{:?}"#,
                            id,
                            port,
                            key_expr,
                            e
                        )
                    })?,
            );
        }

        let futs: Vec<_> = raw_inputs
            .iter()
            .map(|(id, input)| wait_flow_input(id.clone(), input))
            .collect();

        #[cfg(feature = "shared-memory")]
        let shm = SharedMemory::new(&id, session.clone(), shm_configuration);

        Ok(Self {
            id,
            inputs: raw_inputs,
            publishers,
            key_exprs: key_exprs.clone(),
            state: Arc::new(Mutex::new(State {
                #[cfg(feature = "shared-memory")]
                shm,
                futs,
                payload_buffer: Vec::new(),
            })),
        })
    }
}

#[async_trait::async_trait]
impl<'a> Node for ZenohSink<'a> {
    // If the node is aborted / paused while waiting for inputs, the execution will be interrupted while
    // `self.state.futs` is empty. If this happens then we need to fill again `futs`.
    async fn on_resume(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if state.futs.is_empty() {
            self.inputs
                .iter()
                .for_each(|(id, input)| state.futs.push(wait_flow_input(id.clone(), input)));
        }

        Ok(())
    }

    async fn iteration(&self) -> Result<()> {
        // Getting the list of futures to poll in a temporary variable (that `select_all` can take ownership of)
        let mut state = self.state.lock().await;
        let inputs = std::mem::take(&mut state.futs);
        let mut payload_buffer = std::mem::take(&mut state.payload_buffer);

        let ((id, message), _index, mut remaining) = select_all(inputs).await;

        let (key_expr, input, publisher) = self.get(&id);

        match message {
            Ok(data) => {
                // NOTE: In most of cases sending through the shared memory should suffice.
                //
                // This holds true EVEN IF THERE IS NO SHARED MEMORY. Zenoh will, by default, automatically fallback to
                // a "regular" put if there is no shared-memory channel.
                //
                // The only case where sending through it would fail is if it is impossible to allocate enough space in
                // the shared memory.
                //
                // This can happen if:
                // - not enough memory is allocated on the shared memory manager (data is bigger than the allocated
                //   memory),
                // - the memory is full (is there a slow subscriber? some congestion on the network?).
                #[cfg(feature = "shared-memory")]
                {
                    if let Err(e) = state
                        .shm
                        .try_send_payload(key_expr, data, &mut payload_buffer)
                        .await
                    {
                        tracing::warn!(
                            r#"
[built-in zenoh sink: {}][port: {}] Failed to send the data via Zenoh's shared memory.

Caused by:
{:?}
"#,
                            self.id,
                            key_expr,
                            e
                        );
                        tracing::warn!(
                        "[built-in zenoh sink: {}][port: {}] Attempting to send via a non-shared memory channel.",
                        self.id,
                        key_expr
                    );

                        publisher
                            .put(payload_buffer)
                            .res()
                            .await
                            .map_err(|e| anyhow!("{:?}", e))?
                    }
                }

                #[cfg(not(feature = "shared-memory"))]
                {
                    data.try_as_bytes_into(&mut payload_buffer)?;
                    publisher
                        .put(payload_buffer)
                        .res()
                        .await
                        .map_err(|e| anyhow!("{:?}", e))?
                }
            }
            Err(e) => tracing::error!(
                "[built-in zenoh sink: {}][port: {}] Channel returned an error: {e:?}",
                self.id,
                key_expr
            ),
        }

        remaining.push(wait_flow_input(id, input));

        // Set back the complete list for the next iteration
        state.futs = remaining;

        Ok(())
    }
}
