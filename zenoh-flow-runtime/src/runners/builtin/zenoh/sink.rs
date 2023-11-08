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

use crate::shared_memory::SharedMemory;
use anyhow::{anyhow, Context};
use async_std::sync::Mutex;
use futures::{future::select_all, Future};
use std::{collections::HashMap, pin::Pin, sync::Arc};
use zenoh::{prelude::r#async::*, publication::Publisher};
use zenoh_flow_commons::{NodeId, PortId, Result, SharedMemoryConfiguration};
use zenoh_flow_nodes::prelude::{InputRaw, Inputs, LinkMessage, Node};

/// Internal type of pending futures for the ZenohSink
type ZFInputFut = Pin<Box<dyn Future<Output = (PortId, Result<LinkMessage>)> + Send + Sync>>;

fn wait_flow_input(id: PortId, input: &InputRaw) -> ZFInputFut {
    let input = input.clone();
    Box::pin(async move { (id, input.recv().await) })
}

/// The builtin Zenoh Sink
/// It can publish to multiple KEs and can have multiple outputs
/// It expects a configuration in the format
///
/// <input_id> : <key expression>
/// <input_id> : <key expression>
///
/// It expects the input(s) defined in the configuration to be connected.
pub(crate) struct ZenohSink<'a> {
    id: NodeId,
    inputs: HashMap<PortId, InputRaw>,
    publishers: HashMap<PortId, Publisher<'a>>,
    key_exprs: HashMap<PortId, OwnedKeyExpr>,
    state: Arc<Mutex<State>>,
}

/// The ZenohSinkState stores in a single structure all the fields protected by a lock.
///
/// The fields are:
/// - `futs` contains the pending futures waiting for inputs on their channel;
/// - `buffer` holds a growable vector of bytes in which the result of the serialization of the data
///   is stored.
struct State {
    pub(crate) futs: Vec<ZFInputFut>,
    pub(crate) payload_buffer: Vec<u8>,
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
        shm_configuration: &SharedMemoryConfiguration,
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

        let shm = SharedMemory::new(&id, session.clone(), shm_configuration);

        Ok(Self {
            id,
            inputs: raw_inputs,
            publishers,
            key_exprs: key_exprs.clone(),
            state: Arc::new(Mutex::new(State {
                shm,
                futs,
                payload_buffer: Vec::new(),
            })),
        })
    }
}

#[async_trait::async_trait]
impl<'a> Node for ZenohSink<'a> {
    async fn iteration(&self) -> Result<()> {
        // Getting the list of futures to poll in a temporary variable (that `select_all` can take ownership of)
        let mut state = self.state.lock().await;
        let tmp = std::mem::take(&mut state.futs);
        let mut payload_buffer = std::mem::take(&mut state.payload_buffer);

        let ((id, message), _index, mut remaining) = select_all(tmp).await;

        let (key_expr, input, publisher) = self.get(&id);

        match message {
            Ok(LinkMessage::Data(data)) => {
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
            Ok(_) => (), // Not the right message, ignore it.
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
