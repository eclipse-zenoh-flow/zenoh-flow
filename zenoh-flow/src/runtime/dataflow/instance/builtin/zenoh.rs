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

use crate::{
    bail,
    model::descriptor::{SinkDescriptor, SourceDescriptor},
    prelude::{
        zferror, Configuration, Context, ErrorKind, InputRaw, Inputs, Node, OutputRaw, Outputs,
        PortId, Sink, Source,
    },
    runtime::dataflow::{
        loader::{NodeDeclaration, CORE_VERSION, RUSTC_VERSION},
        node::{SinkFn, SourceFn},
    },
    types::LinkMessage,
    Result as ZFResult,
};
use async_std::sync::Mutex;
use async_trait::async_trait;
use flume::{Receiver, RecvError};
use futures::{future::select_all, Future};
use std::mem;
use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};
use zenoh::{
    prelude::r#async::*, publication::Publisher, shm::SharedMemoryManager, subscriber::Subscriber,
};

/// Key for the key expressions used by the built-in Source/Sink.
static KEY_KEYEXPRESSIONS: &str = "key-expressions";

/// Key for the shared memory element size used by the built-in Sink.
static KEY_SHM_ELEM_SIZE: &str = "shared_memory_element_size";

/// Key for the number of shared memory element of size KEY_SHM_ELEM_SIZE
///  used by the built-in Sink.
static KEY_SHM_TOTAL_ELEMENTS: &str = "shared_memory_elements";

/// Key for the backoff time to be used when no shared memory element are available.
static KEY_SHM_BACKOFF: &str = "shared_memory_backoff";

/// Internal type of pending futures for the ZenohSource
pub(crate) type ZSubFut =
    Pin<Box<dyn Future<Output = (PortId, Result<Sample, RecvError>)> + Send + Sync>>;

fn wait_zenoh_sub(id: PortId, sub: &Subscriber<Receiver<Sample>>) -> ZSubFut {
    let sub = sub.receiver.clone();
    Box::pin(async move { (id, sub.recv_async().await) })
}

/// The builtin Zenoh Source
/// It can subscribe to multiple KEs and can have multiple outputs
/// It expects a configuration in the format
///
/// <output_id> : <key expression>
/// <output_id> : <key expression>
///
/// It expects the output(s) defined in the configuration to be connected.
pub(crate) struct ZenohSource<'a> {
    _session: Arc<Session>,
    outputs: HashMap<PortId, OutputRaw>,
    subscribers: HashMap<PortId, Subscriber<'a, Receiver<Sample>>>,
    futs: Arc<Mutex<Vec<ZSubFut>>>,
}

/// Private function to retrieve the "Constructor" for the ZenohSource
pub(crate) fn get_zenoh_source_declaration() -> NodeDeclaration<SourceFn> {
    NodeDeclaration::<SourceFn> {
        rustc_version: RUSTC_VERSION,
        core_version: CORE_VERSION,
        constructor: |context: Context, configuration: Option<Configuration>, outputs: Outputs| {
            Box::pin(async {
                let node = ZenohSource::new(context, configuration, outputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    }
}

/// Private function to retrieve the Descriptor for the ZenohSource
pub(crate) fn get_zenoh_source_descriptor(
    configuration: &Configuration,
) -> ZFResult<SourceDescriptor> {
    let mut outputs = vec![];
    let local_configuration = configuration.as_object().ok_or_else(|| {
        zferror!(
            ErrorKind::ConfigurationError,
            "Unable to convert configuration to HashMap: {:?}",
            configuration
        )
    })?;

    let keyexpressions = local_configuration.get(KEY_KEYEXPRESSIONS).ok_or_else(|| {
        zferror!(
            ErrorKind::ConfigurationError,
            "Missing key-expressions in builtin sink configuration"
        )
    })?;

    let keyexpressions = keyexpressions.as_object().ok_or_else(|| {
        zferror!(
            ErrorKind::ConfigurationError,
            "Unable to convert configuration to HashMap: {:?}",
            configuration
        )
    })?;

    for id in keyexpressions.keys() {
        outputs.push(id.clone().into());
    }

    Ok(SourceDescriptor {
        id: "zenoh-source".into(),
        outputs,
        uri: Some("builtin://zenoh".to_string()),
        configuration: Some(configuration.clone()),
    })
}

#[async_trait]
impl<'a> Source for ZenohSource<'a> {
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        mut outputs: Outputs,
    ) -> ZFResult<Self> {
        let mut source_outputs: HashMap<PortId, OutputRaw> = HashMap::new();
        let mut subscribers: HashMap<PortId, Subscriber<'a, Receiver<Sample>>> = HashMap::new();

        match configuration {
            Some(configuration) => {
                let keyexpressions = configuration.get(KEY_KEYEXPRESSIONS).ok_or_else(|| {
                    zferror!(
                        ErrorKind::ConfigurationError,
                        "Missing key-expressions in builtin sink configuration"
                    )
                })?;

                let keyexpressions = keyexpressions.as_object().ok_or_else(|| {
                    zferror!(
                        ErrorKind::ConfigurationError,
                        "Unable to convert configuration to HashMap: {:?}",
                        configuration
                    )
                })?;

                for (id, value) in keyexpressions {
                    let ke = value
                        .as_str()
                        .ok_or_else(|| {
                            zferror!(
                                ErrorKind::ConfigurationError,
                                "Unable to convert value to string: {:?}",
                                value
                            )
                        })?
                        .to_string();

                    let output = outputs
                        .take(id)
                        .ok_or(zferror!(
                            ErrorKind::MissingOutput(id.clone()),
                            "Unable to find output: {id}"
                        ))?
                        .raw();
                    let subscriber = context
                        .zenoh_session()
                        .declare_subscriber(&ke)
                        .res()
                        .await?;

                    subscribers.insert(id.clone().into(), subscriber);
                    source_outputs.insert(id.clone().into(), output);
                }

                let futs = subscribers
                    .iter()
                    .map(|(id, sub)| wait_zenoh_sub(id.clone(), sub))
                    .collect();

                Ok(ZenohSource {
                    _session: context.zenoh_session(),
                    outputs: source_outputs,
                    subscribers,
                    futs: Arc::new(Mutex::new(futs)),
                })
            }
            None => {
                bail!(
                    ErrorKind::MissingConfiguration,
                    "Builtin ZenohSource needs a configuration!"
                )
            }
        }
    }
}

#[async_trait]
impl<'a> Node for ZenohSource<'a> {
    async fn iteration(&self) -> ZFResult<()> {
        // Getting the list of futures to poll in a temporary variable (that `select_all` can take
        // ownership of)
        let mut futs = self.futs.lock().await;
        let tmp = mem::take(&mut (*futs));

        let ((id, result), _index, mut remaining) = select_all(tmp).await;

        match result {
            Ok(sample) => {
                let data = sample.payload.contiguous().to_vec();
                let ke = sample.key_expr;
                log::trace!(
                    "[ZenohSource] Received data from {ke:?} Len: {} for output: {id}",
                    data.len()
                );
                let output = self.outputs.get(&id).ok_or(zferror!(
                    ErrorKind::MissingOutput(id.to_string()),
                    "Unable to find output!"
                ))?;
                output.send(data, None).await?;
            }
            Err(e) => log::error!("[ZenohSource] got a Zenoh error from output {id} : {e:?}"),
        }

        // Add back the subscriber that got polled
        let sub = self.subscribers.get(&id).ok_or_else(|| {
            zferror!(
                ErrorKind::RecvError,
                "Unable to find < {id} > for built-in Zenoh Source"
            )
        })?;
        remaining.push(wait_zenoh_sub(id, sub));

        // Setting back a complete list for the next iteration
        *futs = remaining;

        Ok(())
    }
}

/// Internal type of pending futures for the ZenohSink
pub(crate) type ZFInputFut =
    Pin<Box<dyn Future<Output = (PortId, ZFResult<LinkMessage>)> + Send + Sync>>;

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
    _session: Arc<Session>,
    inputs: HashMap<PortId, InputRaw>,
    publishers: HashMap<PortId, Publisher<'a>>,
    state: Arc<Mutex<ZenohSinkState>>,
    shm_element_size: usize,
    shm_backoff: u64,
}

/// The ZenohSinkState stores in a single structure all the fields protected by a lock.
///
/// The fields are:
/// - `futs` contains the pending futures waiting for inputs on their channel;
/// - `shm` holds the [SharedMemoryManager] used to send data through Zenoh's shared memory;
/// - `buffer` holds a growable vector of bytes in which the result of the serialization of the data
///   is stored.
pub(crate) struct ZenohSinkState {
    pub(crate) futs: Vec<ZFInputFut>,
    pub(crate) shm: Option<SharedMemoryManager>,
    pub(crate) buffer: Vec<u8>,
}

/// Private function to retrieve the "Constructor" for the ZenohSink
pub(crate) fn get_zenoh_sink_declaration() -> NodeDeclaration<SinkFn> {
    NodeDeclaration::<SinkFn> {
        rustc_version: RUSTC_VERSION,
        core_version: CORE_VERSION,
        constructor: |context: Context, configuration: Option<Configuration>, inputs: Inputs| {
            Box::pin(async {
                let node = ZenohSink::new(context, configuration, inputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    }
}

/// Private function to retrieve the Descriptor for the ZenohSink
pub(crate) fn get_zenoh_sink_descriptor(configuration: &Configuration) -> ZFResult<SinkDescriptor> {
    let mut inputs = vec![];
    let local_configuration = configuration.as_object().ok_or_else(|| {
        zferror!(
            ErrorKind::ConfigurationError,
            "Unable to convert configuration to HashMap: {:?}",
            configuration
        )
    })?;

    let keyexpressions = local_configuration.get(KEY_KEYEXPRESSIONS).ok_or_else(|| {
        zferror!(
            ErrorKind::ConfigurationError,
            "Missing key-expressions in builtin sink configuration"
        )
    })?;

    let keyexpressions = keyexpressions.as_object().ok_or_else(|| {
        zferror!(
            ErrorKind::ConfigurationError,
            "Unable to convert configuration to HashMap: {:?}",
            configuration
        )
    })?;

    for id in keyexpressions.keys() {
        inputs.push(id.clone().into());
    }

    Ok(SinkDescriptor {
        id: "zenoh-sink".into(),
        inputs,
        uri: Some("builtin://zenoh".to_string()),
        configuration: Some(configuration.clone()),
    })
}

#[async_trait]
impl<'a> Sink for ZenohSink<'a> {
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        mut inputs: Inputs,
    ) -> ZFResult<Self> {
        let mut sink_inputs: HashMap<PortId, InputRaw> = HashMap::new();
        let mut publishers: HashMap<PortId, Publisher<'a>> = HashMap::new();

        let id = uuid::Uuid::new_v4().to_string();
        match configuration {
            Some(configuration) => {
                let get_or_default = |configuration: &serde_json::Value, key, default| {
                    if let Some(value) = configuration.get(key) {
                        if let Some(value) = value.as_u64() {
                            return value;
                        }
                        log::warn!("Failed to parse / interpret provided value for {key} into u64, using default value: {default}");
                    }

                    default
                };

                let mut shm_element_size = 0;
                let mut shm_backoff = 0;
                let mut shm_manager = None;

                if *context.shared_memory_enabled() {
                    shm_element_size = get_or_default(
                        &configuration,
                        KEY_SHM_ELEM_SIZE,
                        *context.shared_memory_element_size() as u64,
                    ) as usize;
                    let shm_elem_count = get_or_default(
                        &configuration,
                        KEY_SHM_TOTAL_ELEMENTS,
                        *context.shared_memory_elements() as u64,
                    ) as usize;
                    shm_backoff = get_or_default(
                        &configuration,
                        KEY_SHM_BACKOFF,
                        *context.shared_memory_backoff(),
                    );

                    let shm_size = shm_element_size * shm_elem_count;

                    shm_manager = Some(SharedMemoryManager::make(id, shm_size).map_err(|_| {
                        zferror!(
                            ErrorKind::ConfigurationError,
                            "Unable to allocate {shm_size} bytes of shared memory"
                        )
                    })?);
                }

                let keyexpressions = configuration.get(KEY_KEYEXPRESSIONS).ok_or_else(|| {
                    zferror!(
                        ErrorKind::ConfigurationError,
                        "Missing key-expressions in builtin sink configuration"
                    )
                })?;

                let keyexpressions = keyexpressions.as_object().ok_or_else(|| {
                    zferror!(
                        ErrorKind::ConfigurationError,
                        "Unable to convert configuration to HashMap: {:?}",
                        configuration
                    )
                })?;

                for (id, value) in keyexpressions {
                    let ke = value
                        .as_str()
                        .ok_or_else(|| {
                            zferror!(
                                ErrorKind::ConfigurationError,
                                "Unable to convert value to string: {:?}",
                                value
                            )
                        })?
                        .to_string();

                    let input = inputs
                        .take(id)
                        .ok_or(zferror!(
                            ErrorKind::MissingInput(id.clone()),
                            "Unable to find input: {id}"
                        ))?
                        .raw();
                    let subscriber = context.zenoh_session().declare_publisher(ke).res().await?;

                    publishers.insert(id.clone().into(), subscriber);
                    sink_inputs.insert(id.clone().into(), input);
                }

                let futs = sink_inputs
                    .iter()
                    .map(|(id, input)| wait_flow_input(id.clone(), input))
                    .collect();

                Ok(ZenohSink {
                    _session: context.zenoh_session(),
                    inputs: sink_inputs,
                    publishers,
                    state: Arc::new(Mutex::new(ZenohSinkState {
                        futs,
                        shm: shm_manager,
                        buffer: Vec::new(),
                    })),
                    shm_element_size,
                    shm_backoff,
                })
            }
            None => {
                bail!(
                    ErrorKind::MissingConfiguration,
                    "Builtin ZenohSink needs a configuration!"
                )
            }
        }
    }
}

#[async_trait]
impl<'a> Node for ZenohSink<'a> {
    async fn iteration(&self) -> ZFResult<()> {
        // Getting the list of futures to poll in a temporary variable (that `select_all` can take
        // ownership of)
        let mut state = self.state.lock().await;
        let tmp = mem::take(&mut state.futs);

        let ((id, result), _index, mut remaining) = select_all(tmp).await;

        match result {
            Ok(LinkMessage::Data(dm)) => {
                // Getting serialized data
                dm.try_as_bytes_into(&mut state.buffer)?;

                // Getting publisher
                let publisher = self.publishers.get(&id).ok_or_else(|| {
                    zferror!(ErrorKind::SendError, "Unable to find Publisher for {id}")
                })?;

                match state.shm {
                    Some(ref mut shm) => {
                        // Getting shared memory manager
                        let mut buff = match shm.alloc(self.shm_element_size) {
                            Ok(buf) => buf,
                            Err(_) => {
                                async_std::task::sleep(std::time::Duration::from_nanos(
                                    self.shm_backoff,
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

                        let data_len = state.buffer.len();

                        // If the shared memory block is big enough we send the data through it,
                        // otherwise we go through the network.
                        if data_len < slice.len() {
                            // WARNING ACHTUNG ATTENTION
                            // We are coping memory here!
                            // we should be able to serialize directly in the shared
                            // memory buffer.
                            slice[0..data_len].copy_from_slice(&state.buffer);
                            publisher.put(buff).res().await?;
                        } else {
                            log::warn!("[ZenohSink] Sending data via network as we are unable to send it over shared memory, the serialized size is {} while shared memory is {}", data_len, self.shm_element_size);
                            publisher.put(state.buffer.as_slice()).res().await?;
                        }
                    }
                    None => publisher.put(state.buffer.as_slice()).res().await?,
                };
            }
            Ok(_) => (), // Not the right message, ignore it.
            Err(e) => log::error!("[ZenohSink] got error on link {id}: {e:?}"),
        }

        // Add back the input that got polled
        let input = self.inputs.get(&id).ok_or_else(|| {
            zferror!(
                ErrorKind::RecvError,
                "Unable to find input < {id} > for built-in Zenoh Sink"
            )
        })?;
        remaining.push(wait_flow_input(id, input));

        // Set back the complete list for the next iteration
        state.futs = remaining;

        Ok(())
    }
}

#[cfg(test)]
#[path = "./tests/builtin-zenoh.rs"]
mod tests;
