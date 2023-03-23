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
use zenoh::buffers::SharedMemoryManager;
use zenoh::{prelude::r#async::*, publication::Publisher, subscriber::Subscriber};

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
    for id in local_configuration.keys() {
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
                let keyexpressions = configuration.get(KEY_KEYEXPRESSIONS).ok_or(zferror!(
                    ErrorKind::ConfigurationError,
                    "Missing key-expressions in builtin sink configuration"
                ))?;

                let keyexpressions = keyexpressions.as_object().ok_or(zferror!(
                    ErrorKind::ConfigurationError,
                    "Unable to convert configuration to HashMap: {:?}",
                    configuration
                ))?;

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

                    let output = outputs.take_raw(id).ok_or(zferror!(
                        ErrorKind::MissingOutput(id.clone()),
                        "Unable to find output: {id}"
                    ))?;
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
    futs: Arc<Mutex<Vec<ZFInputFut>>>,
    shm: Arc<Mutex<SharedMemoryManager>>,
    shm_size: usize,
    shm_backoff: u64,
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
    for id in local_configuration.keys() {
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
                let shm_elem_size = configuration
                    .get(KEY_SHM_ELEM_SIZE)
                    .unwrap_or(&serde_json::Value::from(
                        *context.shared_memory_element_size() as u64,
                    ))
                    .as_u64()
                    .unwrap_or(*context.shared_memory_element_size() as u64)
                    as usize;

                let shm_elem_count = configuration
                    .get(KEY_SHM_TOTAL_ELEMENTS)
                    .unwrap_or(&serde_json::Value::from(
                        *context.shared_memory_elements() as u64
                    ))
                    .as_u64()
                    .unwrap_or(*context.shared_memory_elements() as u64)
                    as usize;

                let shm_size = shm_elem_size * shm_elem_count;

                let shm_backoff = configuration
                    .get(KEY_SHM_BACKOFF)
                    .unwrap_or(&serde_json::Value::from(*context.shared_memory_backoff()))
                    .as_u64()
                    .unwrap_or(*context.shared_memory_backoff());

                let keyexpressions = configuration.get(KEY_KEYEXPRESSIONS).ok_or(zferror!(
                    ErrorKind::ConfigurationError,
                    "Missing key-expressions in builtin sink configuration"
                ))?;

                let keyexpressions = keyexpressions.as_object().ok_or(zferror!(
                    ErrorKind::ConfigurationError,
                    "Unable to convert configuration to HashMap: {:?}",
                    configuration
                ))?;

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

                    let input = inputs.take_raw(id).ok_or(zferror!(
                        ErrorKind::MissingInput(id.clone()),
                        "Unable to find input: {id}"
                    ))?;
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
                    futs: Arc::new(Mutex::new(futs)),
                    shm: Arc::new(Mutex::new(
                        SharedMemoryManager::make(id, shm_size).map_err(|_| {
                            zferror!(
                                ErrorKind::ConfigurationError,
                                "Unable to allocate {shm_size} bytes of shared memory"
                            )
                        })?,
                    )),
                    shm_size: shm_elem_size,
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
        // Getting shared memory manager
        let mut shm = self.shm.lock().await;

        // Getting the list of futures to poll in a temporary variable (that `select_all` can take
        // ownership of)
        let mut futs = self.futs.lock().await;
        let tmp = mem::take(&mut (*futs));

        let ((id, result), _index, mut remaining) = select_all(tmp).await;

        match result {
            Ok(LinkMessage::Data(mut dm)) => {
                // Getting serialized data
                let data = dm.get_inner_data().try_as_bytes()?;

                // Getting publisher
                let publisher = self.publishers.get(&id).ok_or(zferror!(
                    ErrorKind::SendError,
                    "Unable to find Publisher for {id}"
                ))?;

                // Getting the shared memory buffer
                let mut buff = match shm.alloc(self.shm_size) {
                    Ok(buf) => buf,
                    Err(_) => {
                        async_std::task::sleep(std::time::Duration::from_millis(self.shm_backoff))
                            .await;
                        log::trace!(
                            "After failing allocation the GC collected: {} bytes -- retrying",
                        shm.alloc(self.shm_size).map_err(|_| {
                            zferror!(
                                ErrorKind::ConfigurationError,
                                "Unable to allocated {} in the shared memory buffer!",
                                self.shm_size
                            )
                        })?
                    }
                };

                // Getting the underlying slice in the shared memory
                let slice = unsafe { buff.as_mut_slice() };

                let data_len = data.len();

                // If the shared memory slice is big enough we
                if data_len < slice.len() {
                    // WARNING ACHTUNG ATTENTION
                    // We are coping memory here!
                    // we should be able to serialize directly in the shared
                    // memory buffer.
                    slice[0..data_len].copy_from_slice(&data);
                    // Sending the data as shared memory
                    publisher.put(buff).res().await?;
                } else {
                    publisher.put(&**data).res().await?;
                }
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
        *futs = remaining;

        Ok(())
    }
