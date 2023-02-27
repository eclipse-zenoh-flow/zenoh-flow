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
    prelude::{
        zferror, Configuration, Context, ErrorKind, InputRaw, Inputs, Node, OutputRaw, Outputs,
        PortId, Sink, Source,
    },
    types::LinkMessage,
    Result as ZFResult,
};
use async_trait::async_trait;
use flume::{Receiver, RecvError};
use futures::future::select_all;
use std::collections::HashMap;
use std::sync::Arc;
use zenoh::{prelude::r#async::*, publication::Publisher, subscriber::Subscriber};
// use async_std::sync::Mutex;

// Source

pub struct ZenohSource<'a> {
    _session: Arc<Session>,
    outputs: HashMap<PortId, OutputRaw>,
    subscribers: HashMap<PortId, Subscriber<'a, Receiver<Sample>>>,
    // remaining : Arc<Mutex<Vec<RecvFut<'a, Sample>>>>,
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
                // let mut config : HashMap<String, String> = HashMap::new();
                let configuration = configuration.as_object().ok_or(zferror!(
                    ErrorKind::ConfigurationError,
                    "Unable to convert configuration to HashMap: {:?}",
                    configuration
                ))?;

                for (id, value) in configuration {
                    let ke = value
                        .as_str()
                        .ok_or(zferror!(
                            ErrorKind::ConfigurationError,
                            "Unable to value to string: {:?}",
                            value
                        ))?
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

                Ok(ZenohSource {
                    _session: context.zenoh_session(),
                    outputs: source_outputs,
                    subscribers,
                    // remaining: Arc::new(Mutex::new(Vec::new())),
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
        async fn wait_zenoh_input<'a>(
            id: PortId,
            sub: &'a Subscriber<'a, Receiver<Sample>>,
        ) -> (PortId, Result<Sample, RecvError>) {
            (id, sub.recv_async().await)
        }

        let mut futs = vec![];

        for s in &self.subscribers {
            let (id, sub) = s;
            futs.push(Box::pin(wait_zenoh_input(id.clone(), sub)))
        }

        // TODO:
        // Let's get the first one that finished and send the result to the
        // corresponding output.
        // The ones that did not finish are stored and used in next
        // iteration.

        let (done, _index, _remaining) = select_all(futs).await;

        match done {
            (id, Ok(sample)) => {
                let data = sample.payload.contiguous().to_vec();
                let ke = sample.key_expr;
                log::trace!(
                    "[ZenohSource] Received data from {ke:?} Len: {} for output: {id}",
                    data.len()
                );
                let output = self.outputs.get(&id).ok_or(zferror!(
                    ErrorKind::MissingOutput(id.to_string()),
                    "Unable convert to find output!"
                ))?;
                output.send(data, None).await?;
            }
            (_, Err(e)) => log::error!("[ZenohSource] got error from Zenoh {e:?}"),
        }

        // *self.remaining.lock().await = remaining;

        Ok(())
    }
}

// Sink

pub struct ZenohSink<'a> {
    _session: Arc<Session>,
    inputs: HashMap<PortId, InputRaw>,
    publishers: HashMap<PortId, Publisher<'a>>,
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

        match configuration {
            Some(configuration) => {
                // let mut config : HashMap<String, String> = HashMap::new();
                let configuration = configuration.as_object().ok_or(zferror!(
                    ErrorKind::ConfigurationError,
                    "Unable to convert configuration to HashMap: {:?}",
                    configuration
                ))?;

                for (id, value) in configuration {
                    let ke = value
                        .as_str()
                        .ok_or(zferror!(
                            ErrorKind::ConfigurationError,
                            "Unable to convert value to string: {:?}",
                            value
                        ))?
                        .to_string();

                    let input = inputs.take_raw(id).ok_or(zferror!(
                        ErrorKind::MissingOutput(id.clone()),
                        "Unable to find output: {id}"
                    ))?;
                    let subscriber = context.zenoh_session().declare_publisher(ke).res().await?;

                    publishers.insert(id.clone().into(), subscriber);
                    sink_inputs.insert(id.clone().into(), input);
                }

                Ok(ZenohSink {
                    _session: context.zenoh_session(),
                    inputs: sink_inputs,
                    publishers,
                    // remaining: Arc::new(Mutex::new(Vec::new())),
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
        let mut futs = vec![];

        async fn wait_flow_input(id: PortId, input: &InputRaw) -> (PortId, ZFResult<LinkMessage>) {
            (id, input.recv().await)
        }

        for i in &self.inputs {
            let (id, input) = i;
            futs.push(Box::pin(wait_flow_input(id.clone(), input)))
        }

        // TODO:
        // Let's get the first one that finished and send the result to the
        // corresponding output.
        // The ones that did not finish are stored and used in next
        // iteration.

        let (done, _index, _remaining) = select_all(futs).await;

        match done {
            (id, Ok(LinkMessage::Data(mut dm))) => {
                let data = dm.get_inner_data().try_as_bytes()?;
                let publisher = self.publishers.get(&id).ok_or(zferror!(
                    ErrorKind::SendError,
                    "Unable to find Publisher for {id}"
                ))?;
                publisher.put(&**data).res().await?;
            }
            (_, Ok(_)) => (), // Not the right message, ignore it.
            (id, Err(e)) => log::error!("[ZenohSink] got error on link {id}: {e:?}"),
        }

        // *self.remaining.lock().await = remaining;

        Ok(())
    }
}
