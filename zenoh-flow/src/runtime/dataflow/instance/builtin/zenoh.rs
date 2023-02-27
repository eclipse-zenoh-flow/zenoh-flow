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


use async_trait::async_trait;
use flume::{Receiver, RecvError};
use std::sync::Arc;
use zenoh::{prelude::r#async::*, subscriber::Subscriber};
use crate::{bail, Result as ZFResult, prelude::{Source, Node, Context, Configuration, Outputs, OutputRaw, zferror, ErrorKind}};
use std::collections::HashMap;
use futures::future::select_all;
// use async_std::sync::Mutex;


pub struct ZenohSource<'a> {
    _session: Arc<Session>,
    outputs: HashMap<String,OutputRaw>,
    subscribers: HashMap<String, Subscriber<'a, Receiver<Sample>>>,
    // remaining : Arc<Mutex<Vec<RecvFut<'a, Sample>>>>,
}


#[async_trait]
impl<'a> Source for ZenohSource<'a> {
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        mut outputs: Outputs,
    ) -> ZFResult<Self> {


        let mut source_outputs : HashMap<String, OutputRaw> = HashMap::new();
        let mut subscribers : HashMap<String, Subscriber<'a, Receiver<Sample>>> = HashMap::new();

        match configuration {
            Some(configuration) => {
                // let mut config : HashMap<String, String> = HashMap::new();
                let configuration = configuration.as_object().ok_or(zferror!(ErrorKind::ConfigurationError, "Unable to convert configuration to HashMap: {:?}", configuration))?;

                for (id, value) in configuration {
                    let ke = value.as_str().ok_or(zferror!(ErrorKind::ConfigurationError, "Unable to value to string: {:?}", value))?.to_string();

                    let output = outputs.take_raw(id).ok_or(zferror!(ErrorKind::MissingOutput(id.clone()), "Unable to find output: {id}"))?;
                    let subscriber = context.zenoh_session().declare_subscriber(&ke).res().await?;

                    subscribers.insert(id.clone(), subscriber);
                    source_outputs.insert(id.clone(), output);

                }

                Ok(ZenohSource {
                    _session: context.zenoh_session(),
                    outputs: source_outputs,
                    subscribers,
                    // remaining: Arc::new(Mutex::new(Vec::new())),
                })

            },
            None => {
                bail!(ErrorKind::MissingConfiguration, "Builtin Zenoh source needs a configuration!")
            }
        }
    }
}

#[async_trait]
impl<'a> Node for ZenohSource<'a> {
    async fn iteration(&self) -> ZFResult<()> {


        async fn wait_zenoh_input<'a>(id : String, sub: &'a Subscriber<'a, Receiver<Sample>>) -> (String, Result<Sample, RecvError>) {
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
                log::trace!("[ZenohSource] Received data from {ke:?} Len: {} for output: {id}", data.len());
                let output = self.outputs.get(&id).ok_or(zferror!(ErrorKind::MissingOutput(id), "Unable to find output!"))?;
                output.send(data, None).await?;

            },
            (_,Err(e)) => log::error!("[ZenohSource] got error from Zenoh {e:?}"),
        }

        // *self.remaining.lock().await = remaining;

        Ok(())

    }
}