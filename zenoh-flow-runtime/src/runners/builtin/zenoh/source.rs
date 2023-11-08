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

use anyhow::{anyhow, Context as ac};
use async_std::sync::Mutex;
use futures::future::select_all;
use futures::Future;
use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};
use zenoh::prelude::r#async::*;
use zenoh::sample::Sample;
use zenoh::{subscriber::FlumeSubscriber, Session};
use zenoh_flow_commons::{NodeId, PortId, Result};
use zenoh_flow_nodes::prelude::OutputRaw;
use zenoh_flow_nodes::prelude::{Node, Outputs};

/// Internal type of pending futures for the ZenohSource
pub(crate) type ZSubFut = Pin<Box<dyn Future<Output = (PortId, Result<Sample>)> + Send + Sync>>;

fn wait_zenoh_sub(id: PortId, sub: &FlumeSubscriber<'_>) -> ZSubFut {
    let sub = sub.receiver.clone();
    Box::pin(async move { (id, sub.recv_async().await.map_err(|e| e.into())) })
}

pub(crate) struct ZenohSource<'a> {
    id: NodeId,
    outputs: HashMap<PortId, OutputRaw>,
    subscribers: HashMap<PortId, FlumeSubscriber<'a>>,
    futs: Arc<Mutex<Vec<ZSubFut>>>,
}

impl<'a> ZenohSource<'a> {
    pub(crate) async fn try_new(
        id: &NodeId,
        session: Arc<Session>,
        key_exprs: &HashMap<PortId, OwnedKeyExpr>,
        mut outputs: Outputs,
    ) -> Result<ZenohSource<'a>> {
        let mut raw_outputs = HashMap::with_capacity(key_exprs.len());
        let mut subscribers = HashMap::with_capacity(key_exprs.len());

        for (port, key_expr) in key_exprs.iter() {
            raw_outputs.insert(
                port.clone(),
                outputs
                    .take(port.as_ref())
                    .context(
                        r#"
Zenoh-Flow encountered a fatal internal error.

The Zenoh built-in Source < {} > wants to subscribe to the key expression < {} > but
no channel for this key expression was created.
"#,
                    )?
                    .raw(),
            );

            subscribers.insert(
                port.clone(),
                session
                    .declare_subscriber(key_expr)
                    .res()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            r#"
Zenoh-Flow encountered a fatal internal error.

Zenoh failed to declare a subscriber on < {} > for the Zenoh built-in Source < {} >.
Caused by:

{:?}"#,
                            key_expr,
                            id,
                            e
                        )
                    })?,
            );
        }

        let futs = subscribers
            .iter()
            .map(|(id, sub)| wait_zenoh_sub(id.clone(), sub))
            .collect::<Vec<_>>();

        Ok(Self {
            id: id.clone(),
            outputs: raw_outputs,
            subscribers,
            futs: Arc::new(Mutex::new(futs)),
        })
    }
}

#[async_trait::async_trait]
impl<'a> Node for ZenohSource<'a> {
    async fn iteration(&self) -> Result<()> {
        // Getting the list of futures to poll in a temporary variable (that `select_all` can take
        // ownership of)
        let mut futs = self.futs.lock().await;
        let tmp = std::mem::take(&mut (*futs));

        let ((id, result), _index, mut remaining) = select_all(tmp).await;

        match result {
            Ok(sample) => {
                let data = sample.payload.contiguous().to_vec();
                let ke = sample.key_expr;
                tracing::trace!(
                    "[ZenohSource] Received data from {ke:?} Len: {} for output: {id}",
                    data.len()
                );
                let output = self.outputs.get(&id).ok_or(anyhow!(
                    "[{}] Built-in Zenoh source, unable to find output < {} >",
                    self.id,
                    id
                ))?;
                output.send(data, None).await?;
            }
            Err(e) => tracing::error!("[ZenohSource] got a Zenoh error from output {id} : {e:?}"),
        }

        // Add back the subscriber that got polled
        let sub = self
            .subscribers
            .get(&id)
            .ok_or_else(|| anyhow!("[{}] Cannot find port < {} >", self.id, id))?;

        let next_sub = sub.receiver.clone();
        let source_id = self.id.clone();
        remaining.push(Box::pin(async move {
            (
                id.clone(),
                next_sub.recv_async().await.context(format!(
                    "[{}] Zenoh subscriber < {} > failed",
                    source_id, id
                )),
            )
        }));

        // Setting back a complete list for the next iteration
        *futs = remaining;

        Ok(())
    }
}
