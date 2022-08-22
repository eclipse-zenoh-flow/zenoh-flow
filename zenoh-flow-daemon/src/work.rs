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

use async_std::pin::Pin;
use async_std::stream::Stream;
use async_std::task::{Context, JoinHandle, Poll};
use flume::{unbounded, Receiver, Sender};
use futures::StreamExt;
use futures_lite::future::FutureExt;
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uhlc::Timestamp;
use uuid::Uuid;
use zenoh::{prelude::*, Session};
use zenoh_flow::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobKind {
    CreateInstance(FlattenDataFlowDescriptor),
    DeleteInstance(Uuid),
    Instantiate(FlattenDataFlowDescriptor),
    Teardown(Uuid),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStatus {
    Sumbitted(Timestamp),
    Started(Timestamp),
    Done(Timestamp),
    Failed(Timestamp, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    id: Uuid,
    job: JobKind,
    status: JobStatus,
    assignee: Option<u64>,
}

impl Job {
    pub fn new(hlc: &uhlc::HLC) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            job: JobKind::Teardown(uuid::Uuid::new_v4()),
            status: JobStatus::Sumbitted(hlc.new_timestamp()),
            assignee: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Worker {
    id: u64,
    incoming_jobs: Arc<Receiver<Job>>,
    outgoing_jobs: Arc<Sender<Job>>,
}

pin_project! {
    /// Custom stream to lister for Jobs.
    pub struct ZFJobStream {
        #[pin]
        sub: zenoh::subscriber::Subscriber<'static>,
    }
}

impl ZFJobStream {
    pub async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl Stream for ZFJobStream {
    type Item = Job;

    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match self.sub.next().poll(cx) {
            Poll::Ready(Some(sample)) => match sample.kind {
                SampleKind::Put | SampleKind::Patch => match sample.value.encoding {
                    Encoding::APP_OCTET_STREAM => {
                        match zenoh_flow::runtime::resources::deserialize_data::<Job>(
                            &sample.value.payload.contiguous(),
                        ) {
                            Ok(info) => Poll::Ready(Some(info)),
                            Err(_) => Poll::Ready(None),
                        }
                    }
                    _ => {
                        log::warn!(
                            "Received sample with wrong encoding {:?}, dropping",
                            sample.value.encoding
                        );
                        Poll::Ready(None)
                    }
                },
                SampleKind::Delete => {
                    log::warn!("Received delete sample drop it");
                    Poll::Ready(None)
                }
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Subscribes to the job queue of the given `rtid`
///
/// # Errors
/// An error variant is returned in case of:
/// - zenoh subscribe fails
/// - fails to deserialize
pub async fn subscribe_sumbitted_jobs(z: &Arc<Session>, rtid: &Uuid) -> Result<ZFJobStream> {
    let selector =
        zenoh_flow::JQ_SUMBITTED_SEL!(zenoh_flow::runtime::resources::ROOT_STANDALONE, rtid);
    Ok(z.subscribe(&selector)
        .await
        .map(|sub| ZFJobStream { sub })?)
}

impl std::fmt::Display for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl Worker {
    fn new(id: u64, rx: Receiver<Job>, tx: Sender<Job>) -> Self {
        Self {
            id,
            incoming_jobs: Arc::new(rx),
            outgoing_jobs: Arc::new(tx),
        }
    }

    async fn run(&self) {
        log::info!("Worker [{}]: Started", self.id);
        while let Ok(j) = self.incoming_jobs.recv_async().await {
            log::info!("Worker [{}]: Received Job {j:?}", self.id);
        }
    }
}

// Job Queue Related,

/// Submits the given [`Job`](`Job`) in the queue
///
/// # Errors
/// An error variant is returned in case of:
/// - fails to serialize
/// - zenoh put fails
pub async fn add_submitted_job(z: &Arc<Session>, rtid: &Uuid, job: &Job) -> Result<()> {
    let path = zenoh_flow::JQ_SUMBITTED_JOB!(
        zenoh_flow::runtime::resources::ROOT_STANDALONE,
        rtid,
        &job.id
    );
    let encoded_info = zenoh_flow::runtime::resources::serialize_data(job)?;
    z.put(&path, encoded_info).await
}

pub async fn del_submitted_job(z: &Arc<Session>, rtid: &Uuid, id: &Uuid) -> Result<()> {
    let path =
        zenoh_flow::JQ_SUMBITTED_JOB!(zenoh_flow::runtime::resources::ROOT_STANDALONE, rtid, id);
    z.delete(&path).await
}

pub struct WorkerPool {
    rtid: Uuid,
    workers: Vec<Worker>,
    handlers: Vec<JoinHandle<()>>,
    self_handler: Option<JoinHandle<()>>,
    tx: Sender<Job>,
    session: Arc<Session>,
}

impl WorkerPool {
    pub fn new(size: usize, session: Arc<Session>, rtid: Uuid) -> Self {
        let (tx, rx) = unbounded();
        let mut workers = Vec::with_capacity(size);

        for i in 0..size {
            workers.push(Worker::new(i as u64, rx.clone(), tx.clone()));
        }

        Self {
            rtid,
            workers,
            handlers: Vec::with_capacity(size),
            self_handler: None,
            tx,
            session,
        }
    }

    pub fn start(&mut self) {
        for w in &self.workers {
            let c_w = w.clone();
            let h = async_std::task::spawn(async move { c_w.clone().run().await });
            self.handlers.push(h);
        }

        let c_tx = self.tx.clone();
        let c_session = self.session.clone();
        let c_id = self.rtid.clone();

        let h = async_std::task::spawn(async move {
            let mut j_stream = subscribe_sumbitted_jobs(&c_session, &c_id).await.unwrap();
            log::info!("[Job Queue {c_id:?} ] Started");
            loop {
                match j_stream.next().await {
                    Some(job) => {
                        log::info!("[Job Queue {c_id:?} ]Received Job {job:?}");
                        let jid = job.id.clone();
                        c_tx.send_async(job).await.unwrap();
                        del_submitted_job(&c_session, &c_id, &jid).await.unwrap();
                    }
                    None => (), //log::info!("[Job Queue {c_id:?} ] Received nothing"),
                }
            }
        });

        self.self_handler = Some(h);
    }

    pub async fn sumbit(&self, job: Job) -> Result<()> {
        add_submitted_job(&self.session, &self.rtid, &job).await
    }
}
