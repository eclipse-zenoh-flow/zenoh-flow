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

use std::sync::Arc;

use super::{resources::DataStore, Job};
use crate::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use crate::Result as ZFResult;
use async_std::{stream::StreamExt, task::JoinHandle};
use flume::{unbounded, Receiver, Sender};
use futures::stream::{AbortHandle, Abortable, Aborted};
use uhlc::HLC;
use uuid::Uuid;

#[derive(Clone)]
pub struct Worker {
    id: usize,
    incoming_jobs: Arc<Receiver<Job>>,
    hlc: Arc<HLC>,
}

impl Worker {
    fn new(id: usize, incoming_jobs: Arc<Receiver<Job>>, hlc: Arc<HLC>) -> Self {
        Self {
            id,
            incoming_jobs,
            hlc,
        }
    }

    async fn run(&self) -> ZFResult<()> {
        log::info!("Worker [{}]: Started", self.id);
        while let Ok(j) = self.incoming_jobs.recv_async().await {
            log::info!("Worker [{}]: Received Job {j:?}", self.id);
        }
        Ok(())
    }

    pub(crate) fn get_id(&self) -> &usize {
        &self.id
    }
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "id: {:?} incoming_jobs: {:?}",
            self.id, self.incoming_jobs
        )
    }
}

pub struct WorkerPool {
    rtid: Uuid,
    workers: Vec<Worker>,
    handlers: Vec<JoinHandle<Result<ZFResult<()>, Aborted>>>,
    handle: Option<JoinHandle<Result<ZFResult<()>, Aborted>>>,
    abort_handle: Option<AbortHandle>,
    abort_handlers: Vec<AbortHandle>,
    tx: Sender<Job>,
    session: DataStore,
    hlc: Arc<HLC>,
}

impl WorkerPool {
    pub fn new(size: usize, session: DataStore, rtid: Uuid, hlc: Arc<HLC>) -> Self {
        let (tx, rx) = unbounded();
        let rx = Arc::new(rx);

        let mut workers = Vec::with_capacity(size);

        for i in 0..size {
            workers.push(Worker::new(i, rx.clone(), hlc.clone()));
        }

        Self {
            rtid,
            workers,
            handlers: Vec::with_capacity(size),
            abort_handlers: Vec::with_capacity(size),
            handle: None,
            abort_handle: None,
            tx,
            session,
            hlc,
        }
    }

    pub fn start(&mut self) {
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Job Queue: {}] Trying to start while it is already started, aborting",
                self.rtid
            );
            return;
        }

        for w in &self.workers {
            let c_w = w.clone();

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let handle = async_std::task::spawn(Abortable::new(
                async move { c_w.run().await },
                abort_registration,
            ));
            self.handlers.push(handle);
            self.abort_handlers.push(abort_handle);
        }

        let c_tx = self.tx.clone();
        let c_session = self.session.clone();
        let c_id = self.rtid.clone();

        let run_loop = async move {
            let mut j_stream = c_session.subscribe_sumbitted_jobs(&c_id).await?;
            log::info!("[Job Queue {c_id:?} ] Started");
            loop {
                match j_stream.next().await {
                    Some(job) => {
                        // Receiving a Job and sending to the workers via the
                        // flume channel
                        log::info!("[Job Queue: {c_id:?}] Received Job {job:?}");
                        c_tx.send_async(job).await?;
                    }
                    None => (),
                }
            }
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

        self.handle = Some(handle);
        self.abort_handle = Some(abort_handle);
    }

    pub async fn stop(&mut self) {
        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort()
        }

        for ah in self.abort_handlers.drain(..).into_iter() {
            ah.abort()
        }

        if let Some(handle) = self.handle.take() {
            log::trace!(
                "[Job Queue: {:?}] handler finished with {:?}",
                self.rtid,
                handle.await
            );
        }

        for wh in self.handlers.drain(..).into_iter() {
            log::trace!(
                "[Job Queue: {:?} - Worker] handler finished with {:?}",
                self.rtid,
                wh.await
            );
        }
    }

    pub async fn submit_instantiate(&self, dfd: &FlattenDataFlowDescriptor) -> ZFResult<Job> {
        let jid = Uuid::new_v4();
        let job = Job::new_instantiate(dfd.clone(), jid.clone(), self.hlc.new_timestamp());

        self.session.add_submitted_job(&self.rtid, &job).await?;

        Ok(job)
    }

    pub async fn submit_teardown(&self, fid: &Uuid) -> ZFResult<Job> {
        let jid = Uuid::new_v4();
        let job = Job::new_teardown(fid.clone(), jid.clone(), self.hlc.new_timestamp());

        self.session.add_submitted_job(&self.rtid, &job).await?;

        Ok(job)
    }
}
