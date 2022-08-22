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

use crate::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use crate::runtime::{resources::DataStore, Job};
use crate::Result as ZFResult;
use async_std::{stream::StreamExt, task::JoinHandle};
use async_trait::async_trait;
use flume::{unbounded, Receiver, Sender};
use futures::stream::{AbortHandle, Abortable, Aborted};
use uhlc::HLC;
use uuid::Uuid;

/// The trait of the different workers implementations.
#[async_trait]
pub trait WorkerTrait: Send + Sync {
    async fn run(&self) -> ZFResult<()>;
}

/// The trait for the function creating a new worker.
pub trait FnNewWorkerTrait: Send + Sync {
    fn call(&self, id: usize, rx: Arc<Receiver<Job>>, hlc: Arc<HLC>) -> Box<dyn WorkerTrait>;
}

/// This impl allows to create a [`FnNewWorkerTrait`](`FnNewWorkerTrait`) from
/// an `FnOnce(usize, Arc<Receiver<Job>>, Arc<HLC>)`.
impl<F> FnNewWorkerTrait for F
where
    F: FnOnce(usize, Arc<Receiver<Job>>, Arc<HLC>) -> Box<dyn WorkerTrait> + Clone + Send + Sync,
{
    fn call(&self, id: usize, rx: Arc<Receiver<Job>>, hlc: Arc<HLC>) -> Box<dyn WorkerTrait> {
        self.clone()(id, rx, hlc)
    }
}

/// Type alias for the function used to create new workers.
pub type FnNewWorker = Arc<dyn FnNewWorkerTrait>;

pub struct WorkerPool {
    rtid: Uuid,
    pool_size: usize,
    new_worker_fn: FnNewWorker,
    workers: Vec<Box<dyn WorkerTrait>>,
    handlers: Vec<JoinHandle<Result<ZFResult<()>, Aborted>>>,
    handle: Option<JoinHandle<Result<ZFResult<()>, Aborted>>>,
    abort_handle: Option<AbortHandle>,
    abort_handlers: Vec<AbortHandle>,
    tx: Sender<Job>,
    rx: Arc<Receiver<Job>>,
    session: DataStore,
    hlc: Arc<HLC>,
}

unsafe impl Send for WorkerPool {}
unsafe impl Sync for WorkerPool {}

impl WorkerPool {
    pub fn new(
        pool_size: usize,
        session: DataStore,
        rtid: Uuid,
        hlc: Arc<HLC>,
        new_worker_fn: FnNewWorker,
    ) -> Self {
        let (tx, rx) = unbounded();
        let rx = Arc::new(rx);

        let mut workers = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            let new_fn_clone = Arc::clone(&new_worker_fn);
            let worker = new_fn_clone.call(i, rx.clone(), hlc.clone());
            workers.push(worker);
        }

        Self {
            rtid,
            new_worker_fn,
            pool_size,
            workers,
            handlers: Vec::with_capacity(pool_size),
            abort_handlers: Vec::with_capacity(pool_size),
            handle: None,
            abort_handle: None,
            tx,
            rx,
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

        for worker in self.workers.drain(..).into_iter() {
            let worker_loop = async move { worker.run().await };

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let handle = async_std::task::spawn(Abortable::new(worker_loop, abort_registration));
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
                        log::trace!("[Job Queue: {c_id:?}] Received Job {job:?}");
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

        for i in 0..self.pool_size {
            let new_fn_clone = Arc::clone(&self.new_worker_fn);
            let worker = new_fn_clone.call(i, self.rx.clone(), self.hlc.clone());
            self.workers.push(worker);
        }
    }

    pub async fn submit_instantiate(
        &self,
        dfd: &FlattenDataFlowDescriptor,
        instance_id: &Uuid,
    ) -> ZFResult<Job> {
        let jid = Uuid::new_v4();
        let job = Job::new_instantiate(
            dfd.clone(),
            instance_id.clone(),
            jid.clone(),
            self.hlc.new_timestamp(),
        );

        self.session.add_submitted_job(&self.rtid, &job).await?;

        Ok(job)
    }

    pub async fn submit_create(
        &self,
        dfd: &FlattenDataFlowDescriptor,
        instance_id: &Uuid,
    ) -> ZFResult<Job> {
        let jid = Uuid::new_v4();
        let job = Job::new_create(
            dfd.clone(),
            instance_id.clone(),
            jid.clone(),
            self.hlc.new_timestamp(),
        );

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
