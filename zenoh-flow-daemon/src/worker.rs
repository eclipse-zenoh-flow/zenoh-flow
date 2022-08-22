//
// Copyright (c) 2022 ZettaScale Technology
//
// This progr&am and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use flume::Receiver;
use uhlc::HLC;
use uuid::Uuid;
use zenoh_flow::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::model::dataflow::record::DataFlowRecord;
use zenoh_flow::runtime::{worker_pool::WorkerTrait, Job, Runtime, RuntimeClient};
use zenoh_flow::zfresult::ErrorKind;
use zenoh_flow::{DaemonResult, Result as ZFResult};

use crate::daemon::Daemon;

#[derive(Clone)]
pub struct Worker {
    id: usize,
    incoming_jobs: Arc<Receiver<Job>>,
    hlc: Arc<HLC>,
    daemon: Daemon,
}

unsafe impl Send for Worker {}
unsafe impl Sync for Worker {}

impl Worker {
    pub fn new(
        id: usize,
        incoming_jobs: Arc<Receiver<Job>>,
        hlc: Arc<HLC>,
        daemon: Daemon,
    ) -> Self {
        Self {
            id,
            incoming_jobs,
            hlc,
            daemon,
        }
    }

    async fn create_instance(
        &self,
        dfd: FlattenDataFlowDescriptor,
        instance_id: Uuid,
    ) -> DaemonResult<DataFlowRecord> {
        let mut rt_clients = vec![];
        let mapped =
            zenoh_flow::runtime::map_to_infrastructure(dfd, &self.daemon.ctx.runtime_name).await?;

        // Getting runtime involved in this instance
        let involved_runtimes = mapped.get_runtimes();
        let involved_runtimes = involved_runtimes
            .into_iter()
            .filter(|rt| *rt != self.daemon.ctx.runtime_name);

        // Creating the record
        let dfr = DataFlowRecord::try_from((mapped, instance_id))?;

        self.daemon
            .store
            .add_runtime_flow(&self.daemon.ctx.runtime_uuid, &dfr)
            .await?;

        // Creating clients to talk with other runtimes
        for rt in involved_runtimes {
            let rt_info = self.daemon.store.get_runtime_info_by_name(&rt).await?;
            let client = RuntimeClient::new(self.daemon.ctx.session.clone(), rt_info.id);
            rt_clients.push(client);
        }

        // remote prepare
        for client in rt_clients.iter() {
            client.prepare(dfr.uuid).await??;
        }

        // self prepare
        Runtime::prepare(&self.daemon, dfr.uuid).await?;

        Ok(dfr)
    }

    async fn start_instance(&self, instance_id: Uuid) -> DaemonResult<()> {
        Runtime::start_instance(&self.daemon, instance_id).await
    }

    async fn store_error(&self, job: &mut Job, e: ErrorKind) -> ZFResult<()> {
        log::error!(
            "[Worker: {}] Got error when running Job: {} - {:?}",
            self.id,
            job.get_id(),
            e
        );
        job.failed(self.hlc.new_timestamp(), format!("{:?}", e));
        self.daemon
            .store
            .add_failed_job(&self.daemon.ctx.runtime_uuid, &job)
            .await?;
        Ok(())
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

#[async_trait]
impl WorkerTrait for Worker {
    async fn run(&self) -> ZFResult<()> {
        log::info!("Worker [{}]: Started", self.id);
        while let Ok(mut job) = self.incoming_jobs.recv_async().await {
            log::trace!("Worker [{}]: Received Job {job:?}", self.id);
            job.started(self.id, self.hlc.new_timestamp());
            self.daemon
                .store
                .add_started_job(&self.daemon.ctx.runtime_uuid, &job)
                .await?;

            match job.get_kind() {
                zenoh_flow::runtime::JobKind::CreateInstance(dfd, inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Creating Flow {} : {}",
                        self.id,
                        job.get_id(),
                        dfd.flow,
                        inst_uuid
                    );

                    match self.create_instance(dfd.clone(), *inst_uuid).await {
                        Ok(dfr) => {
                            log::info!(
                                "[Worker: {}] Created Flow {} - Instance UUID: {}",
                                self.id,
                                dfr.flow,
                                dfr.uuid
                            );
                        }
                        Err(e) => {
                            self.store_error(&mut job, e).await?;
                            continue;
                        }
                    }
                }

                zenoh_flow::runtime::JobKind::DeleteInstance(_) => todo!(),
                zenoh_flow::runtime::JobKind::Instantiate(dfd, inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Instantiating Flow {} : {}",
                        self.id,
                        job.get_id(),
                        dfd.flow,
                        inst_uuid
                    );

                    match self.create_instance(dfd.clone(), *inst_uuid).await {
                        Ok(dfr) => match self.start_instance(dfr.uuid).await {
                            Ok(_) => {
                                log::info!(
                                    "[Worker: {}] Instantiated Flow {} - Instance UUID: {}",
                                    self.id,
                                    dfr.flow,
                                    dfr.uuid
                                );
                            }
                            Err(e) => {
                                self.store_error(&mut job, e).await?;
                                continue;
                            }
                        },
                        Err(e) => {
                            self.store_error(&mut job, e).await?;
                            continue;
                        }
                    }
                }
                zenoh_flow::runtime::JobKind::Teardown(_) => todo!(),
            }

            job.done(self.hlc.new_timestamp());
            self.daemon
                .store
                .add_done_job(&self.daemon.ctx.runtime_uuid, &job)
                .await?;
        }
        Ok(())
    }
}
