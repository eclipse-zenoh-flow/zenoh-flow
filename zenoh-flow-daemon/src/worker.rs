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

use async_trait::async_trait;
use flume::Receiver;
use uhlc::HLC;

use zenoh_flow::runtime::JobKind;
use zenoh_flow::runtime::{worker_pool::WorkerTrait, Job};
use zenoh_flow::zfresult::ErrorKind;
use zenoh_flow::Result as ZFResult;

use crate::runtime::Runtime;

#[derive(Clone)]
pub struct Worker {
    id: usize,
    incoming_jobs: Arc<Receiver<Job>>,
    hlc: Arc<HLC>,
    runtime: Runtime,
}

unsafe impl Send for Worker {}
unsafe impl Sync for Worker {}

impl Worker {
    pub fn new(
        id: usize,
        incoming_jobs: Arc<Receiver<Job>>,
        hlc: Arc<HLC>,
        runtime: Runtime,
    ) -> Self {
        Self {
            id,
            incoming_jobs,
            hlc,
            runtime,
        }
    }

    async fn store_error(&self, job: &mut Job, e: ErrorKind) -> ZFResult<()> {
        log::error!(
            "[Worker: {}] Got error when running Job: {} - {:?}",
            self.id,
            job.get_id(),
            e
        );
        job.failed(self.hlc.new_timestamp(), format!("{:?}", e));
        self.runtime
            .store
            .add_failed_job(&self.runtime.ctx.runtime_uuid, job)
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
            self.runtime
                .store
                .add_started_job(&self.runtime.ctx.runtime_uuid, &job)
                .await?;

            match job.get_kind() {
                JobKind::CreateInstance(dfd, inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Creating Flow {} : {}",
                        self.id,
                        job.get_id(),
                        dfd.flow,
                        inst_uuid
                    );

                    match self.runtime.create_instance(dfd.clone(), *inst_uuid).await {
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

                JobKind::DeleteInstance(inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Deleting Flow Instance {}",
                        self.id,
                        job.get_id(),
                        inst_uuid
                    );

                    match self.runtime.delete_instance(*inst_uuid).await {
                        Ok(dfr) => {
                            log::info!(
                                "[Worker: {}] Deleted Flow {} - Instance UUID: {}",
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
                JobKind::Instantiate(dfd, inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Instantiating Flow {} : {}",
                        self.id,
                        job.get_id(),
                        dfd.flow,
                        inst_uuid
                    );

                    match self.runtime.instantiate(dfd.clone(), *inst_uuid).await {
                        Ok(dfr) => {
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
                    }
                }
                JobKind::Teardown(inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Teardown Flow Instance {}",
                        self.id,
                        job.get_id(),
                        inst_uuid
                    );

                    match self.runtime.teardown(*inst_uuid).await {
                        Ok(dfr) => {
                            log::info!(
                                "[Worker: {}] Teardown Flow {} - Instance UUID: {}",
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
                JobKind::StartInstance(inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Start Flow Instance {}",
                        self.id,
                        job.get_id(),
                        inst_uuid
                    );

                    match self.runtime.start_instance(*inst_uuid).await {
                        Ok(_) => {
                            log::info!(
                                "[Worker: {}] Started Flow Instance UUID: {}",
                                self.id,
                                inst_uuid
                            );
                        }
                        Err(e) => {
                            self.store_error(&mut job, e).await?;
                            continue;
                        }
                    }
                }
                JobKind::StopInstance(inst_uuid) => {
                    log::info!(
                        "[Worker: {}] Job: {} Stop Flow Instance {}",
                        self.id,
                        job.get_id(),
                        inst_uuid
                    );

                    match self.runtime.stop_instance(*inst_uuid).await {
                        Ok(dfr) => {
                            log::info!(
                                "[Worker: {}] Stopped Flow Instance UUID: {}",
                                self.id,
                                dfr.uuid
                            );
                        }
                        Err(e) => {
                            self.store_error(&mut job, e).await?;
                            continue;
                        }
                    }
                }
                JobKind::StartNode(inst_uuid, node_id) => {
                    log::info!(
                        "[Worker: {}] Job: {} Start Node {} Instance {}",
                        self.id,
                        job.get_id(),
                        node_id,
                        inst_uuid
                    );

                    match self.runtime.start_node(*inst_uuid, node_id.clone()).await {
                        Ok(_) => {
                            log::info!(
                                "[Worker: {}] Started Node {}  Instance UUID: {}",
                                self.id,
                                node_id,
                                inst_uuid,
                            );
                        }
                        Err(e) => {
                            self.store_error(&mut job, e).await?;
                            continue;
                        }
                    }
                }
                JobKind::StopNode(inst_uuid, node_id) => {
                    log::info!(
                        "[Worker: {}] Job: {} Stop Node {} Instance {}",
                        self.id,
                        job.get_id(),
                        node_id,
                        inst_uuid
                    );

                    match self.runtime.stop_node(*inst_uuid, node_id.clone()).await {
                        Ok(_) => {
                            log::info!(
                                "[Worker: {}] Stopped Node {}  Instance UUID: {}",
                                self.id,
                                node_id,
                                inst_uuid
                            );
                        }
                        Err(e) => {
                            self.store_error(&mut job, e).await?;
                            continue;
                        }
                    }
                }
            }

            job.done(self.hlc.new_timestamp());
            self.runtime
                .store
                .add_done_job(&self.runtime.ctx.runtime_uuid, &job)
                .await?;
        }
        Ok(())
    }
}
