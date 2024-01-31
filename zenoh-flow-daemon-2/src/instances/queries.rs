//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use super::{abort, create, delete, start};
use crate::Origin;

use std::{fmt::Debug, sync::Arc};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use zenoh::{prelude::r#async::*, queryable::Query};
use zenoh_flow_commons::{InstanceId, Result};
use zenoh_flow_descriptors::FlattenedDataFlowDescriptor;
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::Runtime;

async fn reply<T: Serialize + Debug>(query: Query, data: Result<T>) -> Result<()> {
    let sample = match data {
        Ok(data) => match serde_json::to_vec(&data) {
            Ok(payload) => Ok(Sample::new(query.key_expr().clone(), payload)),
            Err(e) => Err(Value::from(e.to_string())),
        },

        Err(e) => Err(Value::from(e.to_string())),
    };

    query
        .reply(sample)
        .res()
        .await
        .map_err(|e| anyhow!("Failed to send reply: {:?}", e))
}

#[derive(Debug, Deserialize, Serialize)]
pub enum InstancesQuery {
    /// Requests the runtime to create a new data flow instance based on the [FlattenedDataFlowDescriptor].
    ///
    /// This query returns the unique identifier, [InstanceId], associated with the instance as soon as a
    /// [DataFlowRecord] is generated but *before* the instance is loaded (i.e. ready to be started).
    Create(Box<FlattenedDataFlowDescriptor>),
    /// Requests the runtime to load the provided [DataFlowRecord].
    Load(Box<DataFlowRecord>),
    /// Requests the runtime to start the data flow instance identified by the provided [InstanceId].
    ///
    /// If the [Origin] of the query is [Client](Origin::Client) then the Daemon will query all the other runtimes
    /// involved in the execution of the data flow to also start it.
    Start {
        origin: Origin,
        instance_id: InstanceId,
    },
    /// Requests the runtime to abort the execution of the data flow instance identified by the provided [InstanceId].
    ///
    /// If the [Origin] of the query is [Client](Origin::Client) then the Daemon will query all the other runtimes
    /// involved in the execution of the data flow to also abort it.
    Abort {
        origin: Origin,
        instance_id: InstanceId,
    },
    /// Requests the runtime to delete the instance.
    Delete {
        origin: Origin,
        instance_id: InstanceId,
    },
    /// Requests the status of the data flow instance identified by the provided [InstanceId].
    ///
    /// A Daemon that answers this query will only provide its *local view* of the data flow instance.
    Status(InstanceId),
    /// Requests the list of data flow instances currently running on the runtime.
    List,
}

impl InstancesQuery {
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn process(self, query: Query, runtime: Arc<Runtime>) {
        match self {
            InstancesQuery::Create(data_flow) => {
                if let Err(e) = reply(query, create::create_instance(runtime, &data_flow)).await {
                    tracing::error!("Failed to reply to 'create' query: {:?}", e);
                }
            }

            InstancesQuery::Load(record) => {
                if let Err(e) =
                    reply(query, runtime.try_load_data_flow(*record.clone()).await).await
                {
                    tracing::error!("Failed to reply to 'load' query: {:?}", e);
                }
            }

            InstancesQuery::Start {
                origin,
                instance_id,
            } => start::start(runtime, query, origin, instance_id),

            InstancesQuery::Abort {
                origin,
                instance_id,
            } => {
                abort::abort(runtime, origin, instance_id);
            }

            InstancesQuery::Delete {
                origin,
                instance_id,
            } => delete::delete_instance(runtime, origin, instance_id),

            InstancesQuery::Status(instance_id) => {
                if let Err(e) = reply(
                    query,
                    runtime.get_status(&instance_id).await.ok_or_else(|| {
                        anyhow!("Found no data flow with instance id < {} >", instance_id)
                    }),
                )
                .await
                {
                    tracing::error!("Failed to reply to 'Status' query: {:?}", e);
                }
            }

            InstancesQuery::List => {
                if let Err(e) = reply(query, Ok(runtime.instances_status().await)).await {
                    tracing::error!("Failed to reply to 'List' query: {:?}", e);
                }
            }
        }
    }
}
