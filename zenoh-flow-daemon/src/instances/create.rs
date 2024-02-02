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

use std::sync::Arc;

use crate::selectors;

use super::InstancesQuery;

use anyhow::Context;
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{InstanceId, Result};
use zenoh_flow_descriptors::FlattenedDataFlowDescriptor;
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::Runtime;

/// Create a new instance of the data flow described by the provided (flattened) descriptor.
///
/// The identifier of the instance is returned before its node are fully loaded (locally and/or remotely, according to
/// the descriptor).
///
/// If a single Zenoh-Flow runtime fails to load its portion of the instance, the instance is deleted everywhere. The
/// objective is to not leave any Zenoh-Flow runtime in an incoherent state.
///
/// # Error
///
/// This function can return an error if the Zenoh-Flow runtime failed to create an instance based on the flattened
/// descriptor.
///
/// # Consistency
///
/// If a Zenoh-Flow runtime fails to load the node(s) it is responsible for, all other involved runtime will *delete*
/// that same instance.
pub(crate) fn create_instance(
    runtime: Arc<Runtime>,
    data_flow: &FlattenedDataFlowDescriptor,
) -> Result<InstanceId> {
    let record =
        DataFlowRecord::try_new(data_flow, runtime.id()).context("Failed to create Record")?;
    let instance_id = record.instance_id().clone();

    // Spawn a new task to handle the query to minimize the amount of time the Zenoh-Flow runtime is blocked.
    async_std::task::spawn(async move {
        let instance_id = record.instance_id().clone();
        let current_runtime_id = runtime.id().clone();
        let involved_runtimes = record
            .mapping
            .keys()
            .filter(|&runtime_id| runtime_id != &current_runtime_id)
            .cloned()
            .collect::<Vec<_>>();

        // We start with the current Zenoh-Flow runtime such that, if there is an error, we don't need to go through
        // the network to rollback everything.
        if record.mapping.contains_key(&current_runtime_id) {
            if let Err(e) = runtime.try_load_data_flow(record.clone()).await {
                tracing::error!("Failed to load data flow < {} >: {:?}", instance_id, e);
                return;
            }
        }

        let mut contacted_runtimes = Vec::with_capacity(involved_runtimes.len());
        let load_query = InstancesQuery::Load(Box::new(record.clone()));

        // -------------------------------------------------------------------------------------------------------------
        // NOTE: If at any step of the creation of the data flow something fails, we have to rollback: we have to
        // contact each remote Zenoh-Flow runtime and request it to delete the instance.
        //
        // The following macro let's us not repeat this rollback code.
        //
        // Defining the macro inside the function allows "capturing the context" and reduces the amount of parameters we
        // have to pass to a strict minimum.
        macro_rules! rollback_if_err {
            (
                $faillible: expr,
                $message: expr,
                $( $item: expr ),*
            ) => {
                match $faillible {
                    Ok(result) => result,
                    Err(e) => {
                        let error_message = format!($message, $( $item ),*);
                        tracing::error!(r#"{}
Caused by:
{:?}"#,
                            error_message, e
                        );
                        super::delete::query_delete(&runtime.session(),
                            contacted_runtimes.iter(),
                            &instance_id
                        ).await;

                        if record.mapping.contains_key(&current_runtime_id) {
                            if let Err(e) = runtime.try_delete_instance(&instance_id).await {
                                tracing::error!(
                                    "Failed to delete instance < {} > while cleaning up after failed creation
Caused by:
{:?}",
                                    &instance_id,
                                    e
                                );
                            }
                        }

                        return;
                    }
                }
            };
        }
        // -------------------------------------------------------------------------------------------------------------

        let payload = rollback_if_err!(
            serde_json::to_vec(&load_query),
            r#"`serde_json` failed to serialize the query:
Query:
{:?},"#,
            load_query
        );

        for runtime_id in involved_runtimes {
            let selector = rollback_if_err!(
                selectors::selector_instances(&runtime_id),
                r#"Failed to generate 'instances' selector for runtime < {} >"#,
                &runtime_id
            );

            let receiver_reply = rollback_if_err!(
                runtime
                    .session()
                    .get(&selector)
                    .with_value(payload.clone())
                    .res()
                    .await,
                r#"Zenoh query on < {} > failed"#,
                selector
            );

            let reply = rollback_if_err!(
                receiver_reply.recv_async().await,
                "Failed to receive acknowledgment from runtime < {} >",
                &runtime_id
            );

            rollback_if_err!(
                reply.sample,
                "Runtime < {} > failed to load data flow instance < {} >",
                &runtime_id,
                &instance_id
            );

            contacted_runtimes.push(runtime_id);
        }
    });

    Ok(instance_id)
}
