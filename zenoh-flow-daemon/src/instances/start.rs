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

use super::abort;
use crate::{selectors, InstancesQuery, Origin};

use std::sync::Arc;

use anyhow::bail;
use zenoh::{prelude::r#async::*, queryable::Query};
use zenoh_flow_commons::{InstanceId, Result, RuntimeId};
use zenoh_flow_runtime::Runtime;

/// Starts the data flow instance identified by `instance_id`.
///
/// If this query originates from a [Client](Origin::Client) then this function also queries the other runtimes to start
/// the same data flow instance.
///
/// # Consistency
///
/// If a Zenoh-Flow runtime fails to start the data flow instance, all other involved runtimes will abort that instance.
pub(crate) fn start(runtime: Arc<Runtime>, query: Query, origin: Origin, instance_id: InstanceId) {
    async_std::task::spawn(async move {
        // -------------------------------------------------------------------------------------------------------------
        // NOTE: If, at any step, starting the data flow instance fails, we have to stop and inform the client / daemon
        // that initiated this process that it failed.
        macro_rules! return_if_err {
            (
                $faillible: expr,
                $message: expr,
                $( $item: expr ),*
            ) => {
                match $faillible {
                    Ok(result) => result,
                    Err(e) => {
                        let message = format!($message, $( $item, )*);
                        tracing::error!(r#"{}
Caused by:
{:?}"#,
                            message,
                            e
                        );
                        if let Err(e) = query.reply(Err(Value::from(message))).res().await {
                            tracing::error!(
                                "Failed to reply (error) to query on < {} >: {:?}",
                                query.key_expr(),
                                e
                            );
                        }
                        return;
                    }
                }
            };
        }
        // -------------------------------------------------------------------------------------------------------------

        let record = return_if_err!(
            runtime
                .get_record(&instance_id)
                .await
                .ok_or(anyhow::anyhow!("Not found")),
            "Found no data flow with id: {}",
            instance_id
        );

        if matches!(origin, Origin::Client) {
            return_if_err!(
                query_start(
                    &runtime.session(),
                    record
                        .mapping
                        .keys()
                        .filter(|&runtime_id| runtime_id != runtime.id()),
                    &instance_id,
                )
                .await,
                "Failed to query other runtime(s) to start instance < {} >",
                instance_id
            );
        }

        if record.mapping.contains_key(runtime.id()) {
            return_if_err!(
                runtime.try_start_instance(&instance_id).await,
                "Failed to start instance < {} >",
                instance_id
            );
        }

        tracing::trace!("Successfully started instance < {} >", instance_id);
        return_if_err!(
            query
                .reply(Ok(Sample::new(query.key_expr().clone(), Value::empty())))
                .res()
                .await,
            "Failed to reply (success) to query on < {} >",
            query.key_expr()
        );
    });
}

/// Queries the `runtimes` to start the data flow instance identified by `instance_id` — rollback if needed.
///
/// This function is intended to only be called by the runtime that received the query from a [Client](Origin::Client).
///
/// If a runtime fails to start the data flow instance, all the previously contacted runtimes are queried again to
/// abort the instance.
///
/// # Rollback
///
/// This function will rollback the `start` request if a single Zenoh-Flow runtime that is involved fails to start the
/// data flow instance.
///
/// This rollback consists in querying all the previously contacted Zenoh-Flow runtime to abort the execution of the
/// data flow instance.
async fn query_start(
    session: &Session,
    runtimes: impl Iterator<Item = &RuntimeId>,
    instance_id: &InstanceId,
) -> Result<()> {
    let start_query = match serde_json::to_vec(&InstancesQuery::Start {
        origin: Origin::Daemon,
        instance_id: instance_id.clone(),
    }) {
        Ok(query) => query,
        Err(e) => {
            bail!("serde_json failed to serialize `start query`: {:?}", e)
        }
    };

    let mut contacted_runtimes = Vec::new();

    // -----------------------------------------------------------------------------------------------------------------
    // NOTE: We are using a labelled block to handle the errors and avoid code repetition.
    //
    // We should be sure that for each "error" case there is a `break 'happy_path;` line.
    macro_rules! rollback_if_err {
        (
            $faillible: expr,
            $message: expr,
            $( $item: expr ),*
        ) => {
            match $faillible {
                Ok(result) => result,
                Err(e) => {
                    let message = format!($message, $( $item, )*);
                    abort::query_abort(session, contacted_runtimes.iter(), instance_id).await;
                    anyhow::bail!(r#"{}
Caused by:
{:?}"#,
                        message,
                        e
                    );
                }
            }
        };
    }
    // -----------------------------------------------------------------------------------------------------------------

    for runtime_id in runtimes {
        let selector = rollback_if_err!(
            selectors::selector_instances(runtime_id),
            "Failed to generate selector 'instances' for runtime < {} >",
            runtime_id
        );

        rollback_if_err!(
            session
                .get(selector)
                .with_value(start_query.clone())
                .res()
                .await,
            "Query `start` on runtime < {} > failed",
            runtime_id
        );

        tracing::trace!(
            "Queried runtime < {} > to start instance < {} >",
            runtime_id,
            instance_id
        );
        contacted_runtimes.push(runtime_id.clone());
    }

    Ok(())
}
