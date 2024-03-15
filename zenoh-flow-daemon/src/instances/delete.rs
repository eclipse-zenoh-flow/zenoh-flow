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

use super::InstancesQuery;
use crate::{selectors, Origin};

use std::sync::Arc;

use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{InstanceId, RuntimeId};
use zenoh_flow_runtime::{DataFlowErr, Runtime};

/// Query all the runtimes to delete the provided data flow instance.
pub(crate) async fn query_delete(
    session: &Session,
    runtimes: impl Iterator<Item = &RuntimeId>,
    instance_id: &InstanceId,
) {
    let delete_query = serde_json::to_vec(&InstancesQuery::Delete {
        origin: Origin::Daemon,
        instance_id: instance_id.clone(),
    })
    .expect("serde_json failed to serialize InstancesQuery::Delete");

    for runtime_id in runtimes {
        let selector = selectors::selector_instances(runtime_id);

        // NOTE: No need to process the request, as, even if the query failed, this is not something we want to recover
        // from.
        if let Err(e) = session
            .get(selector)
            .with_value(delete_query.clone())
            .res()
            .await
        {
            tracing::error!(
                "Sending delete query to runtime < {} > failed with error: {:?}",
                runtime_id,
                e
            );
        }
        tracing::trace!("Sent delete query to runtime < {} >", runtime_id);
    }
}

/// Deletes the data flow instance.
///
/// If the query comes from a [Client](Origin::Client) then this daemon will query all the runtimes involved in this
/// instance to make them also delete the data flow instance.
pub(crate) fn delete_instance(runtime: Arc<Runtime>, origin: Origin, instance_id: InstanceId) {
    async_std::task::spawn(async move {
        if matches!(origin, Origin::Client) {
            match runtime.try_get_record(&instance_id).await {
                Ok(record) => {
                    query_delete(
                        &runtime.session(),
                        record
                            .mapping()
                            .keys()
                            .filter(|&runtime_id| runtime_id != runtime.id()),
                        &instance_id,
                    )
                    .await
                }
                Err(DataFlowErr::NotFound) => return,
                // NOTE: If the data flow is in a failed state we still want to process the delete request but only on
                // this runtime.
                Err(DataFlowErr::FailedState) => {}
            }
        }

        if let Err(e) = runtime.try_delete_instance(&instance_id).await {
            tracing::error!("Failed to delete instance < {} >: {:?}", instance_id, e);
        }
    });
}
