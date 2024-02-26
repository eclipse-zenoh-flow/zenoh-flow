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

use crate::{selectors, InstancesQuery, Origin};

use std::sync::Arc;

use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{InstanceId, RuntimeId};
use zenoh_flow_runtime::Runtime;

pub(crate) fn abort(runtime: Arc<Runtime>, origin: Origin, instance_id: InstanceId) {
    async_std::task::spawn(async move {
        if matches!(origin, Origin::Client) {
            if let Some(record) = runtime.get_record(&instance_id).await {
                query_abort(
                    &runtime.session(),
                    record
                        .mapping()
                        .keys()
                        .filter(|&runtime_id| runtime_id != runtime.id()),
                    &instance_id,
                )
                .await;
            } else {
                tracing::error!(
                    "No data flow instance with id < {} > was found",
                    instance_id
                );
                return;
            }
        }

        if let Err(e) = runtime.try_abort_instance(&instance_id).await {
            tracing::error!("Failed to abort instance < {} >: {:?}", instance_id, e);
        }
    });
}

pub(crate) async fn query_abort(
    session: &Session,
    runtimes: impl Iterator<Item = &RuntimeId>,
    instance_id: &InstanceId,
) {
    let abort_query = match serde_json::to_vec(&InstancesQuery::Abort {
        origin: Origin::Daemon,
        instance_id: instance_id.clone(),
    }) {
        Ok(query) => query,
        Err(e) => {
            tracing::error!(
                "serde_json failed to serialize InstancesQuery::Abort: {:?}",
                e
            );
            return;
        }
    };

    for runtime_id in runtimes {
        let selector = match selectors::selector_instances(runtime_id) {
            Ok(selector) => selector,
            Err(e) => {
                tracing::error!(
                    "Generation of selector 'instances' for runtime < {} > failed: {:?}",
                    runtime_id,
                    e
                );
                continue;
            }
        };

        if let Err(e) = session
            .get(selector)
            .with_value(abort_query.clone())
            .res()
            .await
        {
            tracing::error!(
                "Sending abort query to runtime < {} > failed with error: {:?}",
                runtime_id,
                e
            );
        }
        tracing::trace!("Sent abort query to runtime < {} >", runtime_id);
    }
}
