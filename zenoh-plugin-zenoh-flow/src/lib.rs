//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use flume::Sender;
use zenoh::internal::{
    plugins::{RunningPluginTrait, ZenohPlugin},
    zerror,
};
use zenoh_flow_daemon::daemon::*;
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin, PluginControl};

pub struct ZenohFlowPlugin(Sender<()>);

pub const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");

#[cfg(feature = "dynamic_plugin")]
zenoh_plugin_trait::declare_plugin!(ZenohFlowPlugin);

impl ZenohPlugin for ZenohFlowPlugin {}
impl Plugin for ZenohFlowPlugin {
    type StartArgs = zenoh::internal::runtime::Runtime;
    type Instance = zenoh::internal::plugins::RunningPlugin;

    const DEFAULT_NAME: &'static str = "zenoh-flow";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(
        name: &str,
        zenoh_runtime: &Self::StartArgs,
    ) -> zenoh::Result<zenoh::internal::plugins::RunningPlugin> {
        let _ = tracing_subscriber::fmt::try_init();

        let zenoh_config = zenoh_runtime.config().lock();
        let zenoh_flow_config = zenoh_config
            .plugin(name)
            .cloned()
            .ok_or_else(|| zerror!("Plugin '{}': missing configuration", name))?;

        let (abort_tx, abort_rx) = flume::bounded(1);
        let zenoh_runtime = zenoh_runtime.clone();
        async_std::task::spawn(async move {
            let zenoh_session = zenoh::session::init(zenoh_runtime).await.unwrap();

            let zenoh_flow_config: ZenohFlowConfiguration =
                match serde_json::from_value(zenoh_flow_config) {
                    Ok(config) => config,
                    Err(e) => {
                        tracing::error!("Failed to parse configuration: {e:?}");
                        return;
                    }
                };

            let daemon = match Daemon::spawn_from_config(zenoh_session, zenoh_flow_config).await {
                Ok(daemon) => daemon,
                Err(e) => {
                    tracing::error!("Failed to spawn the Daemon from a configuration: {e:?}");
                    return;
                }
            };

            if let Err(e) = abort_rx.recv_async().await {
                tracing::error!("Abort channel failed with: {e:?}");
            }

            daemon.stop().await;
        });

        Ok(Box::new(ZenohFlowPlugin(abort_tx)))
    }
}

impl PluginControl for ZenohFlowPlugin {}
impl RunningPluginTrait for ZenohFlowPlugin {
    fn adminspace_getter(
        &self,
        selector: &zenoh::key_expr::KeyExpr,
        plugin_status_key: &str,
    ) -> zenoh::Result<Vec<zenoh::internal::plugins::Response>> {
        let mut responses = Vec::new();
        let version_key = [plugin_status_key, "/__version__"].concat();
        let ke = zenoh::key_expr::KeyExpr::new(&version_key)?;

        if selector.intersects(&ke) {
            responses.push(zenoh::internal::plugins::Response::new(
                version_key,
                GIT_VERSION.into(),
            ));
        }
        Ok(responses)
    }
}
