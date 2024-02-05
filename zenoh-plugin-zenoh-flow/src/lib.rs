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

use flume::Sender;
use zenoh::{
    plugins::{RunningPluginTrait, ZenohPlugin},
    prelude::r#async::*,
};
use zenoh_flow_daemon::{configuration::ZenohFlowConfiguration, Daemon};
use zenoh_plugin_trait::Plugin;
use zenoh_result::{bail, zerror};

pub struct ZenohFlowPlugin(Sender<()>);

pub const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");

zenoh_plugin_trait::declare_plugin!(ZenohFlowPlugin);
impl ZenohPlugin for ZenohFlowPlugin {}
impl Plugin for ZenohFlowPlugin {
    type StartArgs = zenoh::runtime::Runtime;
    type RunningPlugin = zenoh::plugins::RunningPlugin;

    const STATIC_NAME: &'static str = "zenoh_flow";

    fn start(name: &str, zenoh_runtime: &Self::StartArgs) -> zenoh::Result<Self::RunningPlugin> {
        let _ = tracing_subscriber::fmt::try_init();

        let zenoh_config = zenoh_runtime.config.lock();
        let zenoh_flow_config = zenoh_config
            .plugin(name)
            .cloned()
            .ok_or_else(|| zerror!("Plugin '{}': missing configuration", name))?;

        let (abort_tx, abort_rx) = flume::bounded(1);
        let zenoh_runtime = zenoh_runtime.clone();
        async_std::task::spawn(async move {
            let zenoh_session = Arc::new(zenoh::init(zenoh_runtime).res().await.unwrap());

            let zenoh_flow_config: ZenohFlowConfiguration =
                match serde_json::from_value(zenoh_flow_config) {
                    Ok(config) => config,
                    Err(e) => {
                        tracing::error!("Failed to parse configuration: {e:?}");
                        return;
                    }
                };

            let daemon = match Daemon::from_config(zenoh_session, zenoh_flow_config).await {
                Ok(daemon) => daemon,
                Err(e) => {
                    tracing::error!("Failed to build Daemon from configuration: {e:?}");
                    return;
                }
            };

            let daemon = daemon.start().await;

            if let Err(e) = abort_rx.recv_async().await {
                tracing::error!("Abort channel failed with: {e:?}");
            }

            daemon.stop().await;
        });

        Ok(Box::new(ZenohFlowPlugin(abort_tx)))
    }
}

impl RunningPluginTrait for ZenohFlowPlugin {
    fn config_checker(&self) -> zenoh::plugins::ValidationFunction {
        Arc::new(|_, _, _| bail!("Plugin 'zenoh-flow' does not support hot configuration changes"))
    }

    fn adminspace_getter<'a>(
        &'a self,
        selector: &'a zenoh::selector::Selector<'a>,
        plugin_status_key: &str,
    ) -> zenoh_result::ZResult<Vec<zenoh::plugins::Response>> {
        let mut responses = Vec::new();
        let version_key = [plugin_status_key, "/__version__"].concat();
        let ke = KeyExpr::new(&version_key)?;

        if selector.key_expr.intersects(&ke) {
            responses.push(zenoh::plugins::Response::new(
                version_key,
                GIT_VERSION.into(),
            ));
        }
        Ok(responses)
    }
}

impl Drop for ZenohFlowPlugin {
    fn drop(&mut self) {
        let res = self.0.send(());
        tracing::debug!("Zenoh Flow plugin, shutting down: {:?}", res);
    }
}
