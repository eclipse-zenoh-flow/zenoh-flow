//
// Copyright (c) 2017, 2022 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use git_version::git_version;
use std::env;
use std::sync::Arc;
use zenoh::plugins::{Plugin, RunningPluginTrait, Runtime, ZenohPlugin};
use zenoh::prelude::r#async::*;
use zenoh::Result as ZResult;
use zenoh_core::{bail, zerror};

use zenoh_flow_daemon::daemon::Daemon;
use zenoh_flow_daemon::daemon::DaemonConfig;

use flume::{Receiver, Sender};

extern crate zenoh_core;

pub const GIT_VERSION: &str = git_version!(prefix = "v", cargo_prefix = "v");
lazy_static::lazy_static! {
    pub static ref LONG_VERSION: String = format!("{} built with {}", GIT_VERSION, env!("RUSTC_VERSION"));
}

macro_rules! ke_for_sure {
    ($val:expr) => {
        unsafe { keyexpr::from_str_unchecked($val) }
    };
}

pub struct ZenohFlowPlugin(Sender<()>);

impl ZenohPlugin for ZenohFlowPlugin {}
impl Plugin for ZenohFlowPlugin {
    type StartArgs = Runtime;
    type RunningPlugin = zenoh::plugins::RunningPlugin;

    const STATIC_NAME: &'static str = "zenoh-flow-plugin";

    fn start(name: &str, runtime: &Self::StartArgs) -> ZResult<Self::RunningPlugin> {
        // Try to initiate login.
        // Required in case of dynamic lib, otherwise no logs.
        // But cannot be done twice in case of static link.
        let _ = env_logger::try_init();

        let runtime_conf = runtime.config.lock();
        let plugin_conf = runtime_conf
            .plugin(name)
            .ok_or_else(|| zerror!("Plugin `{}`: missing config", name))?;

        let config: DaemonConfig = serde_json::from_value(plugin_conf.clone())
            .map_err(|e| zerror!("Plugin `{}` configuration error: {}", name, e))?;

        let (tx, rx) = flume::bounded(1);

        async_std::task::spawn(run(runtime.clone(), config, rx));

        Ok(Box::new(ZenohFlowPlugin(tx)))
    }
}

impl RunningPluginTrait for ZenohFlowPlugin {
    fn config_checker(&self) -> zenoh::plugins::ValidationFunction {
        Arc::new(|_, _, _| bail!("zenoh-flow-plugin does not support hot configuration changes."))
    }

    fn adminspace_getter<'a>(
        &'a self,
        selector: &'a Selector<'a>,
        plugin_status_key: &str,
    ) -> ZResult<Vec<zenoh::plugins::Response>> {
        let mut responses = Vec::new();
        let version_key = [plugin_status_key, "/__version__"].concat();
        if selector.key_expr.intersects(ke_for_sure!(&version_key)) {
            responses.push(zenoh::plugins::Response::new(
                version_key,
                GIT_VERSION.into(),
            ));
        }
        Ok(responses)
    }
}

async fn run(runtime: Runtime, config: DaemonConfig, rx: Receiver<()>) -> ZResult<()> {
    // Try to initiate login.
    // Required in case of dynamic lib, otherwise no logs.
    // But cannot be done twice in case of static link.
    let _ = env_logger::try_init();

    log::debug!("Zenoh-Flow plugin {}", LONG_VERSION.as_str());
    log::debug!("Zenoh-Flow plugin {:?}", config);

    let zsession = Arc::new(zenoh::init(runtime).res().await?);

    let daemon = Daemon::from_session_and_config(zsession, config)
        .map_err(|e| zerror!("Plugin `zenoh-flow-plugin` configuration error: {}", e))?;

    let (s, h) = daemon
        .start()
        .await
        .map_err(|e| zerror!("Plugin `zenoh-flow-plugin` unable to start runtime {}", e))?;

    if (rx.recv_async().await).is_ok() {
        log::debug!("Zenoh-Flow plugin, stopping.");
    } else {
        log::warn!("Zenoh-Flow plugin, stopping, channel sender was dropped!");
    }

    daemon.stop(s).await.map_err(|e| {
        zerror!(
            "Plugin `zenoh-flow-plugin` error when stopping the runtime {}",
            e
        )
    })?;
    //wait for the futures to ends
    h.await.map_err(|e| {
        zerror!(
            "Plugin `zenoh-flow-plugin` error while runtime shutdown {}",
            e
        )
    })?;

    Ok(())
}

impl Drop for ZenohFlowPlugin {
    fn drop(&mut self) {
        let res = self.0.send(());
        log::debug!("Zenoh Flow plugin, shuting down: {:?}", res);
    }
}
