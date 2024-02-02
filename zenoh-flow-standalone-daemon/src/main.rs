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

use std::{path::PathBuf, sync::Arc};

use async_std::stream::StreamExt;
use clap::Parser;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::Signals;
use uhlc::HLC;
use zenoh::prelude::r#async::*;
use zenoh_flow_daemon_2::DaemonBuilder;

#[derive(Parser)]
struct Cli {
    name: Arc<str>,
    #[clap(short, long)]
    configuration: Option<PathBuf>,
    #[clap(short, long)]
    runtime_id: Option<ZenohId>,
}

#[async_std::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    let cli = Cli::parse();

    let mut zenoh_config = zenoh::config::peer();
    if let Some(runtime_id) = cli.runtime_id {
        if zenoh_config.set_id(runtime_id).is_err() {
            tracing::error!(
                "Failed to set id of the Zenoh session: (desired) {}, (current) {}",
                runtime_id,
                zenoh_config.id()
            );
        }
    }

    let zenoh_session = zenoh::open(zenoh_config)
        .res()
        .await
        .expect("Failed to open Zenoh session in PEER mode.")
        .into_arc();

    let hlc = Arc::new(HLC::default());

    let builder = DaemonBuilder::new(zenoh_session, hlc, cli.name);

    let daemon = builder.start().await;

    async_std::task::spawn(async move {
        let mut signals = Signals::new([SIGTERM, SIGINT, SIGQUIT])
            .expect("Failed to create SignalsInfo for: [SIGTERM, SIGINT, SIGQUIT]");

        while let Some(signal) = signals.next().await {
            match signal {
                SIGTERM | SIGINT | SIGQUIT => {
                    tracing::info!("Received termination signal, shutting down.");
                    daemon.stop().await;
                    break;
                }

                _ => {
                    unreachable!()
                }
            }
        }
    })
    .await;
}
