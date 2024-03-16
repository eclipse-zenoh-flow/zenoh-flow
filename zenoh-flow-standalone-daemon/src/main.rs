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

use std::path::PathBuf;

use async_std::stream::StreamExt;
use clap::Parser;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::Signals;
use zenoh_flow_commons::RuntimeId;
use zenoh_flow_daemon::daemon::*;

#[derive(Parser)]
struct Cli {
    name: String,
    #[clap(short, long)]
    configuration: Option<PathBuf>,
    #[clap(short, long)]
    runtime_id: Option<RuntimeId>,
}

#[async_std::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    let cli = Cli::parse();

    let mut runtime_builder = Runtime::builder(cli.name);

    if let Some(runtime_id) = cli.runtime_id {
        runtime_builder = runtime_builder
            .runtime_id(runtime_id)
            .expect("Failed to set the identifier of the Runtime");
    }

    let runtime = runtime_builder
        .build()
        .await
        .expect("Failed to build the Zenoh-Flow Runtime");

    let daemon = Daemon::spawn(runtime)
        .await
        .expect("Failed to spawn the Zenoh-Flow Daemon");

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
