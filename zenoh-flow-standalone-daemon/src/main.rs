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
use zenoh::prelude::r#async::AsyncResolve;
use zenoh_flow_commons::{try_parse_from_file, Vars};
use zenoh_flow_daemon::daemon::*;

#[derive(Parser)]
struct Cli {
    /// The human-readable name to give the Zenoh-Flow Runtime wrapped by this
    /// Daemon.
    ///
    /// To start a Zenoh-Flow Daemon, at least a name is required.
    name: Option<String>,
    /// The path of the configuration of the Zenoh-Flow Daemon.
    ///
    /// This configuration allows setting extensions supported by the Runtime
    /// and its name.
    #[arg(short, long, verbatim_doc_comment)]
    configuration: Option<PathBuf>,
    /// The path to a Zenoh configuration to manage the connection to the Zenoh
    /// network.
    ///
    /// If no configuration is provided, the Daemon will default to connecting as
    /// a peer with multicast scouting enabled.
    #[arg(short = 'z', long, verbatim_doc_comment)]
    zenoh_configuration: Option<PathBuf>,
}

#[async_std::main]
async fn main() {
    let _ = tracing_subscriber::fmt::try_init();

    let cli = Cli::parse();

    if cli.configuration.is_some() && cli.name.is_some() {
        tracing::error!("Please either specify a <NAME> or a <CONFIGURATION>, not both.");
        return;
    } else if cli.configuration.is_none() && cli.name.is_none() {
        tracing::error!("Please provide at least a <NAME> for this Zenoh-Flow Runtime.");
        return;
    }

    let zenoh_config = match cli.zenoh_configuration {
        Some(path) => zenoh::prelude::Config::from_file(path.clone()).unwrap_or_else(|e| {
            panic!(
                "Failed to parse the Zenoh configuration from < {} >:\n{e:?}",
                path.display()
            )
        }),
        None => zenoh::config::peer(),
    };

    let zenoh_session = zenoh::open(zenoh_config)
        .res_async()
        .await
        .unwrap_or_else(|e| panic!("Failed to open Zenoh session:\n{e:?}"))
        .into_arc();

    let daemon = match cli.configuration {
        Some(path) => {
            let (zenoh_flow_configuration, _) =
                try_parse_from_file::<ZenohFlowConfiguration>(&path, Vars::default())
                    .unwrap_or_else(|e| {
                        panic!(
                            "Failed to parse a Zenoh-Flow Configuration from < {} >:\n{e:?}",
                            path.display()
                        )
                    });

            Daemon::spawn_from_config(zenoh_session, zenoh_flow_configuration)
                .await
                .expect("Failed to spawn the Zenoh-Flow Daemon")
        }
        None => Daemon::spawn(
            Runtime::builder(cli.name.unwrap())
                .session(zenoh_session)
                .build()
                .await
                .expect("Failed to build the Zenoh-Flow Runtime"),
        )
        .await
        .expect("Failed to spawn the Zenoh-Flow Daemon"),
    };

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

                signal => {
                    tracing::warn!("Ignoring signal ({signal})");
                }
            }
        }
    })
    .await;
}
