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

mod instance_command;
mod runtime_command;

use anyhow::anyhow;
use clap::{Parser, Subcommand};
use instance_command::InstanceCommand;
use rand::Rng;
use runtime_command::{get_all_runtimes, RuntimeCommand};
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{Result, RuntimeId};

const ZENOH_FLOW_INTERNAL_ERROR: &str = r#"
`zfctl` encountered a fatal internal error.

If the above error log does not help you troubleshoot the reason, you can contact us on:
- Discord:  https://discord.gg/CeJB5rxk9x
- GitHub:   https://github.com/eclipse-zenoh/zenoh-flow
"#;

/// Macro to facilitate the creation of a [Row](comfy_table::Row) where its contents are not of the same type.
#[macro_export]
macro_rules! row {
    (
        $( $cell: expr ),*
    ) => {
        comfy_table::Row::from(vec![ $( &$cell as &dyn std::fmt::Display ),*])
    };
}

#[derive(Parser)]
struct Zfctl {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// To manage a data flow instance.
    Instance {
        #[command(subcommand)]
        command: InstanceCommand,
        /// The unique identifier of the Zenoh-Flow runtime to contact.
        /// If no identifier is provided, a random Zenoh-Flow runtime is
        /// selected among those reachable.
        #[arg(short, long, verbatim_doc_comment)]
        runtime: Option<RuntimeId>,
    },

    /// To interact with a Zenoh-Flow runtime.
    #[command(subcommand)]
    Runtime(RuntimeCommand),
}

#[async_std::main]
async fn main() -> Result<()> {
    // TODO Configure tracing such that:
    // - if the environment variable RUST_LOG is set, it is applied,
    // let a = std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV);
    // - otherwise, provide a default that will only log INFO or above messages, for zfctl only.
    let _ = tracing_subscriber::fmt::try_init();

    let zfctl = Zfctl::parse();

    let session = zenoh::open(zenoh::config::peer())
        .res()
        .await
        .map_err(|e| anyhow!("Failed to open Zenoh session:\n{:?}", e))?;

    match zfctl.command {
        Command::Instance { command, runtime } => {
            let orchestrator_id = match runtime {
                Some(id) => id,
                None => {
                    let mut runtimes = get_all_runtimes(&session).await?;
                    let orchestrator =
                        runtimes.remove(rand::thread_rng().gen_range(0..runtimes.len()));
                    tracing::trace!(
                        "Orchestrator: < {} > (id: {})",
                        orchestrator.name,
                        orchestrator.id
                    );

                    orchestrator.id
                }
            };

            command.run(session, orchestrator_id).await
        }
        Command::Runtime(r) => r.run(&session).await,
    }
}
