//
// Copyright (c) 2022 ZettaScale Technology
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

use clap::Parser;

use async_ctrlc::CtrlC;
use async_std::prelude::*;
use std::convert::TryFrom;
use std::path::Path;
use std::str;

mod daemon;
mod util;
mod work;
use daemon::Daemon;
use daemon::DaemonConfig;
use util::read_file;

/// Default path for the runtime configuration file.
static RUNTIME_CONFIG_FILE: &str = "/etc/zenoh-flow/runtime.yaml";

/// Git version (commit id) for the runtime.
const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");

/// The CLI parameters accepted by the runtime.
#[derive(Debug, Parser)]
#[clap(name = "dpn")]
struct RuntimeOpt {
    #[clap(short = 'c', long = "configuration", default_value = RUNTIME_CONFIG_FILE)]
    config: String,
    #[clap(short = 'v', long = "version")]
    print_version: bool,
    #[clap(short = 'i', long = "node_uuid")]
    node_uuid: bool,
}

/// The runtime main function.
///
/// It initializes the `env_logger`, parses the arguments and then creates
/// a runtime.
///
/// It is able to catch SIGINT for graceful termination.
#[async_std::main]
async fn main() {
    // Init logging
    env_logger::try_init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    )
    .unwrap_or_else(|_| log::warn!("`env_logger` already initialized"));

    let args = RuntimeOpt::parse();

    if args.print_version {
        println!("Zenoh Flow runtime version: {}", GIT_VERSION);
        std::process::exit(0);
    }

    if args.node_uuid {
        println!("{}", daemon::get_machine_uuid().unwrap());
        std::process::exit(0);
    }

    let conf_file_path = Path::new(&args.config);
    let config =
        serde_yaml::from_str::<DaemonConfig>(&(read_file(conf_file_path).unwrap())).unwrap();

    let rt = Daemon::try_from(config).unwrap();

    let (s, h) = rt.start().await.unwrap();

    let ctrlc = CtrlC::new().expect("Unable to create Ctrl-C handler");
    let mut stream = ctrlc.enumerate().take(1);
    stream.next().await;
    log::trace!("Received Ctrl-C start teardown");

    //Here we send the stop signal to the rt object and waits that it ends
    rt.stop(s).await.unwrap();

    //wait for the futures to ends
    h.await.unwrap();
}
