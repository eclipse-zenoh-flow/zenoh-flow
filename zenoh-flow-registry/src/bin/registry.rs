//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
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

use structopt::StructOpt;

use async_ctrlc::CtrlC;
use std::{convert::TryFrom, str};

use async_std::fs;
use async_std::path::Path;
use async_std::prelude::*;
use zenoh_flow_registry::registry::{RegistryConfig, ZFRegistry};

static REGISTRY_CONFIG_FILE: &str = "/etc/zenoh-flow/runtime.yaml";
const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");

#[derive(Debug, StructOpt)]
struct RegistryOpt {
    #[structopt(
        short = "c",
        long = "configuration",
        default_value = REGISTRY_CONFIG_FILE
    )]
    config: String,
    #[structopt(short = "v", long = "version")]
    print_version: bool,
}

async fn read_file(path: &Path) -> String {
    fs::read_to_string(path).await.unwrap()
}

#[async_std::main]
async fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = RegistryOpt::from_args();

    if args.print_version {
        println!("Zenoh Flow runtime version: {}", GIT_VERSION);
        std::process::exit(0);
    }

    let conf_file_path = Path::new(&args.config);
    let conf = RegistryConfig::try_from(read_file(conf_file_path).await).unwrap();

    let registry = ZFRegistry::try_from(conf).unwrap();

    let (s, h) = registry.start().await.unwrap();

    let ctrlc = CtrlC::new().expect("Unable to create Ctrl-C handler");
    let mut stream = ctrlc.enumerate().take(1);
    stream.next().await;
    log::trace!("Received Ctrl-C start teardown");

    //Here we send the stop signal to the registry object and waits that it ends
    registry.stop(s).await.unwrap();

    //wait for the futures to ends
    h.await.unwrap();
}
