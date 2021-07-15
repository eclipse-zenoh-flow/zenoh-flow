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

use std::process;
use std::str;

use std::collections::HashMap;

use async_std::fs;
use async_std::path::Path;
use async_std::prelude::*;

use log::{error, info, trace};

mod runtime;
use runtime::{Runtime, RuntimeConfig};

static RUNTIME_CONFIG_FILE: &str = "/etc/zenoh-flow/runtime.yaml";

const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");

#[derive(Debug, StructOpt)]
#[structopt(name = "dpn")]
struct RuntimeOpt {
    #[structopt(short = "c", long = "configuration")]
    config: String,
}

async fn read_file(path: &Path) -> String {
    fs::read_to_string(path).await.unwrap()
}

async fn write_file(path: &Path, content: Vec<u8>) {
    let mut file = fs::File::create(path).await.unwrap();
    file.write_all(&content).await.unwrap();
    file.sync_all().await.unwrap();
}

#[async_std::main]
async fn main() {
    // Init logging
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let args = RuntimeOpt::from_args();

    let conf_file_path = Path::new(&args.config);
    let config =
        serde_yaml::from_str::<RuntimeConfig>(&(read_file(&conf_file_path).await)).unwrap();

    let mut rt = Runtime::from_config(config).unwrap();

    let h = rt.run();


    h.await.unwrap();

    println!("Hello, world!");
}
