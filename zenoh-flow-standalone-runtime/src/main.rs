//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

use anyhow::Context;
use async_std::io::ReadExt;
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use zenoh_flow_commons::Vars;
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::{
    zenoh::{self, AsyncResolve},
    Loader, LoaderConfig, Runtime,
};

#[derive(Parser)]
struct Cli {
    /// The data flow to execute.
    flow: PathBuf,
    /// The, optional, location of the configuration to load nodes implemented not in Rust.
    #[arg(short, long, value_name = "path")]
    loader: Option<PathBuf>,
}

#[async_std::main]
async fn main() {
    let cli = Cli::parse();

    let loader_config = match cli.loader {
        Some(loader_config_path) => {
            let (config, _) = zenoh_flow_descriptors::try_load_from_file::<LoaderConfig>(
                loader_config_path.as_os_str(),
                Vars::default(),
            )
            .context(format!(
                "Failed to load Loader configuration from < {} >",
                &loader_config_path.display()
            ))
            .unwrap();

            config
        }
        None => LoaderConfig::empty(),
    };

    let loader = Loader::new(loader_config);

    let (data_flow, vars) = zenoh_flow_descriptors::try_load_from_file::<DataFlowDescriptor>(
        cli.flow.as_os_str(),
        Vars::default(),
    )
    .context(format!(
        "Failed to load data flow descriptor from < {} >",
        &cli.flow.display()
    ))
    .unwrap();

    let flattened_flow = FlattenedDataFlowDescriptor::try_flatten(data_flow, vars)
        .context(format!(
            "Failed to flattened data flow extracted from < {} >",
            &cli.flow.display()
        ))
        .unwrap();

    let hlc = Arc::new(uhlc::HLC::default());

    let session = zenoh::open(zenoh::peer()).res().await.unwrap().into_arc();
    let mut runtime = Runtime::new(loader, hlc, session);

    let record = DataFlowRecord::try_new(flattened_flow, runtime.id())
        .context("Failed to create a Record from the flattened data flow descriptor")
        .unwrap();

    let record_id = record.id().clone();
    runtime
        .try_instantiate_data_flow(record)
        .await
        .context("Failed to instantiate Record")
        .unwrap();

    let instance = runtime.get_instance_mut(&record_id).unwrap();

    // NOTE: The order in which the nodes are started matters. If the Sources start producing data while the downstream
    // nodes are not up (Operators and Sinks) then data theoretically could be lost.
    //
    // In practice, the channels were already created and data would be stored there.
    instance.start_sinks();
    instance.start_operators();
    instance.start_sources();

    let mut stdin = async_std::io::stdin();
    let mut input = [0_u8];
    println!(
        r#"
The flow < {} > was successfully started.

To gracefully stop its execution, simply press 'q'.
"#,
        instance.name()
    );

    loop {
        let _ = stdin.read_exact(&mut input).await;
        if input[0] == b'q' {
            break;
        }
    }

    // NOTE: Similarly to how the nodes are started, stopping them should be done from Sources to Sinks.
    instance.stop_sources().await;
    instance.stop_operators().await;
    instance.stop_sinks().await;
}
