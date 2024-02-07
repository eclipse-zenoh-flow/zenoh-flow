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
use zenoh_flow_commons::{parse_vars, RuntimeId, Vars};
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::{
    zenoh::{self, AsyncResolve},
    Extensions, Loader, Runtime,
};

#[derive(Parser)]
struct Cli {
    /// The data flow to execute.
    flow: PathBuf,
    /// The, optional, location of the configuration to load nodes implemented not in Rust.
    #[arg(short, long, value_name = "path")]
    extensions: Option<PathBuf>,
    /// Variables to add / overwrite in the `vars` section of your data
    /// flow, with the form `KEY=VALUE`. Can be repeated multiple times.
    ///
    /// Example:
    ///     --vars HOME_DIR=/home/zenoh-flow --vars BUILD=debug
    #[arg(long, value_parser = parse_vars::<String, String>, verbatim_doc_comment)]
    vars: Option<Vec<(String, String)>>,
}

#[async_std::main]
async fn main() {
    let cli = Cli::parse();

    let extensions = match cli.extensions {
        Some(extensions_path) => {
            let (extensions, _) = zenoh_flow_commons::try_load_from_file::<Extensions>(
                extensions_path.as_os_str(),
                Vars::default(),
            )
            .context(format!(
                "Failed to load Loader configuration from < {} >",
                &extensions_path.display()
            ))
            .unwrap();

            extensions
        }
        None => Extensions::default(),
    };

    let loader = Loader::new(extensions);

    let vars = match cli.vars {
        Some(v) => Vars::from(v),
        None => Vars::default(),
    };

    let (data_flow, vars) =
        zenoh_flow_commons::try_load_from_file::<DataFlowDescriptor>(cli.flow.as_os_str(), vars)
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
    let runtime = Runtime::new(
        RuntimeId::rand(),
        "zenoh-flow-standalone-runtime".into(),
        loader,
        hlc,
        session,
    );

    let record = DataFlowRecord::try_new(&flattened_flow, runtime.id())
        .context("Failed to create a Record from the flattened data flow descriptor")
        .unwrap();

    let instance_id = record.instance_id().clone();
    let record_name = record.name().clone();
    runtime
        .try_load_data_flow(record)
        .await
        .context("Failed to load Record")
        .unwrap();

    runtime
        .try_start_instance(&instance_id)
        .await
        .unwrap_or_else(|e| panic!("Failed to start data flow < {} >: {:?}", &instance_id, e));

    let mut stdin = async_std::io::stdin();
    let mut input = [0_u8];
    println!(
        r#"
    The flow ({}) < {} > was successfully started.

    To abort its execution, simply enter 'q'.
    "#,
        record_name, instance_id
    );

    loop {
        let _ = stdin.read_exact(&mut input).await;
        if input[0] == b'q' {
            break;
        }
    }

    runtime
        .try_delete_instance(&instance_id)
        .await
        .unwrap_or_else(|e| panic!("Failed to delete data flow < {} >: {:?}", &instance_id, e));
}
