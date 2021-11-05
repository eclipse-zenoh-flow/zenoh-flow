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

// TODO: this should become a deamon.

use async_std::sync::Arc;
use std::convert::TryFrom;
use std::fs::{File, *};
use std::io::Write;
use std::path::Path;
use structopt::StructOpt;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;

#[derive(Debug, StructOpt)]
#[structopt(name = "dpn")]
struct Opt {
    #[structopt(short = "g", long = "graph-file")]
    graph_file: String,
    #[structopt(short = "o", long = "out-file", default_value = "output.dot")]
    outfile: String,
    #[structopt(short = "l", long = "loader_config")]
    loader_config: Option<String>,
    #[structopt(short = "r", long = "runtime")]
    runtime: String,
}

fn _write_record_to_file(record: zenoh_flow::model::dataflow::DataFlowRecord, filename: &str) {
    let path = Path::new(filename);
    let mut write_file = File::create(path).unwrap();
    write!(write_file, "{}", record.to_yaml().unwrap()).unwrap();
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let opt = Opt::from_args();
    let yaml_df = read_to_string(opt.graph_file).unwrap();

    let loader_config = match opt.loader_config {
        Some(config) => {
            let yaml_conf = read_to_string(config).unwrap();
            serde_yaml::from_str::<LoaderConfig>(&yaml_conf).unwrap()
        }
        None => LoaderConfig { extentions: vec![] },
    };

    let session =
        async_std::sync::Arc::new(zenoh::net::open(zenoh::net::config::peer()).await.unwrap());
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let loader = Arc::new(Loader::new(loader_config));

    let ctx = RuntimeContext {
        session,
        hlc,
        loader,
        runtime_name: opt.runtime.clone().into(),
        runtime_uuid: uuid::Uuid::new_v4(),
    };

    // loading the descriptor
    let df = zenoh_flow::model::dataflow::DataFlowDescriptor::from_yaml(&yaml_df).unwrap();

    // mapping to infrastructure
    let mapped = zenoh_flow::runtime::map_to_infrastructure(df, &opt.runtime)
        .await
        .unwrap();

    // creating record
    let dfr =
        zenoh_flow::model::dataflow::DataFlowRecord::try_from((mapped, uuid::Uuid::nil())).unwrap();

    _write_record_to_file(dfr.clone(), "computed-record.yaml");

    // creating dataflow
    let dataflow = zenoh_flow::runtime::dataflow::Dataflow::try_new(ctx.clone(), dfr).unwrap();

    // instantiating
    let instance =
        zenoh_flow::runtime::dataflow::instance::DataflowInstance::try_instantiate(dataflow)
            .unwrap();
    let mut managers = vec![];

    let mut sinks = instance.get_sinks();
    for runner in sinks.drain(..) {
        let m = runner.start();
        managers.push(m);
    }

    let mut operators = instance.get_operators();
    for runner in operators.drain(..) {
        let m = runner.start();
        managers.push(m);
    }

    let mut connectors = instance.get_connectors();
    for runner in connectors.drain(..) {
        let m = runner.start();
        managers.push(m);
    }

    let mut sources = instance.get_sources();
    for runner in sources.drain(..) {
        let m = runner.start();
        managers.push(m);
    }

    let () = std::future::pending().await;
}
