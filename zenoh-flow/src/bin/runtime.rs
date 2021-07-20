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

use std::fs::{File, *};
use std::io::Write;
use std::path::Path;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "dpn")]
struct Opt {
    #[structopt(short = "g", long = "graph-file")]
    graph_file: String,
    #[structopt(short = "o", long = "out-file", default_value = "output.dot")]
    outfile: String,
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

    // loading the descriptor
    let df = zenoh_flow::model::dataflow::DataFlowDescriptor::from_yaml(yaml_df).unwrap();

    // mapping to infrastructure
    let mapped = zenoh_flow::runtime::map_to_infrastructure(df, &opt.runtime)
        .await
        .unwrap();

    // creating record
    let dfr =
        zenoh_flow::model::dataflow::DataFlowRecord::from_dataflow_descriptor(mapped).unwrap();

    _write_record_to_file(dfr.clone(), "computed-record.yaml");

    // creating graph
    let mut dataflow_graph =
        zenoh_flow::runtime::graph::DataFlowGraph::from_dataflow_record(dfr).unwrap();

    let dot_notation = dataflow_graph.to_dot_notation();

    let mut file = File::create(opt.outfile).unwrap();
    write!(file, "{}", dot_notation).unwrap();
    file.sync_all().unwrap();

    // instantiating
    dataflow_graph.load(&opt.runtime).unwrap();

    dataflow_graph.make_connections(&opt.runtime).await.unwrap();

    // let mut runners = dataflow_graph.get_sinks();
    // for runner in runners.drain(..) {
    //     async_std::task::spawn(async move {
    //         let mut runner = runner.lock().await;
    //         runner.run().await.unwrap();
    //     });
    // }

    let mut sinks = dataflow_graph.get_sinks();
    for runner in sinks.drain(..) {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let mut operators = dataflow_graph.get_operators();
    for runner in operators.drain(..) {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let mut connectors = dataflow_graph.get_connectors();
    for runner in connectors.drain(..) {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let mut sources = dataflow_graph.get_sources();
    for runner in sources.drain(..) {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let () = std::future::pending().await;
}
