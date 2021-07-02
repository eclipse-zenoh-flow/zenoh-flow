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

use std::fs;
use std::fs::File;
use std::io::prelude::*;
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

#[async_std::main]
async fn main() {
    env_logger::init();

    let opt = Opt::from_args();
    let yaml_df = fs::read_to_string(opt.graph_file).unwrap();

    let df = zenoh_flow::runtime::graph::deserialize_dataflow_description(yaml_df);

    let mut dataflow_graph = zenoh_flow::runtime::graph::DataFlowGraph::new(Some(df));

    let dot_notation = dataflow_graph.to_dot_notation();

    let mut file = File::create(opt.outfile).unwrap();
    write!(file, "{}", dot_notation).unwrap();
    file.sync_all().unwrap();

    dataflow_graph.load(&opt.runtime).unwrap();

    dataflow_graph.make_connections(&opt.runtime).await;

    // // let inputs = dataflow_graph.get_inputs();
    let runners = dataflow_graph.get_runners();
    for runner in runners {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let () = std::future::pending().await;
}
