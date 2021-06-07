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
}
#[async_std::main]
async fn main() {

    let opt = Opt::from_args();
    let yaml_df = fs::read_to_string(opt.graph_file).unwrap();

    let df = zenoh_flow::graph::deserialize_dataflow_description(yaml_df);

    let mut dataflow_graph = zenoh_flow::graph::DataFlowGraph::new(Some(df));

    let dot_notation = dataflow_graph.to_dot_notation();

    let mut file = File::create(opt.outfile).unwrap();
    write!(file, "{}", dot_notation);
    file.sync_all().unwrap();

    dataflow_graph.load().unwrap();

    dataflow_graph.make_connections();

    // // let inputs = dataflow_graph.get_inputs();
    let runners = dataflow_graph.get_runners();

    for runner in runners {
        async_std::task::spawn(
            async move {
                let mut runner  = runner.lock().await;
                runner.run().await;
            }
        );
    }




    let () = std::future::pending().await;
}
