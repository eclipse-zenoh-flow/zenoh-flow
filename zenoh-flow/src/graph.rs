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

use async_std::sync::{Arc, Mutex};
use petgraph::dot::{Config, Dot};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use std::collections::HashMap;

use crate::loader::{load_operator, load_source, load_sink, ZFOperatorRunner, ZFSourceRunner, ZFSinkRunner};
use crate::{DataFlowDescription, ZFOperatorConnection, ZFOperatorDescription, ZFOperatorId};
use crate::types::{ZFLinkId, ZFResult, ZFOperatorKind};
use crate::link::{link, ZFLinkReceiver, ZFLinkSender};
use crate::operator::DataTrait;
use crate::message::ZFMessage;



pub enum Runner {
    Operator(ZFOperatorRunner),
    Source(ZFSourceRunner),
    Sink(ZFSinkRunner)
}

impl Runner {

    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            Runner::Operator(runner) => runner.run().await,
            Runner::Source(runner) => runner.run().await,
            Runner::Sink(runner) => runner.run().await,
        }
    }


    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>)  {

        match self {
            Runner::Operator(runner) => runner.add_input(input),
            Runner::Source(runner) => panic!("Sources does not have inputs!"),
            Runner::Sink(runner) =>  runner.add_input(input),
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        match self {
            Runner::Operator(runner) => runner.add_output(output),
            Runner::Source(runner) => runner.add_output(output),
            Runner::Sink(runner) =>  panic!("Sinks does not have output!"),
        }
    }

}


pub struct DataFlowGraph {
    pub operators: Vec<(NodeIndex, ZFOperatorDescription)>,
    pub connections: Vec<(EdgeIndex, ZFOperatorConnection)>,
    pub graph: StableGraph<ZFOperatorDescription, ZFLinkId>,
    pub operators_runners: HashMap<ZFOperatorId, Arc<Mutex<Runner>>>,
}







pub fn deserialize_dataflow_description(data: String) -> DataFlowDescription {
    serde_yaml::from_str::<DataFlowDescription>(&data).unwrap()
}

impl DataFlowGraph {
    pub fn new(df: Option<DataFlowDescription>) -> Self {
        match df {
            Some(df) => {
                let mut graph = StableGraph::<ZFOperatorDescription, ZFLinkId>::new();
                let mut operators = Vec::new();
                let mut connections = Vec::new();
                for o in df.operators {
                    operators.push((graph.add_node(o.clone()), o));
                }

                for l in df.connections {

                    // First check if the LinkId are the same
                    if l.from.1 == l.to.1 {
                        let (from_i, _from_op, from_l_id) = match operators.iter().find(|&(_, o)| o.id == l.from.0) {
                            Some((idx, op)) => {
                                match op.outputs.iter().find(|&oid| *oid == l.from.1) {
                                    Some(oid) => ((idx, op, oid)),
                                    None => panic!("LinkID From not found for {:?}", l),
                                }
                            }
                            None => panic!("From not found for {:?}", l),
                        };

                        let (to_i, _to_op, to_l_id) = match operators.iter().find(|&(_, o)| o.id == l.to.0) {
                            Some((idx, op)) => {
                                match op.inputs.iter().find(|&iid| *iid == l.from.1) {
                                    Some(iid) => ((idx, op, iid)),
                                    None => panic!("LinkID To not found for {:?}", l),
                                }
                            }
                            None => panic!("To not found for {:?}", l),
                        };

                        connections.push((graph.add_edge(*from_i, *to_i, 1), l));
                    } else  {
                        panic!("Ports ID does not match")
                    }



                }

                Self {
                    operators,
                    connections,
                    graph,
                    operators_runners: HashMap::new(),
                }
            }
            None => Self {
                operators: Vec::new(),
                connections: Vec::new(),
                graph: StableGraph::<ZFOperatorDescription, ZFLinkId>::new(),
                operators_runners: HashMap::new(),
            },
        }
    }

    pub fn to_dot_notation(&self) -> String {
        String::from(format!(
            "{}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        ))
    }

    pub fn load(&mut self) -> std::io::Result<()> {
        unsafe {
            for (_, op) in &self.operators {

                match op.kind {
                    ZFOperatorKind::Compute => {
                        let (operator_id, runner) = load_operator(op.lib.clone())?;
                        let runner = Runner::Operator(runner);
                        self.operators_runners
                            .insert(operator_id, Arc::new(Mutex::new(runner)));
                    },
                    ZFOperatorKind::Source => {
                        let (operator_id, runner) = load_source(op.lib.clone())?;
                        let runner = Runner::Source(runner);
                        self.operators_runners
                            .insert(operator_id, Arc::new(Mutex::new(runner)));
                    },
                    ZFOperatorKind::Sink => {
                        let (operator_id, runner) = load_sink(op.lib.clone())?;
                        let runner = Runner::Sink(runner);
                        self.operators_runners
                            .insert(operator_id, Arc::new(Mutex::new(runner)));
                    }
                }
            }
            Ok(())
        }
    }

    pub async fn make_connections(&mut self) {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            let mut up_runner = self.operators_runners.get(&up_op.id).unwrap().lock().await;
            if self.graph.contains_node(*idx) {
                let mut downstreams = self
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .detach();
                while let Some(down) = downstreams.next(&self.graph) {
                    let (_, jdx) = down;
                    let down_op = match self.operators.iter().find(|&(idx, _)| *idx == jdx) {
                        Some((_, op)) => op,
                        None => panic!("To not found"),
                    };
                    let (_,down_link) = match self.graph.find_edge(*idx, jdx) {
                        Some(l_idx) => self.connections.iter().find(|&(lidx, _)| *lidx == l_idx).unwrap(),
                        None => panic!("Link between {:?} -> {:?} not found", idx, jdx),
                    };

                    let link_id = down_link.from.1; //The check on the link id matching is done when creating the graph


                    let mut down_runner = self
                        .operators_runners
                        .get(&down_op.id)
                        .unwrap()
                        .lock()
                        .await;

                    let (tx, rx) = link::<ZFMessage>(1024, link_id);

                    up_runner.add_output(tx);
                    down_runner.add_input(rx);
                }
            }
        }
    }

    pub fn get_runner(&self, operator_id: &ZFOperatorId) -> Arc<Mutex<Runner>> {
        self.operators_runners.get(operator_id).unwrap().clone()
    }

    pub fn get_runners(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];
        for (_, op) in &self.operators {
            runners.push(
                self.operators_runners.get(&op.id).unwrap().clone(),
            );
        }
        runners
    }
}
