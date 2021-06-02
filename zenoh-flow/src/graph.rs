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

use crate::loader::{load_operator, ZFOperatorRunner};
use crate::{DataFlowDescription, ZFOperatorConnection, ZFOperatorDescription, ZFOperatorId};

pub struct DataFlowGraph {
    pub operators: Vec<(NodeIndex, ZFOperatorDescription)>,
    pub connections: Vec<(EdgeIndex, ZFOperatorConnection)>,
    pub graph: StableGraph<ZFOperatorDescription, usize>,
    pub operators_runners: HashMap<ZFOperatorId, Arc<Mutex<ZFOperatorRunner>>>,
}

impl DataFlowGraph {
    pub fn new(df: Option<DataFlowDescription>) -> Self {
        match df {
            Some(df) => {
                let mut graph = StableGraph::<ZFOperatorDescription, usize>::new();
                let mut operators = Vec::new();
                let mut connections = Vec::new();
                for o in df.operators {
                    operators.push((graph.add_node(o.clone()), o));
                }

                for l in df.connections {
                    let (from_i, _from_op) = match operators.iter().find(|&(_, o)| o.id == l.from) {
                        Some((idx, op)) => (idx, op),
                        None => panic!("From not found for {:?}", l),
                    };

                    let (to_i, _to_op) = match operators.iter().find(|&(_, o)| o.id == l.to) {
                        Some((idx, op)) => (idx, op),
                        None => panic!("To not found for {:?}", l),
                    };

                    connections.push((graph.add_edge(*from_i, *to_i, 1), l));
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
                graph: StableGraph::<ZFOperatorDescription, usize>::new(),
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
                let (operator_id, runner) = load_operator(op.lib.clone())?;
                self.operators_runners
                    .insert(operator_id, Arc::new(Mutex::new(runner)));
            }
            Ok(())
        }
    }

    pub async fn make_connections(&mut self) {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            let mut _up_runner = self.operators_runners.get(&up_op.id).unwrap().lock().await;
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
                    let mut _down_runner = self
                        .operators_runners
                        .get(&down_op.id)
                        .unwrap()
                        .lock()
                        .await;
                    // let ds = DataStream::new();
                    // let (tx, rx) = ds.split();

                    // up_runner.add_output(tx);
                    // down_runner.add_input(rx);
                }
            }
        }
    }

    pub fn get_runner(&self, operator_id: &ZFOperatorId) -> Option<&Arc<Mutex<ZFOperatorRunner>>> {
        self.operators_runners.get(operator_id)
    }

    pub fn get_runners(&self) -> Vec<Box<Arc<Mutex<ZFOperatorRunner>>>> {
        let mut runners = vec![];
        for (_, op) in &self.operators {
            runners.push(Box::new(Arc::clone(
                self.operators_runners.get(&op.id).unwrap(),
            )));
        }
        runners
    }
}
