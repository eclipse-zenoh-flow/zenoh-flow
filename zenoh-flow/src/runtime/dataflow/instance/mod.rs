//
// Copyright (c) 2022 ZettaScale Technology
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

pub mod runners;

use self::runners::Runner;
use super::DataFlow;
use crate::bail;
use crate::model::record::LinkRecord;
use crate::runtime::InstanceContext;
use crate::types::{Input, Inputs, NodeId, Output, Outputs};
use crate::zfresult::ErrorKind;
use crate::Result;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use uhlc::HLC;

/// TODO(J-Loudet) Improve documentation.
pub struct DataFlowInstance {
    pub(crate) _instance_context: Arc<InstanceContext>,
    pub(crate) data_flow: DataFlow,
    pub(crate) runners: HashMap<NodeId, Runner>,
}

impl Deref for DataFlowInstance {
    type Target = DataFlow;

    fn deref(&self) -> &Self::Target {
        &self.data_flow
    }
}

impl DataFlowInstance {
    /// TODO(J-Loudet) Improve documentation.
    pub fn get_sinks(&self) -> Vec<NodeId> {
        self.sink_factories.keys().cloned().collect()
    }

    /// TODO(J-Loudet) Improve documentation.
    pub fn get_sources(&self) -> Vec<NodeId> {
        self.source_factories.keys().cloned().collect()
    }

    /// TODO(J-Loudet) Improve documentation.
    pub fn get_operators(&self) -> Vec<NodeId> {
        self.operator_factories.keys().cloned().collect()
    }

    /// TODO(J-Loudet) Improve documentation.
    pub fn get_connectors(&self) -> Vec<NodeId> {
        self.connectors.keys().cloned().collect()
    }

    /// TODO(J-Loudet) Improve documentation.
    pub fn start_node(&mut self, node_id: &NodeId) -> Result<()> {
        if let Some(runner) = self.runners.get_mut(node_id) {
            runner.start();
            return Ok(());
        }

        bail!(
            ErrorKind::NodeNotFound(Arc::clone(node_id)),
            "Node < {} > not found",
            node_id
        )
    }

    /// TODO(J-Loudet) Improve documentation.
    pub async fn stop_node(&mut self, node_id: &NodeId) -> Result<()> {
        if let Some(runner) = self.runners.get_mut(node_id) {
            return runner.stop().await;
        }

        bail!(
            ErrorKind::NodeNotFound(Arc::clone(node_id)),
            "Node < {} > not found",
            node_id
        )
    }
}

/// Creates the [`Link`](`Link`) between the `nodes` using `links`.
///
/// # Errors
/// An error variant is returned in case of:
/// -  port id is duplicated.
pub(crate) fn create_links(
    nodes: &[NodeId],
    links: &[LinkRecord],
    hlc: Arc<HLC>,
) -> Result<HashMap<NodeId, (Inputs, Outputs)>> {
    let mut io: HashMap<NodeId, (Inputs, Outputs)> = HashMap::with_capacity(nodes.len());

    for link_desc in links {
        let upstream_node = link_desc.from.node.clone();
        let downstream_node = link_desc.to.node.clone();

        // Nodes have been filtered based on their runtime. If the runtime of either one of the node
        // is not equal to that of the current runtime, the channels should not be created.
        if !nodes.contains(&upstream_node) || !nodes.contains(&downstream_node) {
            continue;
        }

        // FIXME Introduce a user-configurable maximum capacity on the links. This also requires
        // implementing a dropping policy.
        let (tx, rx) = flume::unbounded();
        let from = link_desc.from.output.clone();
        let to = link_desc.to.input.clone();

        match io.get_mut(&upstream_node) {
            Some((_, outputs)) => {
                outputs
                    .entry(from.clone())
                    .or_insert_with(|| Output::new(from, hlc.clone()))
                    .add(tx);
            }
            None => {
                let inputs = HashMap::new();

                let mut output = Output::new(from.clone(), hlc.clone());
                output.add(tx);

                let outputs = HashMap::from([(from, output)]);

                io.insert(upstream_node, (inputs, outputs));
            }
        }

        match io.get_mut(&downstream_node) {
            Some((inputs, _)) => {
                inputs
                    .entry(to.clone())
                    .or_insert_with(|| Input::new(to))
                    .add(rx);
            }
            None => {
                let outputs = HashMap::new();

                let mut input = Input::new(to.clone());
                input.add(rx);

                let inputs = HashMap::from([(to, input)]);

                io.insert(downstream_node, (inputs, outputs));
            }
        }
    }

    Ok(io)
}
