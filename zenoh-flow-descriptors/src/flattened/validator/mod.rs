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

use crate::FlattenedDataFlowDescriptor;
use anyhow::bail;
use std::collections::HashSet;
use zenoh_flow_commons::{NodeId, PortId, Result};

#[derive(Default)]
pub(crate) struct Validator<'a> {
    node_ids: HashSet<&'a NodeId>,
    outputs: HashSet<(&'a NodeId, &'a PortId)>,
    inputs: HashSet<(&'a NodeId, &'a PortId)>,
}

impl<'a> Validator<'a> {
    pub(crate) fn validate_node_id(&mut self, node_id: &'a NodeId) -> Result<()> {
        if !self.node_ids.insert(node_id) {
            bail!(
                "Two nodes share the same identifier: < {} >. The identifiers must be unique.",
                node_id
            );
        }

        Ok(())
    }

    pub(crate) fn validate_input(&mut self, node_id: &'a NodeId, input: &'a PortId) -> Result<()> {
        if !self.inputs.insert((node_id, input)) {
            bail!(
                "Node < {} > declares the following input (at least) twice: < {} >",
                node_id,
                input
            );
        }

        Ok(())
    }

    pub(crate) fn validate_output(
        &mut self,
        node_id: &'a NodeId,
        output: &'a PortId,
    ) -> Result<()> {
        if !self.outputs.insert((node_id, output)) {
            bail!(
                "Node < {} > declares the following output (at least) twice: < {} >",
                node_id,
                output
            );
        }

        Ok(())
    }

    pub(crate) fn validate(data_flow: &FlattenedDataFlowDescriptor) -> Result<()> {
        let mut this = Validator::default();

        if data_flow.sources.is_empty() {
            bail!("A data flow must specify at least ONE Source.");
        }

        if data_flow.sinks.is_empty() {
            bail!("A data flow must specify at least ONE Sink.");
        }

        for flat_source in &data_flow.sources {
            this.validate_node_id(&flat_source.id)?;

            for output in flat_source.outputs.iter() {
                this.validate_output(&flat_source.id, output)?;
            }
        }

        for flat_operator in &data_flow.operators {
            this.validate_node_id(&flat_operator.id)?;

            for output in flat_operator.outputs.iter() {
                this.validate_output(&flat_operator.id, output)?;
            }

            for input in flat_operator.inputs.iter() {
                this.validate_input(&flat_operator.id, input)?;
            }
        }

        for flat_sink in &data_flow.sinks {
            this.validate_node_id(&flat_sink.id)?;

            for input in flat_sink.inputs.iter() {
                this.validate_input(&flat_sink.id, input)?;
            }
        }

        let mut unused_inputs = this.inputs.clone();
        let mut unused_outputs = this.outputs.clone();

        for link in data_flow.links.iter() {
            if !this.outputs.contains(&(&link.from.node, &link.from.output)) {
                bail!(
                    r#"
The following `from` section of this link does not exist:
{}

Does the node < {} > exist?
Does it declare an output named < {} >?
"#,
                    link,
                    link.from.node,
                    link.from.output
                );
            }
            unused_outputs.remove(&(&link.from.node, &link.from.output));

            if !this.inputs.contains(&(&link.to.node, &link.to.input)) {
                bail!(
                    r#"
The following `to` section of this link does not exist:
{}

Does the node < {} > exist?
Does it declare an input named < {} >?
"#,
                    link,
                    link.to.node,
                    link.to.input
                );
            }
            unused_inputs.remove(&(&link.to.node, &link.to.input));
        }

        if !unused_inputs.is_empty() {
            let mut error_message = "The following inputs are not connected: ".to_string();
            for (node, input) in unused_inputs {
                error_message = format!("{}\n- {}: {}", error_message, node, input);
            }

            bail!(error_message);
        }

        if !unused_outputs.is_empty() {
            let mut error_message = "The following outputs are not connected:".to_string();
            for (node, output) in unused_outputs {
                error_message = format!("{}\n- {}: {}", error_message, node, output);
            }

            bail!(error_message);
        }

        Ok(())
    }
}

#[cfg(test)]
#[path = "./tests.rs"]
mod tests;
