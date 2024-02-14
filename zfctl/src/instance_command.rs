//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use crate::row;

use super::ZENOH_FLOW_INTERNAL_ERROR;

use std::path::PathBuf;

use anyhow::{anyhow, bail};
use clap::Subcommand;
use comfy_table::Table;
use uuid::Uuid;
use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{parse_vars, Result, RuntimeId, Vars};
use zenoh_flow_daemon::{selectors, InstanceStatus, InstancesQuery, Origin};
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};

#[derive(Subcommand)]
pub(crate) enum InstanceCommand {
    /// Create an instance of the provided data flow descriptor.
    ///
    /// The unique identifier of the instance is then echoed in the terminal.
    ///
    /// NOTES:
    ///
    /// - This call returns the unique identifier before the creation is
    ///   completed. To verify the status, one can call the following:
    ///
    ///       `zfctl instance status <uuid>`
    ///
    /// - This call will **not** start the data flow instance, only load on all
    ///   the involved runtimes the nodes composing the data flow.
    ///
    /// - If Zenoh-Flow runtimes are specified in the data flow descriptor,
    ///   there is no need to contact them separately or even to make the
    ///   `create` query on any of them. The Zenoh-Flow runtime orchestrating
    ///   the creation will query the appropriate Zenoh-Flow runtimes.
    #[command(verbatim_doc_comment)]
    Create {
        /// The path, on your machine, of the data flow descriptor.
        flow: PathBuf,
        /// Variables to add / overwrite in the `vars` section of your data
        /// flow, with the form `KEY=VALUE`. Can be repeated multiple times.
        ///
        /// Example:
        ///     --vars HOME_DIR=/home/zenoh-flow --vars BUILD=debug
        #[arg(long, value_parser = parse_vars::<String, String>, verbatim_doc_comment)]
        vars: Option<Vec<(String, String)>>,
    },
    /// To delete (and abort, if required) the data flow instance
    Delete { instance_id: Uuid },
    /// Obtain the status of the data flow instance.
    Status { instance_id: Uuid },
    /// List all the data flow instances on the contacted Zenoh-Flow runtime
    List,
    /// Start the data flow instance, on all the involved Zenoh-Flow runtimes.
    Start { instance_id: Uuid },
    /// Abort the data flow instance, on all the involved Zenoh-Flow runtimes.
    Abort { instance_id: Uuid },
}

impl InstanceCommand {
    pub async fn run(self, session: Session, orchestrator_id: RuntimeId) -> Result<()> {
        let mut selector = selectors::selector_instances(&orchestrator_id).map_err(|e| {
            tracing::error!(
                "Failed to generate a valid Zenoh key expression for runtime with id < {} >:\n{:?}",
                &orchestrator_id,
                e
            );
            anyhow!(ZENOH_FLOW_INTERNAL_ERROR)
        })?;
        let query = match self {
            InstanceCommand::Create { flow, vars } => {
                let vars = match vars {
                    Some(v) => Vars::from(v),
                    None => Vars::default(),
                };

                tracing::trace!("Path to data flow descriptor is: {}", flow.display());
                let (data_flow_desc, vars) =
                    zenoh_flow_commons::try_parse_from_file::<DataFlowDescriptor>(&flow, vars)
                        .map_err(|e| {
                            tracing::error!("{:?}", e);
                            anyhow!("Failed to parse data flow from < {} >", flow.display())
                        })?;

                let flat_flow = FlattenedDataFlowDescriptor::try_flatten(data_flow_desc, vars)
                    .map_err(|e| {
                        tracing::error!("{:?}", e);
                        anyhow!("Failed to flatten data flow < {} >", flow.display())
                    })?;

                InstancesQuery::Create(Box::new(flat_flow))
            }

            InstanceCommand::Delete { instance_id } => InstancesQuery::Delete {
                origin: Origin::Client,
                instance_id: instance_id.into(),
            },

            InstanceCommand::List => InstancesQuery::List,

            InstanceCommand::Status { instance_id } => {
                selector = selectors::selector_all_instances().unwrap();
                InstancesQuery::Status(instance_id.into())
            }
            InstanceCommand::Start { instance_id } => InstancesQuery::Start {
                origin: Origin::Client,
                instance_id: instance_id.into(),
            },

            InstanceCommand::Abort { instance_id } => InstancesQuery::Abort {
                origin: Origin::Client,
                instance_id: instance_id.into(),
            },
        };

        let value = serde_json::to_vec(&query).map_err(|e| {
            tracing::error!(
                r#"serde_json failed to serialize query: {:?}
Caused by:
{:?}
"#,
                query,
                e
            );
            anyhow!(ZENOH_FLOW_INTERNAL_ERROR)
        })?;

        let reply = session
            .get(&selector)
            .with_value(value)
            // NOTE: We do not want any consolidation, each response, even if identical, is relevant as the origin
            // matters as much as the content.
            .consolidation(ConsolidationMode::None)
            .res()
            .await
            .map_err(|e| anyhow!("Failed to send query on < {} >: {:?}", &selector, e))?;

        // Some requests require to process the response.
        match query {
            InstancesQuery::Create(_) => {
                let sample = match reply.recv_async().await {
                    Ok(reply) => reply.sample,
                    Err(e) => {
                        tracing::error!("Could not create instance: {:?}", e);
                        bail!(ZENOH_FLOW_INTERNAL_ERROR)
                    }
                };

                match sample {
                    Ok(sample) => {
                        tracing::info!(
                            "If successful, the instance will have the id: {}",
                            sample.value
                        );
                        println!("{}", sample.value);
                    }
                    Err(err) => tracing::error!("Failed to create instance: {:?}", err),
                }
            }
            InstancesQuery::Status(_) => {
                let mut table = Table::new();
                table.set_width(80);
                table.set_header(row!("Runtime", "Instance State", "Node"));

                while let Ok(response) = reply.recv_async().await {
                    match response.sample {
                        Ok(sample) => {
                            match serde_json::from_slice::<InstanceStatus>(
                                &sample.value.payload.contiguous(),
                            ) {
                                Ok(mut status) => {
                                    if let Some(node_id) = status.nodes.pop() {
                                        table.add_row(row!(
                                            status.runtime_id,
                                            status.state,
                                            node_id
                                        ));
                                        status.nodes.iter().for_each(|node_id| {
                                            table.add_row(row!("", "", node_id));
                                        });
                                    }
                                }
                                Err(e) => tracing::error!(
                                    "Failed to parse 'status' reply from < {} >: {:?}",
                                    response.replier_id,
                                    e
                                ),
                            }
                        }
                        Err(err) => tracing::error!("{:?}", err),
                    }
                }

                println!("{table}");
            }
            _ => {}
        }

        Ok(())
    }
}
