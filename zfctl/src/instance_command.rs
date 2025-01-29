//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{anyhow, bail};
use clap::Subcommand;
use comfy_table::Table;
use itertools::Itertools;
use uuid::Uuid;
use zenoh::{query::ConsolidationMode, Session};
use zenoh_flow_commons::{parse_vars, InstanceId, Result, RuntimeId, Vars};
use zenoh_flow_daemon::queries::*;
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_runtime::InstanceState;

use super::ZENOH_FLOW_INTERNAL_ERROR;
use crate::row;

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
    ///       zfctl instance status <uuid>
    ///
    /// - This call will **not** start the data flow instance, only load on all
    ///   the involved daemons the nodes composing the data flow.
    ///
    /// - If Zenoh-Flow daemons are specified in the data flow descriptor,
    ///   there is no need to contact them separately or even to make the
    ///   `create` query on any of them. The Zenoh-Flow daemon orchestrating
    ///   the creation will query the appropriate Zenoh-Flow daemons.
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
    /// List all the data flow instances on the contacted Zenoh-Flow daemon
    List,
    /// Start the data flow instance, on all the involved Zenoh-Flow daemons.
    Start { instance_id: Uuid },
    /// Abort the data flow instance, on all the involved Zenoh-Flow daemons.
    Abort { instance_id: Uuid },
}

impl InstanceCommand {
    pub async fn run(self, session: Session, orchestrator_id: RuntimeId) -> Result<()> {
        let mut selector = selector_instances(&orchestrator_id);
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
                selector = selector_all_instances();
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
            .payload(value)
            // NOTE: We do not want any consolidation, each response, even if identical, is relevant as the origin
            // matters as much as the content.
            .consolidation(ConsolidationMode::None)
            .await
            .map_err(|e| anyhow!("Failed to send query on < {} >: {:?}", &selector, e))?;

        // Some requests require to process the response.
        match query {
            InstancesQuery::Create(_) => {
                let sample = match reply.recv_async().await {
                    Ok(reply) => reply,
                    Err(e) => {
                        tracing::error!("Could not create instance: {:?}", e);
                        bail!(ZENOH_FLOW_INTERNAL_ERROR)
                    }
                };

                match sample.result() {
                    Ok(sample) => match sample.payload().try_to_string() {
                        Ok(instance_id) => {
                            tracing::info!("Instance: {instance_id}");
                            tracing::info!(
                                "To check its status:\n\tzfctl instance status {instance_id}",
                            );
                        }
                        Err(e) => {
                            tracing::error!("Failed to parse Instance ID: {e:?}");
                        }
                    },
                    Err(err) => tracing::error!("Failed to create instance: {:?}", err),
                }
            }
            InstancesQuery::Status(_) => {
                let mut table = Table::new();
                table.set_width(80);
                table.set_header(row!("Runtime", "Instance State", "Node"));

                while let Ok(response) = reply.recv_async().await {
                    match response.result() {
                        Ok(sample) => {
                            match serde_json::from_slice::<InstanceStatus>(
                                &sample.payload().to_bytes(),
                            ) {
                                Ok(status) => {
                                    table.add_row(row!(
                                        status.runtime_id,
                                        status.state,
                                        status.nodes.iter().join(", ")
                                    ));
                                }
                                Err(e) => tracing::error!(
                                    "Failed to parse 'status' reply from < {:?} >: {:?}",
                                    response.replier_id(),
                                    e
                                ),
                            }
                        }
                        Err(err) => tracing::error!("{:?}", err),
                    }
                }

                println!("{table}");
            }
            InstancesQuery::List => {
                let mut table = Table::new();
                table.set_width(80);
                table.set_header(row!("Instance Name", "Instance ID", "Instance State"));
                while let Ok(response) = reply.recv_async().await {
                    match response.result() {
                        Ok(sample) => {
                            match serde_json::from_slice::<
                                HashMap<InstanceId, (Arc<str>, InstanceState)>,
                            >(&sample.payload().to_bytes())
                            {
                                Ok(list) => {
                                    for (id, (name, state)) in list {
                                        table.add_row(row!(name, id, state));
                                    }
                                }
                                Err(e) => tracing::error!(
                                    "Failed to parse 'list' reply from < {:?} >: {:?}",
                                    response.replier_id(),
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
