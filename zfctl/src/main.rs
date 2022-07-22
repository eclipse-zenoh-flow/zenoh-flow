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

#![allow(clippy::upper_case_acronyms)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate prettytable;

extern crate base64;
extern crate exitfailure;

use clap::{Parser, Subcommand};
use git_version::git_version;
use prettytable::Table;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::error::Error;
use std::fs::read_to_string;
use uuid::Uuid;
use zenoh::Session;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::runtime::resources::DataStore;
use zenoh_flow::runtime::RuntimeClient;
const GIT_VERSION: &str = git_version!(prefix = "v", cargo_prefix = "v");

const DEFAULT_ZENOH_CFG: &str = ".config/zenoh-flow/zfctl-zenoh.json";
const ENV_ZENOH_CFG: &str = "ZFCTL_CFG";

#[derive(Subcommand, Debug)]
#[clap(about = "Creates new entities in Zenoh Flow")]
pub enum CreateKind {
    #[clap(about = "Adds a new flow into the Zenoh Flow registry")]
    Flow {
        #[clap(
            parse(from_os_str),
            name = "Flow descriptor path",
            help = "Upload the flow in the registry (Unimplemented)"
        )]
        descriptor_path: std::path::PathBuf,
    },
    // When registry will be in place the code below will be used
    // #[clap(about = "Creates a new instance for the given flow")]
    // Instance {
    //     #[clap(
    //         name = "Flow Id",
    //         help = "Creates a new instance for the given flow"
    //     )]
    //     flow_id: String,
    //     #[clap(short, long, help = "Starts the flow after it is created")]
    //     start: bool,
    // },
    #[clap(about = "Creates a new instance for the given flow")]
    Instance {
        #[clap(
            parse(from_os_str),
            name = "Flow descriptor path",
            help = "Creates a new instance for the given flow"
        )]
        descriptor_path: std::path::PathBuf,
    },
}

#[derive(Subcommand, Debug)]
#[clap(about = "Starts entities in Zenoh Flow")]
pub enum StartKind {
    #[clap(about = "Starts the given node in the given instance")]
    Node {
        #[clap(
            short,
            long,
            name = "instance uuid",
            help = "The instance containing the node"
        )]
        instance_id: Uuid,
        #[clap(short, long, name = "node id", help = "The node identifier")]
        node_id: String,
    },
    #[clap(
        about = "Start a replay for the given source, in the given instance, using the given key_expr for retrieving the data"
    )]
    // Replay {
    //     #[clap(
    //         short,
    //         long,
    //         name = "instance uuid",
    //         help = "The instance containing the source"
    //     )]
    //     instance_id: Uuid,
    //     #[clap(short, long, name = "source id", help = "The source identifier")]
    //     source_id: String,
    //     #[clap(
    //         short,
    //         long,
    //         name = "zenoh key expression",
    //         help = "The key expression where the record is stored"
    //     )]
    //     key_expr: String,
    // },
    // #[clap(
    //     about = "Starts recording the given source, in the given instance, returns the key expression containing the recording"
    // )]
    // Record {
    //     #[clap(
    //         short,
    //         long,
    //         name = "instance uuid",
    //         help = "The instance containing the source"
    //     )]
    //     instance_id: Uuid,
    //     #[clap(short, long, name = "source id", help = "The source identifier")]
    //     source_id: String,
    // },
    #[clap(about = "Starts the given flow instance")]
    Instance {
        #[clap(name = "instance uuid", help = "The instance to be started")]
        instance_id: Uuid,
    },
}

#[derive(Subcommand, Debug)]
#[clap(about = "Stop entities in Zenoh Flow")]
pub enum StopKind {
    #[clap(about = "Stops the given node in the given instance")]
    Node {
        #[clap(
            short,
            long,
            name = "instance uuid",
            help = "The instance containing the node"
        )]
        instance_id: Uuid,
        #[clap(short, long, name = "node id", help = "The node identifier")]
        node_id: String,
    },
    #[clap(about = "Stops the given replay, for the given source in the given instance")]
    // Replay {
    //     #[clap(
    //         short,
    //         long,
    //         name = "instance uuid",
    //         help = "The instance containing the source"
    //     )]
    //     instance_id: Uuid,
    //     #[clap(short, long, name = "source id", help = "The source identifier")]
    //     node_id: String,
    //     #[clap(short, long, name = "replay id", help = "The reply identifier")]
    //     replay_id: String,
    // },
    // #[clap(
    //     about = "Stops recording the given source, in the given instance, returns the key expression containing the recording"
    // )]
    // Record {
    //     #[clap(
    //         short,
    //         long,
    //         name = "instance uuid",
    //         help = "The instance containing the source"
    //     )]
    //     instance_id: Uuid,
    //     #[clap(short, long, name = "source id", help = "The source identifier")]
    //     node_id: String,
    // },
    #[clap(about = "Stops the given flow instance")]
    Instance {
        #[clap(name = "instance uuid", help = "The instance to be stopped")]
        instance_id: Uuid,
    },
}

#[derive(Subcommand, Debug)]
#[clap(about = "Gets information about entities from Zenoh Flow")]
pub enum GetKind {
    #[clap(about = "Gets information about the given flow")]
    Flow {
        #[clap(name = "flow identifier", help = "The flow you are interested in")]
        id: String,
    },
    #[clap(about = "Gets information about the given instance")]
    Instance {
        #[clap(name = "instance uuid", help = "The instance you are interested in")]
        id: Uuid,
    },
    #[clap(about = "Gets information about the given runtime")]
    Runtime {
        #[clap(name = "runtime uuid", help = "The runtime you are interested in")]
        id: Uuid,
    },
}

#[derive(Subcommand, Debug)]
#[clap(about = "Lists entities in Zenoh Flow")]
pub enum ListKind {
    #[clap(about = "Lists the exiting flows")]
    Flows,
    #[clap(about = "Lists the exiting instances")]
    Instances,
    #[clap(about = "Lists the runtimes")]
    Runtimes,
}

#[derive(Subcommand, Debug)]
#[clap(about = "Deletes entities from Zenoh Flow")]
pub enum DeleteKind {
    #[clap(about = "Deletes the given flow")]
    Flow {
        #[clap(short, long, name = "flow identifier", help = "The flow to be deleted")]
        id: String,
    },
    #[clap(about = "Deletes the given instance")]
    Instance {
        #[clap(name = "instance uuid", help = "The instance to be deleted")]
        id: Uuid,
    },
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]

pub enum ZFCtl {
    #[clap(subcommand)]
    Create(CreateKind),
    #[clap(subcommand)]
    Delete(DeleteKind),
    #[clap(subcommand)]
    Get(GetKind),
    #[clap(subcommand)]
    List(ListKind),
    #[clap(subcommand)]
    Start(StartKind),
    #[clap(subcommand)]
    Stop(StopKind),
    #[clap(about = "Creates and starts a flow instance")]
    Launch {
        #[clap(
            parse(from_os_str),
            name = "Flow descriptor path",
            help = "Flow to be started"
        )]
        descriptor_path: std::path::PathBuf,
    },
    #[clap(about = "Stops and deletes a flow instance")]
    Destroy {
        #[clap(name = "instance uuid", help = "The instance to be destroyed")]
        id: Uuid,
    },
}

#[async_std::main]
async fn main() {
    env_logger::try_init().unwrap_or_else(|_| log::warn!("`env_logger` already initialized"));
    log::debug!("Eclipse Zenoh-Flow Ctl {}", GIT_VERSION);

    let args = ZFCtl::parse();
    log::debug!("Args: {:?}", args);

    let zsession = Arc::new(get_zenoh().await.unwrap());

    let store = DataStore::new(zsession.clone());

    match args {
        ZFCtl::Create(ak) => match ak {
            CreateKind::Flow { descriptor_path } => {
                println!(
                    "This is going to store the flow described in {:?}",
                    descriptor_path
                );
            }
            CreateKind::Instance { descriptor_path } => {
                log::trace!(
                    "This is going to store the flow described in {:?}",
                    descriptor_path
                );
                let yaml_df = read_to_string(descriptor_path).unwrap();
                let df = zenoh_flow::model::dataflow::descriptor::DataFlowDescriptor::from_yaml(
                    &yaml_df,
                )
                .unwrap();
                let df = df.flatten().await.unwrap();

                df.validate().unwrap();

                let client = get_client(zsession.clone()).await;
                let record = client.create_instance(df).await.unwrap().unwrap();
                log::debug!("Created: {:?}", record);
                println!("{}", record.uuid);
            } // When registry will be in place the code below will be used
              // AddKind::Instance { flow_id, rt_id } => {
              //     println!(
              //         "This is going to instantiate the flow {} in runtime {:?}",
              //         flow_id, rt_id
              //     );
              // }
        },
        ZFCtl::Get(gk) => match gk {
            GetKind::Flow { id } => {
                println!("This is going to get information for the flow {:?}", id);
            }
            GetKind::Instance { id } => {
                log::debug!("This is going to get information for the instance {:?}", id);
                let mut table = Table::new();
                let instance = store.get_flow_by_instance(&id).await.unwrap();
                table.add_row(row![
                    "UUID",
                    "Flow",
                    "Operators",
                    "Sinks",
                    "Sources",
                    "Connectors",
                    "Links",
                ]);
                table.add_row(row![
                    instance.uuid,
                    instance.flow,
                    instance
                        .operators
                        .values()
                        .map(|o| format!("{}", o))
                        .collect::<Vec<String>>()
                        .join("\n"),
                    instance
                        .sinks
                        .values()
                        .map(|o| format!("{}", o))
                        .collect::<Vec<String>>()
                        .join("\n"),
                    instance
                        .sources
                        .values()
                        .map(|o| format!("{}", o))
                        .collect::<Vec<String>>()
                        .join("\n"),
                    instance
                        .connectors
                        .iter()
                        .map(|o| format!("{}", o))
                        .collect::<Vec<String>>()
                        .join("\n"),
                    instance
                        .links
                        .iter()
                        .map(|o| format!("{}", o))
                        .collect::<Vec<String>>()
                        .join("\n")
                ]);

                table.printstd();
            }
            GetKind::Runtime { id } => {
                let mut table = Table::new();
                let runtime_info = store.get_runtime_info(&id).await.unwrap();
                let runtime_status = store.get_runtime_status(&id).await.unwrap();
                table.add_row(row![
                    "UUID",
                    "Name",
                    "Status",
                    "Running Flows",
                    "Running Operators",
                    "Running Sources",
                    "Running Sinks",
                    "Running Connectors"
                ]);
                table.add_row(row![
                    runtime_status.id,
                    runtime_info.name,
                    format!("{:?}", runtime_status.status),
                    runtime_status.running_flows,
                    runtime_status.running_operators,
                    runtime_status.running_sources,
                    runtime_status.running_sinks,
                    runtime_status.running_connectors,
                ]);
                table.printstd();
            }
        },
        ZFCtl::Delete(dk) => match dk {
            DeleteKind::Flow { id } => {
                println!("This is going to delete the flow {:?}", id);
            }
            DeleteKind::Instance { id } => {
                log::debug!("This is going to delete the instance {:?}", id);
                let client = get_client(zsession.clone()).await;
                let record = client.delete_instance(id).await.unwrap().unwrap();

                log::debug!("Deleted: {:?}", record);
                println!("{}", record.uuid);
            }
        },
        ZFCtl::Start(sk) => match sk {
            StartKind::Node {
                instance_id,
                node_id,
            } => {
                let mut table = Table::new();
                let client = get_client(zsession.clone()).await;
                table.add_row(row!["UUID", "Name", "Status",]);
                client
                    .start_node(instance_id, node_id.clone())
                    .await
                    .unwrap()
                    .unwrap();
                table.add_row(row![instance_id, node_id, String::from("Running"),]);
                table.printstd();
            }
            // StartKind::Record {
            //     instance_id,
            //     source_id,
            // } => {
            //     let mut table = Table::new();
            //     let client = get_client(zsession.clone()).await;
            //     table.add_row(row!["UUID", "Name", "Key Expression",]);
            //     let key_expr = client
            //         .start_record(instance_id, source_id.clone().into())
            //         .await
            //         .unwrap()
            //         .unwrap();
            //     table.add_row(row![instance_id, source_id, key_expr,]);
            //     table.printstd();
            // }
            // StartKind::Replay {
            //     instance_id,
            //     source_id,
            //     key_expr,
            // } => {
            //     let mut table = Table::new();
            //     let client = get_client(zsession.clone()).await;
            //     table.add_row(row!["UUID", "Name", "Replay Id",]);
            //     let replay_id = client
            //         .start_replay(instance_id, source_id.clone().into(), key_expr)
            //         .await
            //         .unwrap()
            //         .unwrap();
            //     table.add_row(row![instance_id, source_id, replay_id,]);
            //     table.printstd();
            // }
            StartKind::Instance { instance_id } => {
                log::debug!("This is going to start the instance {:?}", instance_id);
                let client = get_client(zsession.clone()).await;
                client.start_instance(instance_id).await.unwrap().unwrap();
                log::debug!("Started: {:?}", instance_id);
                println!("{}", instance_id);
            }
        },
        ZFCtl::Stop(sk) => match sk {
            StopKind::Node {
                instance_id,
                node_id,
            } => {
                let mut table = Table::new();
                let client = get_client(zsession.clone()).await;
                table.add_row(row!["UUID", "Name", "Status",]);
                client
                    .stop_node(instance_id, node_id.clone())
                    .await
                    .unwrap()
                    .unwrap();
                table.add_row(row![instance_id, node_id, String::from("Stopped"),]);
                table.printstd();
            }
            // StopKind::Record {
            //     instance_id,
            //     node_id,
            // } => {
            //     let mut table = Table::new();
            //     let client = get_client(zsession.clone()).await;
            //     table.add_row(row!["UUID", "Name", "Key Expression",]);
            //     let key_expr = client
            //         .stop_record(instance_id, node_id.clone().into())
            //         .await
            //         .unwrap()
            //         .unwrap();
            //     table.add_row(row![instance_id, node_id, key_expr,]);
            //     table.printstd();
            // }
            // StopKind::Replay {
            //     instance_id,
            //     node_id,
            //     replay_id,
            // } => {
            //     let mut table = Table::new();
            //     table.add_row(row!["UUID", "Name", "Replay Id",]);
            //     let client = get_client(zsession.clone()).await;
            //     let replay_id = client
            //         .stop_replay(
            //             instance_id,
            //             node_id.clone().into(),
            //             replay_id.clone().into(),
            //         )
            //         .await
            //         .unwrap()
            //         .unwrap();
            //     table.add_row(row![instance_id, node_id, replay_id,]);
            //     table.printstd();
            // }
            StopKind::Instance { instance_id } => {
                log::debug!("This is going to stop the instance {:?}", instance_id);
                let client = get_client(zsession.clone()).await;
                let record = client.stop_instance(instance_id).await.unwrap().unwrap();
                log::debug!("stopeed: {:?}", record);
                println!("{}", record.uuid);
            }
        },
        ZFCtl::List(lk) => {
            let mut table = Table::new();
            match lk {
                ListKind::Flows => {
                    panic!("Unimlemented")
                }
                ListKind::Instances => {
                    let instances = store.get_all_instances().await.unwrap();
                    table.add_row(row![
                        "UUID",
                        "Flow",
                        "# Operators",
                        "# Sinks",
                        "# Sources",
                        "# Connectors",
                        "# Links",
                    ]);
                    let instances: HashSet<_> = instances.iter().collect();
                    for instance in instances {
                        table.add_row(row![
                            instance.uuid,
                            instance.flow,
                            instance.operators.len(),
                            instance.sinks.len(),
                            instance.sources.len(),
                            instance.connectors.len(),
                            instance.links.len(),
                        ]);
                    }
                }
                ListKind::Runtimes => {
                    table.add_row(row!["UUID", "Name", "Status",]);
                    let runtimes = store.get_all_runtime_info().await.unwrap();
                    for r in runtimes {
                        table.add_row(row![r.id, r.name, format!("{:?}", r.status),]);
                    }
                }
            };
            table.printstd();
        }
        ZFCtl::Launch { descriptor_path } => {
            log::debug!(
                "This is going to launch the flow described in {:?}",
                descriptor_path
            );
            let yaml_df = read_to_string(descriptor_path).unwrap();
            let df =
                zenoh_flow::model::dataflow::descriptor::DataFlowDescriptor::from_yaml(&yaml_df)
                    .unwrap();
            let df = df.flatten().await.unwrap();

            df.validate().unwrap();

            let client = get_client(zsession.clone()).await;
            let record = client.instantiate(df).await.unwrap().unwrap();
            log::debug!("Launched: {:?}", record);
            println!("{}", record.uuid);
        }
        ZFCtl::Destroy { id } => {
            log::debug!("This is going to destroy the instance {}", id);
            let client = get_client(zsession.clone()).await;
            let record = client.teardown(id).await.unwrap().unwrap();
            log::debug!("Destroyed: {:?}", record);
            println!("{}", record.uuid);
        }
    }
}

async fn get_zenoh() -> Result<Session, Box<dyn Error + Send + Sync + 'static>> {
    let z_config_file = std::env::var(ENV_ZENOH_CFG).ok().unwrap_or_else(|| {
        // FIXME: Replace with `std::env::home_dir` when it gets fixed + remove dependency to dirs.
        let mut config_path = dirs::home_dir().expect("Could not get $HOME directory, aborting.");
        config_path.push(DEFAULT_ZENOH_CFG);
        config_path
            .into_os_string()
            .into_string()
            .expect("Invalid unicode data found while trying to get `zftcl-zenoh.json`")
    });
    let zconfig = zenoh::config::Config::from_file(z_config_file)?;

    Ok(zenoh::open(zconfig).await.unwrap())
}

async fn get_client(zsession: Arc<Session>) -> RuntimeClient {
    let servers = RuntimeClient::find_servers(zsession.clone()).await.unwrap();
    let entry_point = servers.choose(&mut rand::thread_rng()).unwrap();
    log::debug!("Selected entrypoint runtime: {:?}", entry_point);
    RuntimeClient::new(zsession.clone(), *entry_point)
}
