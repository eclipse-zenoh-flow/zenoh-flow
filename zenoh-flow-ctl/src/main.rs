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

#![allow(clippy::upper_case_acronyms)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate prettytable;

extern crate base64;
extern crate exitfailure;

use git_version::git_version;
use prettytable::Table;
use rand::seq::SliceRandom;
use std::collections::HashSet;
use std::fs::read_to_string;
use structopt::StructOpt;
use uuid::Uuid;
use zenoh::*;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::runtime::resources::ZFDataStore;
use zenoh_flow::runtime::ZFRuntimeClient;

const GIT_VERSION: &str = git_version!(prefix = "v", cargo_prefix = "v");

#[derive(StructOpt, Debug)]
pub enum AddKind {
    Flow {
        #[structopt(parse(from_os_str), name = "Flow descriptor path")]
        descriptor_path: std::path::PathBuf,
    },
    // Instance {
    //     flow_id: String,
    //     #[structopt(short = "r", long = "rt-id", name = "Runtime UUID")]
    //     rt_id: Option<Uuid>,
    // },
    Instance {
        #[structopt(parse(from_os_str), name = "Flow descriptor path")]
        descriptor_path: std::path::PathBuf,
    },
}

#[derive(StructOpt, Debug)]
pub enum GetKind {
    Flow { id: Option<String> },
    Instance { id: Option<Uuid> },
    Runtime { id: Option<Uuid> },
}

#[derive(StructOpt, Debug)]
pub enum DeleteKind {
    Flow { id: String },
    Instance { id: Uuid },
}

#[derive(StructOpt, Debug)]
pub enum ZFCtl {
    Add(AddKind),
    Get(GetKind),
    Delete(DeleteKind),
}

#[async_std::main]
async fn main() {
    env_logger::init();
    log::debug!("Eclipse Zenoh-Flow Ctl {}", GIT_VERSION);

    let args = ZFCtl::from_args();
    log::debug!("Args: {:?}", args);

    let znsession = Arc::new(
        zenoh::net::open(Properties::from(String::from("mode=peer")).into())
            .await
            .unwrap(),
    );

    let servers = ZFRuntimeClient::find_servers(znsession.clone())
        .await
        .unwrap();
    let entry_point = servers.choose(&mut rand::thread_rng()).unwrap();
    log::debug!("Selected entrypoint runtime: {:?}", entry_point);
    let client = ZFRuntimeClient::new(znsession, *entry_point);

    let zsession = Arc::new(
        zenoh::Zenoh::new(Properties::from(String::from("mode=peer")).into())
            .await
            .unwrap(),
    );

    let store = ZFDataStore::new(zsession);

    match args {
        ZFCtl::Add(ak) => match ak {
            AddKind::Flow { descriptor_path } => {
                println!(
                    "This is going to store the flow described in {:?}",
                    descriptor_path
                );
            }
            AddKind::Instance { descriptor_path } => {
                log::debug!(
                    "This is going to store the flow described in {:?}",
                    descriptor_path
                );
                let yaml_df = read_to_string(descriptor_path).unwrap();
                let df =
                    zenoh_flow::model::dataflow::DataFlowDescriptor::from_yaml(yaml_df).unwrap();

                let record = client.instantiate(df).await.unwrap().unwrap();
                log::debug!("Instantiated: {:?}", record);
                println!("{}", record.uuid);
            } // AddKind::Instance { flow_id, rt_id } => {
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
                match id {
                    Some(rid) => {
                        let instance = store.get_flow_by_instance(rid).await.unwrap();
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
                                .iter()
                                .map(|o| format!("{}", o))
                                .collect::<Vec<String>>()
                                .join("\n"),
                            instance
                                .sinks
                                .iter()
                                .map(|o| format!("{}", o))
                                .collect::<Vec<String>>()
                                .join("\n"),
                            instance
                                .sources
                                .iter()
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
                    }
                    None => {
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
                }
                table.printstd();
            }
            GetKind::Runtime { id } => {
                let mut table = Table::new();
                match id {
                    Some(rtid) => {
                        let runtime_info = store.get_runtime_info(rtid).await.unwrap();
                        let runtime_status = store.get_runtime_status(rtid).await.unwrap();
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
                    }
                    None => {
                        table.add_row(row!["UUID", "Name", "Status",]);
                        let runtimes = store.get_all_runtime_info().await.unwrap();
                        for r in runtimes {
                            table.add_row(row![r.id, r.name, format!("{:?}", r.status),]);
                        }
                    }
                }
                table.printstd();
            }
        },
        ZFCtl::Delete(dk) => match dk {
            DeleteKind::Flow { id } => {
                println!("This is going to delete the flow {:?}", id);
            }
            DeleteKind::Instance { id } => {
                log::debug!("This is going to delete the instance {:?}", id);
                let record = client.teardown(id).await.unwrap().unwrap();
                log::debug!("Deleted: {:?}", record);
                println!("{}", record.uuid);
            }
        },
    }
}