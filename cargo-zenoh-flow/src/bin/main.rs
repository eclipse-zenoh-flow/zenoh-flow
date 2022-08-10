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

use async_std::process::exit;
use clap::Parser;
use colored::*;

use cargo_zenoh_flow::error::CZFError;
use zenoh_flow::model::node::{SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor};
use zenoh_flow::model::{NodeKind, RegistryNode, RegistryNodeArchitecture, RegistryNodeTag};
use zenoh_flow::types::NodeId;

#[cfg(feature = "local_registry")]
use async_std::sync::Arc;
#[cfg(feature = "local_registry")]
use rand::seq::SliceRandom;
#[cfg(feature = "local_registry")]
use zenoh::*;
#[cfg(feature = "local_registry")]
use zenoh_flow_registry::RegistryClient;
#[cfg(feature = "local_registry")]
use zenoh_flow_registry::RegistryFileClient;

#[derive(Debug)]
pub enum Languages {
    Rust,
    Python,
    Cpp,
}

impl std::str::FromStr for Languages {
    type Err = CZFError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "rust" => Ok(Self::Rust),
            "python" => Ok(Self::Python),
            "py" => Ok(Self::Python),
            "cpp" => Ok(Self::Cpp),
            "c++" => Ok(Self::Cpp),
            _ => Err(CZFError::LanguageNotCompatible(format!(
                "Language {s} is not supported. Supported languages are: rust, python, cpp"
            ))),
        }
    }
}

impl std::string::ToString for Languages {
    fn to_string(&self) -> String {
        match self {
            Self::Rust => String::from("rust"),
            Self::Python => String::from("python"),
            Self::Cpp => String::from("c++"),
        }
    }
}

#[derive(Parser, Debug)]
pub enum ZFCtl {
    Build {
        #[clap(short, long)]
        package: Option<String>,
        #[clap(short = 'm', long = "manifest-path", default_value = "Cargo.toml")]
        manifest_path: std::path::PathBuf,
        #[clap(short, long)]
        release: bool,
        #[clap(short = 't', long = "tag", default_value = "latest")]
        version_tag: String,
        cargo_build_flags: Vec<String>,
    },
    New {
        name: String,
        #[clap(short = 'k', long = "kind", default_value = "operator")]
        kind: NodeKind,
        #[clap(short = 'l', long = "l", default_value = "rust")]
        language: Languages,
    },
    List,
    Push {
        graph_id: String,
    },
    Pull {
        graph_id: String,
    },
}

#[async_std::main]
async fn main() {
    // `cargo zenoh-flow` invocation passes the `zenoh-flow` arg through.
    let mut args: Vec<String> = std::env::args().collect();
    args.remove(1);

    let args = ZFCtl::parse_from(args.iter());

    #[cfg(feature = "local_registry")]
    {
        let mut zconfig = zenoh::config::Config::default();

        match zconfig.set_mode(Some("peer".parse())) {
            Ok => (),
            Err(e) => {
                println!(
                    "{}: to create Zenoh session configuration: {:?}",
                    "error".red().bold(),
                    e
                );
                exit(-1);
            }
        };

        match zconfig.set_peers(vec!["unixsock-stream//tmp/zf-registry.sock".parse()]) {
            Ok => (),
            Err(e) => {
                println!(
                    "{}: to create Zenoh session configuration: {:?}",
                    "error".red().bold(),
                    e
                );
                exit(-1);
            }
        };

        let zsession = match zenoh::open(zconfig).await {
            Ok(zn) => Arc::new(zn),
            Err(e) => {
                println!("{}: to create Zenoh session: {:?}", "error".red().bold(), e);
                exit(-1);
            }
        };

        let servers = match RegistryClient::find_servers(zsession.clone()).await {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "{}: to create find registry servers: {:?}",
                    "error".red().bold(),
                    e
                );
                exit(-1);
            }
        };

        let client = match servers.choose(&mut rand::thread_rng()) {
            Some(entry_point) => {
                log::debug!("Selected entrypoint runtime: {:?}", entry_point);
                let client = RegistryClient::new(zsession.clone(), *entry_point);
                Some(client)
            }
            None => {
                println!(
                    "{}: unable to connect to local registry, node will not be uploaded.",
                    "warning".yellow().bold()
                );
                None
            }
        };

        let file_client = RegistryFileClient::from(zsession);
    }

    match args {
        ZFCtl::Build {
            package,
            manifest_path,
            release,
            version_tag,
            mut cargo_build_flags,
        } => {
            // `cargo zenoh-flow` invocation passes the `zenoh-flow` arg through.
            if cargo_build_flags
                .first()
                .map_or(false, |arg| arg == "zenoh-flow")
            {
                cargo_build_flags.remove(0);
            }

            let (node_info, target_dir, manifest_dir) =
                match cargo_zenoh_flow::utils::from_manifest(&manifest_path, package) {
                    Ok(res) => res,
                    Err(_e) => {
                        println!("{}: unable to parse Cargo.toml", "error".red().bold());
                        exit(-1);
                    }
                };
            let target = if release {
                format!(
                    "{}/release/{}{}{}",
                    target_dir.as_path().display(),
                    std::env::consts::DLL_PREFIX,
                    node_info.id,
                    std::env::consts::DLL_SUFFIX
                )
            } else {
                format!(
                    "{}/debug/{}{}{}",
                    target_dir.as_path().display(),
                    std::env::consts::DLL_PREFIX,
                    node_info.id,
                    std::env::consts::DLL_SUFFIX
                )
            };
            let uri = format!("file://{}", target);

            let (metadata_graph, _metadata_arch, descriptor) = match node_info.kind {
                NodeKind::Operator => {
                    if node_info.inputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing inputs for Operator node",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    if node_info.outputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing outputs for Operator node",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    let inputs = node_info.inputs.unwrap();
                    let outputs = node_info.outputs.unwrap();

                    if inputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty inputs for Operator, it should have at least one input", "error".red().bold());
                        exit(-1);
                    }

                    if outputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty outputs for Operator, it should have at least one output", "error".red().bold());
                        exit(-1);
                    }

                    let descriptor = SimpleOperatorDescriptor {
                        id: NodeId::from(node_info.id.clone()),
                        inputs: inputs.clone(),
                        outputs: outputs.clone(),
                        uri: Some(uri.clone()),
                        configuration: None,
                        tags: vec![],
                    };

                    let metadata_arch = RegistryNodeArchitecture {
                        arch: String::from(std::env::consts::ARCH),
                        os: String::from(std::env::consts::OS),
                        uri,
                        checksum: String::from(""),
                        signature: String::from(""),
                    };

                    let metadata_tag = RegistryNodeTag {
                        name: version_tag,
                        requirement_labels: vec![],
                        architectures: vec![metadata_arch.clone()],
                    };

                    let metadata_graph = RegistryNode {
                        id: NodeId::from(node_info.id.clone()),
                        kind: node_info.kind.clone(),
                        classes: vec![],
                        tags: vec![metadata_tag],
                        inputs,
                        outputs,
                        period: None,
                    };

                    let yml_descriptor = match serde_yaml::to_string(&descriptor) {
                        Ok(yml) => yml,
                        Err(e) => {
                            println!("{}: unable to serialize descriptor {}", "error".red(), e);
                            exit(-1)
                        }
                    };
                    (metadata_graph, metadata_arch, yml_descriptor)
                }
                NodeKind::Source => {
                    if node_info.inputs.is_some() {
                        println!("{}: Zenoh-Flow metadata has inputs for Source node, they will be discarded", "warning".yellow().bold());
                    }

                    if node_info.outputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing outputs for Source node",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    let outputs = node_info.outputs.unwrap();

                    if outputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty outputs for Source, it should exactly one output", "error".red().bold());
                        exit(-1);
                    }

                    if outputs.len() > 1 {
                        println!("{}: Zenoh-Flow metadata has more than one output for Source, it should exactly one output", "error".red().bold());
                        exit(-1);
                    }

                    let output = &outputs[0];

                    let descriptor = SourceDescriptor {
                        id: NodeId::from(node_info.id.clone()),
                        output: output.clone(),
                        uri: Some(uri.clone()),
                        configuration: None,
                        tags: vec![],
                    };

                    let metadata_arch = RegistryNodeArchitecture {
                        arch: String::from(std::env::consts::ARCH),
                        os: String::from(std::env::consts::OS),
                        uri,
                        checksum: String::from(""),
                        signature: String::from(""),
                    };

                    let metadata_tag = RegistryNodeTag {
                        name: version_tag,
                        requirement_labels: vec![],
                        architectures: vec![metadata_arch.clone()],
                    };

                    let metadata_graph = RegistryNode {
                        id: NodeId::from(node_info.id.clone()),
                        kind: node_info.kind.clone(),
                        classes: vec![],
                        tags: vec![metadata_tag],
                        inputs: vec![],
                        outputs: vec![output.clone()],
                        period: None,
                    };

                    let yml_descriptor = match serde_yaml::to_string(&descriptor) {
                        Ok(yml) => yml,
                        Err(e) => {
                            println!("{}: unable to serialize descriptor {}", "error".red(), e);
                            exit(-1)
                        }
                    };
                    (metadata_graph, metadata_arch, yml_descriptor)
                }
                NodeKind::Sink => {
                    if node_info.inputs.is_none() {
                        println!(
                            "{}: Zenoh-Flow metadata is missing inputs for Sink node",
                            "error".red().bold()
                        );
                        exit(-1);
                    }

                    if node_info.outputs.is_some() {
                        println!("{}: Zenoh-Flow metadata has outputs for Sink node, they will be discarded", "warning".yellow().bold());
                    }

                    let inputs = node_info.inputs.unwrap();

                    if inputs.is_empty() {
                        println!("{}: Zenoh-Flow metadata has empty inputs for Sink, it should exactly one inputs", "error".red().bold());
                        exit(-1);
                    }

                    if inputs.len() > 1 {
                        println!("{}: Zenoh-Flow metadata has more than one input for Sink, it should exactly one input", "error".red().bold());
                        exit(-1);
                    }

                    let input = &inputs[0];

                    let descriptor = SinkDescriptor {
                        id: NodeId::from(node_info.id.clone()),
                        input: input.clone(),
                        uri: Some(uri.clone()),
                        configuration: None,
                        tags: vec![],
                    };

                    let metadata_arch = RegistryNodeArchitecture {
                        arch: String::from(std::env::consts::ARCH),
                        os: String::from(std::env::consts::OS),
                        uri,
                        checksum: String::from(""),
                        signature: String::from(""),
                    };

                    let metadata_tag = RegistryNodeTag {
                        name: version_tag,
                        requirement_labels: vec![],
                        architectures: vec![metadata_arch.clone()],
                    };

                    let metadata_graph = RegistryNode {
                        id: NodeId::from(node_info.id.clone()),
                        kind: node_info.kind.clone(),
                        classes: vec![],
                        tags: vec![metadata_tag],
                        inputs: vec![input.clone()],
                        outputs: vec![],
                        period: None,
                    };
                    let yml_descriptor = match serde_yaml::to_string(&descriptor) {
                        Ok(yml) => yml,
                        Err(e) => {
                            println!("{}: unable to serialize descriptor {}", "error".red(), e);
                            exit(-1)
                        }
                    };
                    (metadata_graph, metadata_arch, yml_descriptor)
                }
            };
            println!(
                "{} Node {} - Kind {}",
                "Compiling".green().bold(),
                node_info.id,
                node_info.kind.to_string()
            );

            match cargo_zenoh_flow::utils::cargo_build(&cargo_build_flags, release, &manifest_dir) {
                Ok(_) => (),
                Err(_) => {
                    println!("{}: cargo build failed", "error".red().bold());
                    exit(-1);
                }
            }
            match cargo_zenoh_flow::utils::store_zf_metadata(&metadata_graph, &target_dir) {
                Ok(res) => {
                    println!("{} stored in {}", "Metadata".green().bold(), res.bold());
                }
                Err(e) => {
                    println!("{}: failed to store metadata {:?}", "error".red().bold(), e);
                    exit(-1);
                }
            };

            match cargo_zenoh_flow::utils::store_zf_descriptor(
                &descriptor,
                &target_dir,
                &node_info.id,
            ) {
                Ok(res) => {
                    println!("{} stored in {}", "Descriptor".green().bold(), res.bold());
                }
                Err(e) => {
                    println!(
                        "{}: failed to store descriptor {:?}",
                        "error".red().bold(),
                        e
                    );
                    exit(-1);
                }
            }

            #[cfg(feature = "local_registry")]
            if client.is_some() {
                println!(
                    "{} {} to local registry",
                    "Uploading".green().bold(),
                    node_info.id.bold()
                );
                client
                    .as_ref()
                    .unwrap()
                    .add_graph(metadata_graph)
                    .await
                    .unwrap()
                    .unwrap();
            }

            #[cfg(feature = "local_registry")]
            if client.is_some() {
                println!(
                    "{} {} to local registry",
                    "Uploading".green().bold(),
                    target.bold()
                );
                file_client
                    .send_node(
                        &std::path::PathBuf::from(target),
                        &node_info.id,
                        &metadata_arch,
                        &version_tag,
                    )
                    .await
                    .unwrap();
            }
            let build_target = if release {
                String::from("release")
            } else {
                String::from("debug")
            };
            println!(
                "{} [{}] node {} ",
                "Finished".green().bold(),
                build_target,
                node_info.id
            );
        }
        ZFCtl::New {
            name,
            kind,
            language,
        } => match language {
            Languages::Rust => {
                match cargo_zenoh_flow::utils::create_crate(&name, kind.clone()).await {
                    Ok(_) => {
                        println!(
                            "{} boilerplate for {} {} ",
                            "Created".green().bold(),
                            kind.to_string(),
                            name.bold()
                        );
                    }
                    Err(e) => {
                        println!(
                            "{}: failed to create boilerplate for {} {}: {}",
                            "error".red().bold(),
                            kind.to_string(),
                            name.bold(),
                            e
                        );
                    }
                }
            }
            Languages::Python => {
                match cargo_zenoh_flow::utils::create_python_module(&name, kind.clone()).await {
                    Ok(_) => {
                        println!(
                            "{} boilerplate for {} {} ",
                            "Created".green().bold(),
                            kind.to_string(),
                            name.bold()
                        );
                    }
                    Err(e) => {
                        println!(
                            "{}: failed to create boilerplate for {} {}: {}",
                            "error".red().bold(),
                            kind.to_string(),
                            name.bold(),
                            e
                        );
                    }
                }
            }
            Languages::Cpp => {
                match cargo_zenoh_flow::utils::create_cpp_node(&name, kind.clone()).await {
                    Ok(_) => {
                        println!(
                            "{} boilerplate for {} {} ",
                            "Created".green().bold(),
                            kind.to_string(),
                            name.bold()
                        );
                    }
                    Err(e) => {
                        println!(
                            "{}: failed to create boilerplate for {} {}: {}",
                            "error".red().bold(),
                            kind.to_string(),
                            name.bold(),
                            e
                        );
                    }
                }
            }
        },
        ZFCtl::List => {
            #[cfg(feature = "local_registry")]
            match client {
                Some(client) => {
                    println!("{:?}", client.get_all_graphs().await);
                }
                None => println!("Offline mode!"),
            }
            #[cfg(not(feature = "local_registry"))]
            println!("Offline mode!")
        }
        _ => unimplemented!("Not yet..."),
    }
}
