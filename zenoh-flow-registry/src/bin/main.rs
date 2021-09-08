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
use structopt::StructOpt;
use zenoh_flow::model::operator::ZFOperatorDescriptor;
use zenoh_flow_registry::config::ComponentKind;
use zenoh_flow_registry::{
    ZFRegistryComponentArchitecture, ZFRegistryComponentTag, ZFRegistryGraph,
};

#[derive(StructOpt, Debug)]
pub enum ZFCtl {
    Build {
        #[structopt(short, long)]
        package: Option<String>,
        #[structopt(short = "m", long = "manifest-path", default_value = "Cargo.toml")]
        manifest_path: std::path::PathBuf,
        #[structopt(short, long)]
        release : bool,
        cargo_build_flags: Vec<String>,
    },
    New {
        name: String,
        #[structopt(short = "k", long = "kind", default_value = "operator")]
        kind: ComponentKind,
    },
}

#[async_std::main]
async fn main() {
    let args = ZFCtl::from_args();
    println!("Args: {:?}", args);

    match args {
        ZFCtl::Build {
            package,
            manifest_path,
            release,
            mut cargo_build_flags,
        } => {

            // `cargo deb` invocation passes the `deb` arg through.
            if cargo_build_flags.first().map_or(false, |arg| arg == "deb") {
                cargo_build_flags.remove(0);
            }

            let (component_info, target_dir, manifest_dir) =
                zenoh_flow_registry::config::from_manifest(&manifest_path, package).unwrap();

            println!("Target dir: {:?}", target_dir);
            println!("Manifest dir: {:?}", manifest_dir);

            let target = if release {
                format!("{}/release/{}{}{}", target_dir.as_path().display().to_string(), std::env::consts::DLL_PREFIX, component_info.id.clone(),std::env::consts::DLL_SUFFIX) }
                else {
                    format!("{}/debug/{}{}{}", target_dir.as_path().display().to_string(), std::env::consts::DLL_PREFIX, component_info.id.clone(),std::env::consts::DLL_SUFFIX)
                };
            let uri = format!("file://{}", target);

            let descriptor = ZFOperatorDescriptor {
                id: component_info.id.clone(),
                inputs: component_info.inputs.clone().unwrap(),
                outputs: component_info.outputs.clone().unwrap(),
                uri: Some(uri.clone()),
                configuration: None,
                runtime: None,
            };

            let metadata_arch = ZFRegistryComponentArchitecture {
                arch: String::from(std::env::consts::ARCH),
                os: String::from(std::env::consts::OS),
                uri: uri,
                checksum: String::from(""),
                signature: String::from(""),
            };

            let metadata_tag = ZFRegistryComponentTag {
                name: String::from("latest"),
                requirement_labels: vec![],
                architectures: vec![metadata_arch],
            };

            let metadata_graph = ZFRegistryGraph {
                id: component_info.id.clone(),
                classes: vec![],
                tags: vec![metadata_tag],
                inputs: component_info.inputs.clone().unwrap(),
                outputs: component_info.outputs.clone().unwrap(),
                period: None,
            };

            println!("Cargo.toml metadata: {:?}", component_info);
            println!("Derived Descriptor from metadata {:?}", descriptor);
            println!(
                "Derived Registry metadata from cargo metadata {:?}",
                metadata_graph
            );

            println!("Target: {:?}", target);

            zenoh_flow_registry::config::cargo_build(&cargo_build_flags, release, &manifest_dir).unwrap();
            zenoh_flow_registry::config::store_zf_metadata(&metadata_graph, &target_dir).unwrap();

        }
        ZFCtl::New { name, kind } => {
            zenoh_flow_registry::config::create_crate(&name, kind).unwrap();
        }
        _ => unreachable!(),
    }
}
