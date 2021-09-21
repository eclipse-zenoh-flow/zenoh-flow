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

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::{CZFError, CZFResult};
use async_std::prelude::*;
use serde::Deserialize;
use std::process::Command;
use zenoh_flow::model::link::PortDescriptor;
use zenoh_flow::model::{ComponentKind, RegistryComponent};

pub static ZF_OUTPUT_DIRECTORY: &str = "zenoh-flow";

#[derive(Deserialize, Debug)]
pub struct CargoMetadata {
    packages: Vec<CargoMetadataPackage>,
    resolve: CargoMetadataResolve,
    #[serde(default)]
    workspace_members: Vec<String>,
    target_directory: String,
}

#[derive(Deserialize, Debug)]
pub struct CargoMetadataResolve {
    root: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct CargoMetadataPackage {
    pub id: String,
    pub name: String,
    pub targets: Vec<CargoMetadataTarget>,
    pub manifest_path: String,
}

#[derive(Deserialize, Debug)]
pub struct CargoMetadataTarget {
    pub name: String,
    pub kind: Vec<String>,
    pub crate_types: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Cargo {
    pub package: cargo_toml::Package<CargoPkgMetadata>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CargoPkgMetadata {
    pub zenohflow: Option<CargoZenohFlow>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct CargoZenohFlow {
    pub id: String,
    pub kind: ComponentKind,
    pub inputs: Option<Vec<PortDescriptor>>,
    pub outputs: Option<Vec<PortDescriptor>>,
}

pub fn from_manifest(
    manifest_path: &Path,
    package_name: Option<String>,
) -> CZFResult<(CargoZenohFlow, PathBuf, PathBuf)> {
    let metadata = read_metadata(manifest_path)?;
    let available_package_names = || {
        metadata
            .packages
            .iter()
            .filter(|p| metadata.workspace_members.iter().any(|w| w == &p.id))
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let root_package = if let Some(name) = package_name {
        metadata
            .packages
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| CZFError::PackageNotFoundInWorkspace(name, available_package_names()))
    } else {
        metadata
            .resolve
            .root
            .as_ref()
            .and_then(|root_id| metadata.packages.iter().find(|p| &p.id == root_id))
            .ok_or_else(|| CZFError::NoRootFoundInWorkspace(available_package_names()))
    }?;

    if !root_package
        .targets
        .iter()
        .map(|t| t.crate_types.iter().map(|ct| *ct == "cdylib").all(|e| e))
        .all(|e| e)
    {
        return Err(CZFError::CrateTypeNotCompatible(root_package.id.clone()));
    }

    let target_dir = Path::new(&metadata.target_directory);

    let manifest_path = Path::new(&root_package.manifest_path);

    let manifest_dir = manifest_path.parent().unwrap();
    let content = std::fs::read(&manifest_path)?;

    let metadata = toml::from_slice::<Cargo>(&content)?
        .package
        .metadata
        .ok_or_else(|| {
            CZFError::MissingField(
                root_package.id.clone(),
                "Missing package.metadata.zenohflow",
            )
        })?
        .zenohflow
        .ok_or_else(|| {
            CZFError::MissingField(
                root_package.id.clone(),
                "Missing package.metadata.zenohflow",
            )
        })?;

    Ok((metadata, target_dir.into(), manifest_dir.into()))
}

pub fn read_metadata(manifest_path: &Path) -> CZFResult<CargoMetadata> {
    let mut cmd = Command::new("cargo");
    cmd.arg("metadata");
    cmd.arg("--format-version=1");
    cmd.arg(format!("--manifest-path={}", manifest_path.display()));

    let output = cmd
        .output()
        .map_err(|e| CZFError::CommandFailed(e, "cargo (is it in your PATH?)"))?;
    if !output.status.success() {
        return Err(CZFError::CommandError(
            "cargo",
            "metadata".to_owned(),
            output.stderr,
        ));
    }

    let stdout = String::from_utf8(output.stdout).unwrap();
    let metadata = serde_json::from_str(&stdout)?;
    Ok(metadata)
}

pub async fn create_crate(name: &str, kind: ComponentKind) -> CZFResult<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("new");
    cmd.arg("--lib");
    cmd.arg(name);

    let output = cmd
        .output()
        .map_err(|e| CZFError::CommandFailed(e, "cargo (is it in your PATH?)"))?;
    if !output.status.success() {
        return Err(CZFError::CommandError(
            "cargo",
            "metadata".to_owned(),
            output.stderr,
        ));
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(format!("{}/Cargo.toml", name))
        .unwrap();

    let (cargo_template, lib_template) = match kind {
        ComponentKind::Operator => {
            let cargo_template = crate::templates::operator_template_cargo(name.to_string())?;
            let template = crate::templates::operator_template_lib(name.to_string())?;
            (cargo_template, template)
        }
        ComponentKind::Sink => {
            let cargo_template = crate::templates::sink_template_cargo(name.to_string())?;
            let template = crate::templates::sink_template_lib(name.to_string())?;
            (cargo_template, template)
        }
        ComponentKind::Source => {
            let cargo_template = crate::templates::source_template_cargo(name.to_string())?;
            let template = crate::templates::source_template_lib(name.to_string())?;
            (cargo_template, template)
        }
    };

    let lib_path = std::path::PathBuf::from(format!("{}/src/lib.rs", name));
    write!(file, "{}", cargo_template)?;
    drop(file);

    std::fs::remove_file(&lib_path)?;
    write_string_to_file(&lib_path, &lib_template).await?;

    Ok(())
}

pub async fn write_string_to_file(filename: &Path, content: &str) -> CZFResult<()> {
    let mut file = async_std::fs::File::create(filename).await.map_err(|e| {
        CZFError::GenericError(format!("Error when creating file {:?} {:?}", filename, e))
    })?;

    Ok(file.write_all(content.as_bytes()).await.map_err(|e| {
        CZFError::GenericError(format!("Error when writing to file {:?} {:?}", filename, e))
    })?)
}

pub fn cargo_build(flags: &[String], release: bool, manifest_dir: &Path) -> CZFResult<()> {
    let mut cmd = Command::new("cargo");
    cmd.current_dir(manifest_dir);
    cmd.arg("build");

    //cmd.current_dir(&options.manifest_dir);
    if release {
        cmd.arg("--release");
    }

    for flag in flags {
        cmd.arg(flag);
    }

    let status = cmd
        .status()
        .map_err(|e| CZFError::CommandFailed(e, "cargo"))?;
    if !status.success() {
        return Err(CZFError::BuildFailed);
    }

    Ok(())
}

pub fn store_zf_metadata(metadata: &RegistryComponent, target_dir: &Path) -> CZFResult<String> {
    let metadata_dir = PathBuf::from(format!("{}/{}", target_dir.display(), ZF_OUTPUT_DIRECTORY));

    if metadata_dir.exists() {
        std::fs::remove_dir_all(&metadata_dir)?;
    }

    std::fs::create_dir(&metadata_dir)?;

    let target_metadata = format!(
        "{}/{}/{}.yml",
        target_dir.display(),
        ZF_OUTPUT_DIRECTORY,
        metadata.id
    );
    let yml_metadata = serde_yaml::to_string(metadata)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target_metadata)?;

    write!(file, "{}", yml_metadata)?;

    Ok(target_metadata)
}

pub fn store_zf_descriptor(descriptor: &str, target_dir: &Path, id: &str) -> CZFResult<String> {
    let target_descriptor = format!(
        "{}/{}/descriptor-{}.yml",
        target_dir.display(),
        ZF_OUTPUT_DIRECTORY,
        id
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target_descriptor)?;

    write!(file, "{}", descriptor)?;

    Ok(target_descriptor)
}
