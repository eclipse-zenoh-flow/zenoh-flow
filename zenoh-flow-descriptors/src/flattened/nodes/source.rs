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

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, sync::Arc};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, Result, RuntimeId, Vars};
use zenoh_keyexpr::OwnedKeyExpr;

use crate::{
    flattened::uri,
    nodes::source::{CustomSourceDescriptor, SourceVariants},
    SourceDescriptor, ZenohSourceDescriptor,
};

/// TODO@J-Loudet
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedSourceDescriptor {
    pub id: NodeId,
    pub description: Arc<str>,
    pub library: SourceLibrary,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    #[serde(default)]
    pub runtime: Option<RuntimeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceLibrary {
    Uri(Arc<str>),
    Zenoh(HashMap<PortId, OwnedKeyExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum LocalSourceVariants {
    Custom(CustomSourceDescriptor),
    Zenoh(ZenohSourceDescriptor),
}

impl Display for FlattenedSourceDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedSourceDescriptor {
    pub fn try_flatten(
        source_desc: SourceDescriptor,
        overwritting_vars: Vars,
        mut overwritting_configuration: Configuration,
    ) -> Result<Self> {
        let descriptor = match source_desc.variant {
            SourceVariants::Remote(remote_desc) => {
                let (descriptor, _) = uri::try_load_descriptor::<LocalSourceVariants>(
                    &remote_desc.descriptor,
                    overwritting_vars,
                )
                .context(format!(
                    "[{}] Failed to load source descriptor from < {} >",
                    source_desc.id, &remote_desc.descriptor
                ))?;
                overwritting_configuration = remote_desc
                    .configuration
                    .merge_overwrite(overwritting_configuration);

                descriptor
            }
            SourceVariants::Zenoh(zenoh_desc) => LocalSourceVariants::Zenoh(zenoh_desc),
            SourceVariants::Custom(custom_desc) => {
                overwritting_configuration = custom_desc
                    .clone()
                    .configuration
                    .merge_overwrite(overwritting_configuration);

                LocalSourceVariants::Custom(custom_desc)
            }
        };

        match descriptor {
            LocalSourceVariants::Custom(custom_source) => Ok(Self {
                id: source_desc.id,
                description: custom_source.description,
                library: SourceLibrary::Uri(custom_source.library),
                outputs: custom_source.outputs,
                configuration: overwritting_configuration
                    .merge_overwrite(custom_source.configuration),
                runtime: source_desc.runtime,
            }),
            LocalSourceVariants::Zenoh(zenoh_desc) => Ok(Self {
                id: source_desc.id,
                description: zenoh_desc.description,
                outputs: zenoh_desc.subscribers.keys().cloned().collect(),
                library: SourceLibrary::Zenoh(zenoh_desc.subscribers),
                configuration: Configuration::default(),
                runtime: source_desc.runtime,
            }),
        }
    }
}
