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

use crate::{
    flattened::uri,
    nodes::sink::{CustomSinkDescriptor, SinkVariants},
    SinkDescriptor, ZenohSinkDescriptor,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, sync::Arc};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, Result, RuntimeId, Vars};
use zenoh_keyexpr::OwnedKeyExpr;

/// TODO@J-Loudet
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedSinkDescriptor {
    pub id: NodeId,
    pub description: Arc<str>,
    pub library: SinkLibrary,
    pub inputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
    #[serde(default)]
    pub runtime: Option<RuntimeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SinkLibrary {
    Uri(Arc<str>),
    Zenoh(HashMap<PortId, OwnedKeyExpr>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
enum LocalSinkVariants {
    Custom(CustomSinkDescriptor),
    Zenoh(ZenohSinkDescriptor),
}

impl Display for FlattenedSinkDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedSinkDescriptor {
    pub fn try_flatten(
        sink_desc: SinkDescriptor,
        overwritting_vars: Vars,
        mut overwritting_configuration: Configuration,
    ) -> Result<Self> {
        let descriptor = match sink_desc.variant {
            SinkVariants::Remote(remote_desc) => {
                let (descriptor, _) = uri::try_load_descriptor::<LocalSinkVariants>(
                    &remote_desc.descriptor,
                    overwritting_vars,
                )
                .context(format!(
                    "[{}] Failed to load sink descriptor from < {} >",
                    sink_desc.id, &remote_desc.descriptor
                ))?;

                overwritting_configuration = remote_desc
                    .configuration
                    .merge_overwrite(overwritting_configuration);

                descriptor
            }
            SinkVariants::Zenoh(zenoh_desc) => LocalSinkVariants::Zenoh(zenoh_desc),
            SinkVariants::Custom(custom_desc) => {
                overwritting_configuration = custom_desc
                    .clone()
                    .configuration
                    .merge_overwrite(overwritting_configuration);
                LocalSinkVariants::Custom(custom_desc)
            }
        };

        match descriptor {
            LocalSinkVariants::Custom(custom_sink) => Ok(Self {
                id: sink_desc.id,
                description: custom_sink.description,
                library: SinkLibrary::Uri(custom_sink.library),
                inputs: custom_sink.inputs,
                configuration: overwritting_configuration
                    .merge_overwrite(custom_sink.configuration),
                runtime: sink_desc.runtime,
            }),
            LocalSinkVariants::Zenoh(zenoh_desc) => Ok(Self {
                id: sink_desc.id,
                description: zenoh_desc.description,
                inputs: zenoh_desc.publishers.keys().cloned().collect(),
                library: SinkLibrary::Zenoh(zenoh_desc.publishers),
                configuration: Configuration::default(),
                runtime: sink_desc.runtime,
            }),
        }
    }
}
