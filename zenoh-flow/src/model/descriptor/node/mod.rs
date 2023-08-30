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

pub mod operator;

pub use operator::{CompositeOperatorDescriptor, OperatorDescriptor};
use std::path::PathBuf;
pub mod sink;
pub use sink::SinkDescriptor;
pub mod source;
pub use source::SourceDescriptor;

use crate::model::descriptor::{LinkDescriptor, Vars};
use crate::model::{Middleware, ZFUri};
use crate::runtime::dataflow::instance::builtin::zenoh::{
    get_zenoh_sink_descriptor, get_zenoh_source_descriptor,
};
use crate::types::configuration::Merge;
use crate::types::{Configuration, NodeId};
use crate::utils::parse_uri;
use crate::zfresult::ErrorKind;
use crate::{bail, zferror, Result};
use serde::{Deserialize, Serialize};

/// Describes an node of the graph
///
/// ```yaml
/// id : PrintSink
/// descriptor: file://./target/release/counter_source.yaml
/// configuration:
///   start: 10
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub descriptor: String,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for NodeDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ID: {} Descriptor: {} Configuration: {:?}",
            self.id, self.descriptor, self.configuration
        )
    }
}

impl NodeDescriptor {
    /// Creates a new `NodeDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<NodeDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `NodeDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<NodeDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `NodeDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `NodeDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Flattens the `NodeDescriptor` by loading all the composite operators
    ///
    /// # Errors
    ///
    /// A variant error is returned if loading operators fails. Or if the
    /// node does not contains an operator
    pub async fn flatten(
        self,
        id: NodeId,
        links: &mut Vec<LinkDescriptor>,
        global_configuration: Option<Configuration>,
        ancestors: &mut Vec<String>,
    ) -> Result<Vec<OperatorDescriptor>> {
        log::trace!("[Descriptor] loading operator {}", self.id);
        let descriptor = match parse_uri(&self.descriptor)? {
            crate::model::ZFUri::File(path) => try_load_descriptor_from_file(path).await,
            crate::model::ZFUri::Builtin(_) => bail!(
                ErrorKind::ConfigurationError,
                "Builtin operators are not yet supported!"
            ),
        }?;

        // We try to load the descriptor, first we try as simple one, if it fails we try as a
        // composite one, if that also fails it is malformed.
        let res_simple = OperatorDescriptor::from_yaml(&descriptor);
        if let Ok(mut simple_operator) = res_simple {
            log::trace!("[Descriptor] Operator {} is simple", simple_operator.id);
            simple_operator.configuration = global_configuration
                .clone()
                .merge_overwrite(simple_operator.configuration);
            simple_operator.id = id;
            return Ok(vec![simple_operator]);
        }

        let res_composite = CompositeOperatorDescriptor::from_yaml(&descriptor);
        if let Ok(composite_operator) = res_composite {
            log::trace!(
                "[Descriptor] Operator {} is composite",
                composite_operator.id
            );
            if let Ok(index) = ancestors.binary_search(&self.descriptor) {
                log::error!(
                    "Possible recursion detected, < {} > would be included again after: {:?}",
                    self.descriptor,
                    &ancestors[index..]
                );
                bail!(
                    ErrorKind::GenericError, // FIXME Dedicated error?
                    "Possible recursion detected, < {} > would be included again after: {:?}",
                    self.descriptor,
                    &ancestors[index..]
                );
            }

            ancestors.push(self.descriptor.clone());
            let res = composite_operator
                .flatten(id, links, global_configuration, ancestors)
                .await;
            ancestors.pop();

            return res;
        }

        log::error!("Could not parse operator < {} >", self.descriptor);
        log::error!("(Operator) {:?}", res_simple.err().unwrap());
        log::error!("(Composite) {:?}", res_composite.err().unwrap());

        bail!(
            ErrorKind::ParsingError,
            "Could not parse operator < {} >",
            self.descriptor
        )
    }

    /// Loads the source from the `NodeDescriptor`
    ///
    ///  # Errors
    /// A variant error is returned if loading source fails. Or if the
    ///  node does not contains an source
    pub async fn load_source(
        self,
        global_configuration: Option<Configuration>,
    ) -> Result<SourceDescriptor> {
        log::trace!("[Descriptor] Loading Source {}", self.id);

        match parse_uri(&self.descriptor)? {
            ZFUri::File(path) => {
                let descriptor = try_load_descriptor_from_file(path).await?;

                match SourceDescriptor::from_yaml(&descriptor) {
                    Ok(mut desc) => {
                        desc.id = self.id;
                        desc.configuration =
                            global_configuration.merge_overwrite(desc.configuration);
                        Ok(desc)
                    }
                    Err(e) => {
                        log::warn!("Unable to read descriptor {}, error {}", self.id, e);
                        Err(e)
                    }
                }
            }
            ZFUri::Builtin(mw) => match mw {
                Middleware::Zenoh => match &self.configuration {
                    Some(configuration) => {
                        let mut desc = get_zenoh_source_descriptor(configuration)?;
                        desc.id = self.id;
                        desc.configuration =
                            global_configuration.merge_overwrite(desc.configuration);
                        Ok(desc)
                    }
                    None => {
                        bail!(
                            ErrorKind::MissingConfiguration,
                            "Builtin Zenoh Sink needs a configuration!"
                        )
                    }
                },
            },
        }
    }

    /// Loads the sink from the `NodeDescriptor`
    ///
    /// # Errors
    ///
    /// A variant error is returned if loading sink fails. Or if the
    /// node does not contains an sink
    pub async fn load_sink(
        self,
        global_configuration: Option<Configuration>,
    ) -> Result<SinkDescriptor> {
        log::trace!("[Descriptor] Loading sink {}", self.id);

        match parse_uri(&self.descriptor)? {
            ZFUri::File(path) => {
                let descriptor = try_load_descriptor_from_file(path).await?;

                match SinkDescriptor::from_yaml(&descriptor) {
                    Ok(mut desc) => {
                        desc.id = self.id;
                        desc.configuration =
                            global_configuration.merge_overwrite(desc.configuration);
                        Ok(desc)
                    }
                    Err(e) => {
                        log::warn!("Unable to read descriptor {}, error {}", self.id, e);
                        Err(e)
                    }
                }
            }
            ZFUri::Builtin(mw) => match mw {
                Middleware::Zenoh => match &self.configuration {
                    Some(configuration) => {
                        let mut desc = get_zenoh_sink_descriptor(configuration)?;
                        desc.id = self.id;
                        desc.configuration =
                            global_configuration.merge_overwrite(desc.configuration);
                        Ok(desc)
                    }
                    None => {
                        bail!(
                            ErrorKind::MissingConfiguration,
                            "Builtin Zenoh Sink needs a configuration!"
                        )
                    }
                },
            },
        }
    }
}

/// Attempt to asynchronously read the content of the file pointed at by the `descriptor_path`.
///
/// This function will also expand the mustache notations present (if there are any).
///
/// # Errors
///
/// This function will return an error in the following situations:
/// - The provided `descriptor_path` is incorrect, i.e. the file does not exists.
/// - The content of the file could not be read.
async fn try_load_descriptor_from_file(descriptor_path: PathBuf) -> Result<String> {
    let data = async_std::fs::read_to_string(&descriptor_path).await?;
    Vars::expand_mustache_yaml(&data)
}
