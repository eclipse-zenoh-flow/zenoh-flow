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

pub mod dataflow;
pub use dataflow::{DataFlowDescriptor, FlattenDataFlowDescriptor};
pub mod link;
pub use link::{
    CompositeInputDescriptor, CompositeOutputDescriptor, InputDescriptor, LinkDescriptor,
    OutputDescriptor,
};
pub mod node;
pub use node::{
    CompositeOperatorDescriptor, NodeDescriptor, OperatorDescriptor, SinkDescriptor,
    SourceDescriptor,
};
pub mod validator;

use crate::zfresult::{ErrorKind, ZFResult as Result};
use crate::{bail, zferror};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};

/// The unit of duration used in different descriptors.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DurationUnit {
    #[serde(alias = "s")]
    #[serde(alias = "second")]
    #[serde(alias = "seconds")]
    Second,
    #[serde(alias = "ms")]
    #[serde(alias = "millisecond")]
    #[serde(alias = "milliseconds")]
    Millisecond,
    #[serde(alias = "us")]
    #[serde(alias = "µs")]
    #[serde(alias = "microsecond")]
    #[serde(alias = "microseconds")]
    Microsecond,
}

/// The descriptor for a duration.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DurationDescriptor {
    #[serde(alias = "duration")]
    pub(crate) length: u64,
    pub(crate) unit: DurationUnit,
}

impl DurationDescriptor {
    /// Converts the [`DurationDescriptor`](`DurationDescriptor`) to a [`Duration`](`Duration`).
    pub fn to_duration(&self) -> Duration {
        match self.unit {
            DurationUnit::Second => Duration::from_secs(self.length),
            DurationUnit::Millisecond => Duration::from_millis(self.length),
            DurationUnit::Microsecond => Duration::from_micros(self.length),
        }
    }
}

/// `Vars` is an internal structure that we use to expand the "mustache variables" in a descriptor
/// file.
///
/// Mustache variables take the form: "{{ var }}..." where the number of spaces after the '{{' and
/// before the '}}' do not matter.
///
/// We first parse the descriptor file to only extract the `vars` section and build a
/// `HashMap<String, String>` out of it.
///
/// We then load the descriptor file as a template and "render" it, substituting every "mustache
/// variable" with its corresponding value in the HashMap.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Vars {
    vars: Option<HashMap<String, String>>,
}

impl Vars {
    fn expand_mustache(&self, data: &str) -> Result<String> {
        let mut descriptor = data.to_owned();
        if let Some(vars) = &self.vars {
            match ramhorns::Template::new(data) {
                Ok(template) => descriptor = template.render(vars),
                Err(e) => bail!(
                    ErrorKind::ParsingError,
                    "Could not parse ramhorns::Template:\n(error) {:?}\n(yaml) {}",
                    e,
                    data
                ),
            }
        }

        Ok(descriptor)
    }

    pub(crate) fn expand_mustache_yaml(data: &str) -> Result<String> {
        let vars =
            serde_yaml::from_str::<Vars>(data).map_err(|e| zferror!(ErrorKind::ParsingError, e))?;

        vars.expand_mustache(data)
    }

    pub(crate) fn expand_mustache_json(data: &str) -> Result<String> {
        let vars =
            serde_json::from_str::<Vars>(data).map_err(|e| zferror!(ErrorKind::ParsingError, e))?;

        vars.expand_mustache(data)
    }
}
