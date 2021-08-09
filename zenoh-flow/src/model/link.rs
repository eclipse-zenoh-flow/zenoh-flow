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

use crate::ZFOperatorId;
use serde::{de::Visitor, Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFLinkDescriptor {
    pub from: ZFLinkFromDescriptor,
    pub to: ZFLinkToDescriptor,
    pub size: Option<usize>,
    pub queueing_policy: Option<String>,
    pub priority: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZFPortDescriptor {
    #[serde(alias = "id")]
    pub port_id: String,
    #[serde(alias = "type")]
    pub port_type: String,
}

#[derive(Debug, Clone)]
pub struct ZFLinkFromDescriptor {
    pub component_id: ZFOperatorId,
    pub output_id: String,
}

impl fmt::Display for ZFLinkFromDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.component_id, self.output_id))
    }
}

struct ZFLinkFromVisitor;
impl<'de> Visitor<'de> for ZFLinkFromVisitor {
    type Value = ZFLinkFromDescriptor;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an output port descriptor: 'operator_id.output_id'")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let index_dot = match v.find('.') {
            Some(index) => index,
            None => return Err(E::custom(format!("invalid 'from' descriptor: {}", v))),
        };

        let (component_id, output_id) = v.split_at(index_dot);

        Ok(ZFLinkFromDescriptor {
            component_id: component_id.to_string(),
            output_id: output_id[1..].to_string(),
        })
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v.as_str())
    }
}

impl<'de> Deserialize<'de> for ZFLinkFromDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ZFLinkFromVisitor)
    }
}

impl Serialize for ZFLinkFromDescriptor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

#[derive(Debug, Clone)]
pub struct ZFLinkToDescriptor {
    pub component_id: ZFOperatorId,
    pub input_id: String,
}

impl fmt::Display for ZFLinkToDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.component_id, self.input_id))
    }
}

struct ZFLinkToVisitor;
impl<'de> Visitor<'de> for ZFLinkToVisitor {
    type Value = ZFLinkToDescriptor;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an input port descriptor: 'operator_id.input_id'")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let index_dot = match v.find('.') {
            Some(index) => index,
            None => return Err(E::custom(format!("invalid 'from' descriptor: {}", v))),
        };

        let (component_id, input_id) = v.split_at(index_dot);

        Ok(ZFLinkToDescriptor {
            component_id: component_id.to_string(),
            input_id: input_id[1..].to_string(),
        })
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v.as_str())
    }
}

impl<'de> Deserialize<'de> for ZFLinkToDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ZFLinkToVisitor)
    }
}

impl Serialize for ZFLinkToDescriptor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}
