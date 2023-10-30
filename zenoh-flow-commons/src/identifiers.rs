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

use crate::deserialize::deserialize_id;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A `NodeId` identifies a Node in a data flow.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Clone, Hash)]
pub struct NodeId(#[serde(deserialize_with = "deserialize_id")] Arc<str>);

impl Deref for NodeId {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<String> for NodeId {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for NodeId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

/// A `PortId` identifies an `Input` or an `Output` of a Node.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct PortId(#[serde(deserialize_with = "deserialize_id")] Arc<str>);

impl Deref for PortId {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for PortId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<String> for PortId {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for PortId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

/// A `PortId` identifies an `Input` or an `Output` of a Node.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct RuntimeId(Arc<str>);

impl Deref for RuntimeId {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for RuntimeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<String> for RuntimeId {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for RuntimeId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct RecordId(Arc<Uuid>);

impl Display for RecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<Uuid> for RecordId {
    fn from(value: Uuid) -> Self {
        Self(Arc::new(value))
    }
}
