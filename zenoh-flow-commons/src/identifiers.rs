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
use std::ops::Deref;
use std::sync::Arc;
use std::{fmt::Display, str::FromStr};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zenoh_protocol::core::ZenohId;

/// A `NodeId` uniquely identifies a Node within a data flow.
///
/// A `NodeId` additionally satisfies the following constraints:
/// - it does *not* contain any of the symbols: * # $ ? >
/// - it is a valid [canonical Zenoh key expression](zenoh_keyexpr::OwnedKeyExpr::autocanonize).
///
/// # Performance
///
/// A `NodeId` is encapsulated in an [Arc] rendering clone operations almost cost-free.
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
///
/// A `PortId` additionally satisfies the following constraints:
/// - it does *not* contain any of the symbols: * # $ ? >
/// - it is a valid [canonical Zenoh key expression](zenoh_keyexpr::OwnedKeyExpr::autocanonize).
///
/// # Uniqueness
///
/// A `PortId` does not need to be unique within a data flow. It should only be unique among the ports of the same node
/// and of the same type (i.e. `Input` or `Output`).
/// For instance, a node can have an `Input` and an `Output` with the same `PortId`.
///
/// # Performance
///
/// A `PortId` is encapsulated in an [Arc] rendering clone operations almost cost-free.
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

/// A `RuntimeId` uniquely identifies a Zenoh-Flow runtime within a Zenoh network.
///
/// The `RuntimeId` structure simply wraps a [ZenohId]. Similar to a Uuid, this identifier is (with a high probability)
/// guaranteed to be unique within your infrastructure.
///
/// A Zenoh-Flow runtime will, by default, reuse the [ZenohId] of the Zenoh
/// [session](https://docs.rs/zenoh/0.10.1-rc/zenoh/struct.Session.html) it will create to connect to the Zenoh network.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize, Default)]
#[repr(transparent)]
pub struct RuntimeId(ZenohId);

impl RuntimeId {
    /// Generate a new random identifier, guaranteed (with a high probability) to be unique.
    ///
    /// This internally calls [ZenohId::rand].
    pub fn rand() -> Self {
        Self(ZenohId::rand())
    }
}

impl Display for RuntimeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl Deref for RuntimeId {
    type Target = ZenohId;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ZenohId> for RuntimeId {
    fn from(value: ZenohId) -> Self {
        Self(value)
    }
}

impl FromStr for RuntimeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ZenohId::from_str(s)
            .map_err(|e| anyhow!("Failed to parse < {} > as a valid ZenohId:\n{:?}", s, e))?
            .into())
    }
}

/// An `InstanceId` uniquely identifies a data flow instance.
///
/// A data flow instance is created every time Zenoh-Flow is tasked to run a data flow. Each instance of the same data
/// flow will have a different `InstanceId`.
///
/// Internally, it uses a [Uuid v4](uuid::Uuid::new_v4) that it wraps inside an [Arc]. This allows for almost cost-free
/// `clone` operations.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct InstanceId(Arc<Uuid>);

impl Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<Uuid> for InstanceId {
    fn from(value: Uuid) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for InstanceId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
