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

use serde::{Deserialize, Serialize};
use std::fmt;
use zenoh_flow_commons::{NodeId, PortId};
use zenoh_flow_records::{InputRecord, OutputRecord};

/// An `InputDescriptor` describes an Input port of a Zenoh-Flow node.
///
/// FIXME@J-Loudet See [links] for their usage.
#[derive(Debug, Hash, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct InputDescriptor {
    pub node: NodeId,
    pub input: PortId,
}

impl fmt::Display for InputDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.node, self.input))
    }
}

impl InputDescriptor {
    pub fn new(node: impl AsRef<str>, input: impl AsRef<str>) -> Self {
        Self {
            node: node.as_ref().into(),
            input: input.as_ref().into(),
        }
    }
}

impl From<InputDescriptor> for InputRecord {
    fn from(this: InputDescriptor) -> Self {
        InputRecord {
            node: this.node,
            input: this.input,
        }
    }
}

/// An `OutputDescriptor` describes an Output port of a Zenoh-Flow node.
///
/// See [LinkDescriptor][crate::link::LinkDescriptor] for their usage.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputDescriptor {
    pub node: NodeId,
    pub output: PortId,
}

impl fmt::Display for OutputDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.node, self.output))
    }
}

impl OutputDescriptor {
    pub fn new(node: impl AsRef<str>, output: impl AsRef<str>) -> Self {
        Self {
            node: node.as_ref().into(),
            output: output.as_ref().into(),
        }
    }
}

impl From<OutputDescriptor> for OutputRecord {
    fn from(this: OutputDescriptor) -> Self {
        OutputRecord {
            node: this.node,
            output: this.output,
        }
    }
}
