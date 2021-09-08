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

pub mod component;
pub mod connector;
pub mod dataflow;
pub mod link;
pub mod period;

use crate::model::link::ZFPortDescriptor;
use crate::model::period::ZFPeriodDescriptor;
use crate::serde::{Deserialize, Serialize};

// Registry metadata

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZFRegistryGraph {
    pub id: String,
    pub classes: Vec<String>,
    pub tags: Vec<ZFRegistryComponentTag>,
    pub inputs: Vec<ZFPortDescriptor>,
    pub outputs: Vec<ZFPortDescriptor>,
    pub period: Option<ZFPeriodDescriptor>,
}

impl ZFRegistryGraph {
    pub fn add_tag(&mut self, tag: ZFRegistryComponentTag) {
        let index = self.tags.iter().position(|t| t.name == tag.name);
        match index {
            Some(i) => {
                let mut old_tag = self.tags.remove(i);
                for architecture in tag.architectures.into_iter() {
                    old_tag.add_architecture(architecture);
                }
                self.tags.push(old_tag);
            }
            None => {
                self.tags.push(tag);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZFRegistryComponentTag {
    pub name: String,
    pub requirement_labels: Vec<String>,
    pub architectures: Vec<ZFRegistryComponentArchitecture>,
}

impl ZFRegistryComponentTag {
    pub fn add_architecture(&mut self, arch: ZFRegistryComponentArchitecture) {
        let index = self
            .architectures
            .iter()
            .position(|a| a.os == arch.os && a.arch == arch.arch);
        match index {
            Some(i) => {
                self.architectures.remove(i);
                self.architectures.push(arch);
            }
            None => {
                self.architectures.push(arch);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZFRegistryComponentArchitecture {
    pub arch: String,
    pub os: String,
    pub uri: String,
    pub checksum: String,
    pub signature: String,
}
