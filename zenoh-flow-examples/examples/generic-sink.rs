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

use async_trait::async_trait;
use std::collections::HashMap;
use zenoh_flow::runtime::message::ZFDataMessage;
use zenoh_flow::{
    default_input_rule, export_sink, types::ZFResult, zf_empty_state, Token, ZFComponent,
    ZFComponentInputRule, ZFStateTrait,
};
use zenoh_flow::{ZFContext, ZFSinkTrait};

struct GenericSink;

#[async_trait]
impl ZFSinkTrait for GenericSink {
    async fn run(
        &self,
        _context: &mut ZFContext,
        _state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<()> {
        println!("#######");
        for (k, v) in inputs {
            println!("Example Generic Sink Received on LinkId {:?} -> {:?}", k, v);
        }
        println!("#######");
        Ok(())
    }
}

impl ZFComponent for GenericSink {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn ZFStateTrait> {
        zf_empty_state!()
    }
}

impl ZFComponentInputRule for GenericSink {
    fn input_rule(
        &self,
        _context: &mut ZFContext,
        state: &mut Box<dyn ZFStateTrait>,
        tokens: &mut HashMap<String, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

export_sink!(register);

fn register() -> ZFResult<Box<dyn ZFSinkTrait + Send>> {
    Ok(Box::new(GenericSink) as Box<dyn ZFSinkTrait + Send>)
}
