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

use async_std::sync::Arc;
use std::collections::HashMap;
use zenoh_flow::runtime::message::ZFDataMessage;
use zenoh_flow::{
    default_input_rule, default_output_rule, export_operator, get_input, types::ZFResult, zf_data,
    zf_empty_state, Token, ZFComponent, ZFComponentInputRule, ZFComponentOutput,
    ZFComponentOutputRule, ZFOperatorTrait, ZFStateTrait,
};
use zenoh_flow::{Context, ZFDataTrait};
use zenoh_flow_examples::{ZFString, ZFUsize};

struct FizzOperator;

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl ZFComponentInputRule for FizzOperator {
    fn input_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, inputs)
    }
}

impl ZFComponent for FizzOperator {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn ZFStateTrait> {
        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn ZFStateTrait>) -> ZFResult<()> {
        Ok(())
    }
}

impl ZFOperatorTrait for FizzOperator {
    fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn zenoh_flow::ZFDataTrait>>> {
        let mut results = HashMap::<String, Arc<dyn ZFDataTrait>>::with_capacity(2);

        let mut fizz = ZFString::from("");

        let (_, zfusize) = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        if zfusize.0 % 2 == 0 {
            fizz = ZFString::from("Fizz");
        }

        results.insert(String::from(LINK_ID_OUTPUT_INT), zf_data!(zfusize));
        results.insert(String::from(LINK_ID_OUTPUT_STR), zf_data!(fizz));

        Ok(results)
    }
}

impl ZFComponentOutputRule for FizzOperator {
    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

export_operator!(register);

fn register() -> ZFResult<Arc<dyn ZFOperatorTrait>> {
    Ok(Arc::new(FizzOperator) as Arc<dyn ZFOperatorTrait>)
}
