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
use zenoh_flow::zenoh_flow_derive::ZFState;
use zenoh_flow::{
    default_input_rule, default_output_rule, export_operator, get_input, types::ZFResult, zf_data,
    Token, ZFComponent, ZFComponentInputRule, ZFComponentOutput, ZFComponentOutputRule,
    ZFOperatorTrait, ZFStateTrait,
};
use zenoh_flow::{downcast, ZFContext, ZFDataTrait};
use zenoh_flow_examples::{ZFString, ZFUsize};

struct BuzzOperator;

#[derive(Debug, ZFState)]
struct BuzzState {
    buzzword: String,
}

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_INPUT_STR: &str = "Str";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl ZFOperatorTrait for BuzzOperator {
    fn run(
        &self,
        _context: &mut ZFContext,
        dyn_state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn ZFDataTrait>>> {
        let mut results = HashMap::<String, Arc<dyn ZFDataTrait>>::with_capacity(1);

        let state = downcast!(BuzzState, dyn_state).unwrap();
        let (_, fizz) = get_input!(ZFString, String::from(LINK_ID_INPUT_STR), inputs)?;
        let (_, value) = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        let mut buzz = fizz;
        if value.0 % 3 == 0 {
            buzz.0.push_str(&state.buzzword);
        }

        results.insert(String::from(LINK_ID_OUTPUT_STR), zf_data!(buzz));

        Ok(results)
    }
}

impl ZFComponent for BuzzOperator {
    fn initialize(
        &self,
        configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn ZFStateTrait> {
        let state = match configuration {
            Some(config) => match config.get("buzzword") {
                Some(buzzword) => BuzzState {
                    buzzword: buzzword.to_string(),
                },
                None => BuzzState {
                    buzzword: "Buzz".to_string(),
                },
            },
            None => BuzzState {
                buzzword: "Buzz".to_string(),
            },
        };
        Box::new(state)
    }
}

impl ZFComponentInputRule for BuzzOperator {
    fn input_rule(
        &self,
        _context: &mut ZFContext,
        state: &mut Box<dyn ZFStateTrait>,
        tokens: &mut HashMap<String, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl ZFComponentOutputRule for BuzzOperator {
    fn output_rule(
        &self,
        _context: &mut ZFContext,
        state: &mut Box<dyn ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

export_operator!(register);

fn register() -> ZFResult<Box<dyn ZFOperatorTrait + Send>> {
    Ok(Box::new(BuzzOperator) as Box<dyn ZFOperatorTrait + Send>)
}
