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
    zf_empty_state, Token, ZFComponentInputRule, ZFComponentOutput, ZFComponentOutputRule,
    ZFComponentState, ZFDataTrait, ZFOperatorTrait, ZFStateTrait,
};
use zenoh_flow_examples::{ZFString, ZFUsize};

struct BuzzOperator {
    buzzword: String,
}

impl BuzzOperator {
    fn new(buzzword: String) -> Self {
        Self { buzzword }
    }
}

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_INPUT_STR: &str = "Str";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl ZFOperatorTrait for BuzzOperator {
    fn run(
        &self,
        _state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn ZFDataTrait>>> {
        let mut results = HashMap::<String, Arc<dyn ZFDataTrait>>::with_capacity(1);

        let (_, fizz) = get_input!(ZFString, String::from(LINK_ID_INPUT_STR), inputs)?;
        let (_, value) = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        let mut buzz = fizz;
        if value.0 % 3 == 0 {
            buzz.0.push_str(&self.buzzword);
        }

        results.insert(String::from(LINK_ID_OUTPUT_STR), zf_data!(buzz));

        Ok(results)
    }
}

impl ZFComponentState for BuzzOperator {
    fn initial_state(&self) -> Box<dyn ZFStateTrait> {
        zf_empty_state!()
    }
}

impl ZFComponentInputRule for BuzzOperator {
    fn input_rule(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
        tokens: &mut HashMap<String, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl ZFComponentOutputRule for BuzzOperator {
    fn output_rule(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

export_operator!(register);

fn register(
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn ZFOperatorTrait + Send>> {
    let buzz_operator = match configuration {
        Some(config) => match config.get("buzzword") {
            Some(buzzword) => BuzzOperator::new(buzzword.to_string()),
            None => BuzzOperator::new("Buzz".to_string()),
        },
        None => BuzzOperator::new("Buzz".to_string()),
    };
    Ok(Box::new(buzz_operator) as Box<dyn ZFOperatorTrait + Send>)
}
