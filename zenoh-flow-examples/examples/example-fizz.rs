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
use zenoh_flow_examples::{ZFString, ZFUsize};

use zenoh_flow::{
    export_operator, get_input,
    runtime::message::Message,
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnRun, InputRuleResult, OperatorTrait,
        OutputRuleResult, RunResult, StateTrait, ZFInput, ZFResult,
    },
    zf_data, zf_empty_state, Token, ZFContext,
};

struct FizzOperator;

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl FizzOperator {
    fn input_rule(_ctx: ZFContext, inputs: &mut HashMap<String, Token>) -> InputRuleResult {
        for token in inputs.values() {
            match token {
                Token::Ready(_) => continue,
                Token::NotReady => return Ok(false),
            }
        }

        Ok(true)
    }

    fn run(_ctx: ZFContext, mut inputs: ZFInput) -> RunResult {
        let mut results = HashMap::<String, Arc<dyn DataTrait>>::with_capacity(2);

        let mut fizz = ZFString::from("");

        let (_, zfusize) = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        if zfusize.0 % 2 == 0 {
            fizz = ZFString::from("Fizz");
        }

        results.insert(String::from(LINK_ID_OUTPUT_INT), zf_data!(zfusize.clone()));
        results.insert(String::from(LINK_ID_OUTPUT_STR), zf_data!(fizz));

        Ok(results)
    }

    fn output_rule(
        _ctx: ZFContext,
        outputs: HashMap<String, Arc<dyn DataTrait>>,
    ) -> OutputRuleResult {
        let mut zf_outputs: HashMap<String, Message> = HashMap::with_capacity(2);

        zf_outputs.insert(
            String::from(LINK_ID_OUTPUT_INT),
            Message::from_data(outputs.get(LINK_ID_OUTPUT_INT).unwrap().clone()),
        );
        zf_outputs.insert(
            String::from(LINK_ID_OUTPUT_STR),
            Message::from_data(outputs.get(LINK_ID_OUTPUT_STR).unwrap().clone()),
        );

        Ok(zf_outputs)
    }
}

impl OperatorTrait for FizzOperator {
    fn get_input_rule(&self, _ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(FizzOperator::input_rule)
    }

    fn get_run(&self, _ctx: ZFContext) -> Box<FnRun> {
        Box::new(FizzOperator::run)
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(FizzOperator::output_rule)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        zf_empty_state!()
    }
}

export_operator!(register);

extern "C" fn register(
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::OperatorTrait + Send>> {
    Ok(Box::new(FizzOperator) as Box<dyn OperatorTrait + Send>)
}
