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
use zenoh_flow::{
    export_operator, get_input,
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnRun, InputRuleOutput, OperatorTrait,
        OutputRuleOutput, RunOutput, StateTrait, ZFInput, ZFResult,
    },
    zf_data, zf_empty_state, Token, ZFComponentOutput, ZFContext,
};
use zenoh_flow_examples::{ZFString, ZFUsize};

struct BuzzOperator;

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_INPUT_STR: &str = "Str";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl BuzzOperator {
    fn input_rule(_ctx: ZFContext, inputs: &mut HashMap<String, Token>) -> InputRuleOutput {
        for token in inputs.values() {
            match token {
                Token::Ready(_) => continue,
                Token::NotReady => return Ok(false),
            }
        }

        Ok(true)
    }

    fn run(_ctx: ZFContext, mut inputs: ZFInput) -> RunOutput {
        let mut results = HashMap::<String, Arc<dyn DataTrait>>::with_capacity(1);

        let (_, fizz) = get_input!(ZFString, String::from(LINK_ID_INPUT_STR), inputs)?;
        let (_, value) = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        let mut buzz = fizz;
        if value.0 % 3 == 0 {
            buzz.0.push_str("Buzz3");
        }

        results.insert(String::from(LINK_ID_OUTPUT_STR), zf_data!(buzz));

        Ok(results)
    }

    fn output_rule(
        _ctx: ZFContext,
        outputs: HashMap<String, Arc<dyn DataTrait>>,
    ) -> OutputRuleOutput {
        let mut zf_outputs: HashMap<String, ZFComponentOutput> = HashMap::with_capacity(1);

        zf_outputs.insert(
            String::from(LINK_ID_OUTPUT_STR),
            ZFComponentOutput::Data(outputs.get(LINK_ID_OUTPUT_STR).unwrap().clone()),
        );

        Ok(zf_outputs)
    }
}

impl OperatorTrait for BuzzOperator {
    fn get_input_rule(&self, _ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(BuzzOperator::input_rule)
    }

    fn get_run(&self, _ctx: ZFContext) -> Box<FnRun> {
        Box::new(BuzzOperator::run)
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(BuzzOperator::output_rule)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        zf_empty_state!()
    }
}

export_operator!(register);

extern "C" fn register(
    _configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::OperatorTrait + Send>> {
    Ok(Box::new(BuzzOperator) as Box<dyn OperatorTrait + Send>)
}
