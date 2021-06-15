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
    downcast, export_operator, zf_data, get_input,
    loader::ZFOperatorRegistrarTrait,
    message::ZFMessage,
    operator::{
        DataTrait, FnInputRule, FnOutputRule, FnRun, InputRuleResult, OperatorTrait,
        OutputRuleResult, RunResult, StateTrait,
    },
    Token, ZFContext, ZFError, ZFLinkId,
};

struct BuzzOperator;

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_INPUT_STR: &str = "Str";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl BuzzOperator {
    fn input_rule(_ctx: &mut ZFContext, inputs: &mut HashMap<ZFLinkId, Token>) -> InputRuleResult {
        for token in inputs.values() {
            match token {
                Token::Ready(_) => continue,
                Token::NotReady(_) => return Ok(false),
            }
        }

        Ok(true)
    }

    fn run(_ctx: &mut ZFContext, inputs: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) -> RunResult {
        let mut results = HashMap::<ZFLinkId, Arc<Box<dyn DataTrait>>>::with_capacity(1);

        let mut buzz = get_input!(ZFString, String::from(LINK_ID_INPUT_STR), inputs)?.clone();

        let value = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        if value.0 % 3 == 0 {
            buzz.0.push_str("Buzz");
        }

        results.insert(String::from(LINK_ID_OUTPUT_STR), zf_data!(buzz));

        Ok(results)
    }

    fn output_rule(
        _ctx: &mut ZFContext,
        outputs: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>,
    ) -> OutputRuleResult {
        let mut zf_outputs: HashMap<ZFLinkId, Arc<ZFMessage>> = HashMap::with_capacity(1);

        zf_outputs.insert(
            String::from(LINK_ID_OUTPUT_STR),
            Arc::new(ZFMessage::from_data(
                outputs.get(LINK_ID_OUTPUT_STR).unwrap().clone(),
            )),
        );

        Ok(zf_outputs)
    }
}

impl OperatorTrait for BuzzOperator {
    fn get_input_rule(&self, _ctx: &ZFContext) -> Box<FnInputRule> {
        Box::new(BuzzOperator::input_rule)
    }

    fn get_run(&self, _ctx: &ZFContext) -> Box<FnRun> {
        Box::new(BuzzOperator::run)
    }

    fn get_output_rule(&self, _ctx: &ZFContext) -> Box<FnOutputRule> {
        Box::new(BuzzOperator::output_rule)
    }

    fn get_state(&self) -> Option<Box<dyn StateTrait>> {
        None
    }
}

export_operator!(register);

extern "C" fn register(registrar: &mut dyn ZFOperatorRegistrarTrait) {
    registrar.register_zfoperator(
        "buzz",
        Box::new(BuzzOperator) as Box<dyn OperatorTrait + Send>,
    )
}
