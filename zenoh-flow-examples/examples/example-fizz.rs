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
    downcast, export_operator,
    loader::ZFOperatorRegistrarTrait,
    message::ZFMessage,
    operator::{
        DataTrait, FnInputRule, FnOutputRule, FnRun, InputRuleResult, OperatorTrait,
        OutputRuleResult, RunResult, StateTrait,
    },
    Token, ZFContext, ZFEmptyState, ZFError, ZFLinkId, ZFString,
};

struct FizzOperator {
    pub state: ZFEmptyState,
}

impl FizzOperator {
    const LINK_ID_INPUT_INTEGER: ZFLinkId = 0;

    const LINK_ID_OUTPUT_INTEGER: ZFLinkId = 1;
    const LINK_ID_OUTPUT_STR: ZFLinkId = 2;

    pub fn new() -> Self {
        Self {
            state: ZFEmptyState {},
        }
    }

    fn input_rule(_ctx: &mut ZFContext, inputs: &mut HashMap<ZFLinkId, Token>) -> InputRuleResult {
        for token in inputs.values() {
            match token {
                Token::Ready(_) => continue,
                Token::NotReady(_) => return Ok(false),
            }
        }

        Ok(true)
    }

    fn run(_ctx: &mut ZFContext, inputs: HashMap<ZFLinkId, Arc<dyn DataTrait>>) -> RunResult {
        let mut results = HashMap::<ZFLinkId, Arc<dyn DataTrait>>::with_capacity(2);

        let raw_value = inputs.get(&FizzOperator::LINK_ID_INPUT_INTEGER).unwrap();
        let mut fizz: ZFString = ZFString::from("");
        match downcast!(u128, raw_value) {
            Some(value) => {
                if value % 2 == 0 {
                    fizz = ZFString::from("Fizz");
                }
            }
            None => return Err(ZFError::InvalidData(FizzOperator::LINK_ID_INPUT_INTEGER)),
        }

        results.insert(FizzOperator::LINK_ID_OUTPUT_INTEGER, raw_value.clone());
        results.insert(FizzOperator::LINK_ID_OUTPUT_STR, Arc::new(fizz));

        Ok(results)
    }

    fn output_rule(
        _ctx: &mut ZFContext,
        outputs: HashMap<ZFLinkId, Arc<dyn DataTrait>>,
    ) -> OutputRuleResult {
        let mut zf_outputs: HashMap<ZFLinkId, Arc<ZFMessage>> = HashMap::with_capacity(2);

        zf_outputs.insert(
            FizzOperator::LINK_ID_OUTPUT_INTEGER,
            Arc::new(ZFMessage::from_data(
                outputs
                    .get(&FizzOperator::LINK_ID_OUTPUT_INTEGER)
                    .unwrap()
                    .clone(),
            )),
        );
        zf_outputs.insert(
            FizzOperator::LINK_ID_OUTPUT_STR,
            Arc::new(ZFMessage::from_data(
                outputs
                    .get(&FizzOperator::LINK_ID_OUTPUT_STR)
                    .unwrap()
                    .clone(),
            )),
        );

        Ok(zf_outputs)
    }
}

impl OperatorTrait for FizzOperator {
    fn get_input_rule(&self, _ctx: &ZFContext) -> Box<FnInputRule> {
        Box::new(FizzOperator::input_rule)
    }

    fn get_run(&self, _ctx: &ZFContext) -> Box<FnRun> {
        Box::new(FizzOperator::run)
    }

    fn get_output_rule(&self, _ctx: &ZFContext) -> Box<FnOutputRule> {
        Box::new(FizzOperator::output_rule)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

export_operator!(register);

extern "C" fn register(registrar: &mut dyn ZFOperatorRegistrarTrait) {
    registrar.register_zfoperator(
        "fizz",
        Box::new(FizzOperator::new()) as Box<dyn OperatorTrait + Send>,
    )
}
