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

struct BuzzOperator {
    pub state: ZFEmptyState,
}

impl BuzzOperator {
    const LINK_ID_INPUT_INT: ZFLinkId = 1;
    const LINK_ID_INPUT_STR: ZFLinkId = 2;

    const LINK_ID_OUTPUT_STR: ZFLinkId = 3;

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
        let mut results = HashMap::<ZFLinkId, Arc<dyn DataTrait>>::with_capacity(1);

        let raw_value = inputs.get(&BuzzOperator::LINK_ID_INPUT_INT).unwrap();
        let raw_fizz = inputs.get(&BuzzOperator::LINK_ID_INPUT_STR).unwrap();

        let fizz: ZFString = match downcast!(ZFString, raw_fizz) {
            Some(fizz_str) => (*fizz_str).clone(),
            None => return Err(ZFError::InvalidData(BuzzOperator::LINK_ID_INPUT_STR)),
        };

        let mut buzz = ZFString::from("".to_string());
        match downcast!(u128, raw_value) {
            Some(value) => {
                if value % 3 == 0 {
                    buzz = ZFString::from(format!("{:?}Buzz", fizz));
                }
            }
            None => return Err(ZFError::InvalidData(BuzzOperator::LINK_ID_INPUT_INT)),
        }

        results.insert(BuzzOperator::LINK_ID_OUTPUT_STR, Arc::new(buzz));

        Ok(results)
    }

    fn output_rule(
        _ctx: &mut ZFContext,
        outputs: HashMap<ZFLinkId, Arc<dyn DataTrait>>,
    ) -> OutputRuleResult {
        let mut zf_outputs: HashMap<ZFLinkId, Arc<ZFMessage>> = HashMap::with_capacity(1);

        zf_outputs.insert(
            BuzzOperator::LINK_ID_OUTPUT_STR,
            Arc::new(ZFMessage::from_data(
                outputs
                    .get(&BuzzOperator::LINK_ID_OUTPUT_STR)
                    .unwrap()
                    .clone(),
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

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

export_operator!(register);

extern "C" fn register(registrar: &mut dyn ZFOperatorRegistrarTrait) {
    registrar.register_zfoperator(
        "fizz",
        Box::new(BuzzOperator::new()) as Box<dyn OperatorTrait + Send>,
    )
}
