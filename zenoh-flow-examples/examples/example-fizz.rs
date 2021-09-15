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
use zenoh_flow::Token;
use zenoh_flow::{
    default_input_rule, default_output_rule, export_operator, get_input, types::ZFResult, zf_data,
    zf_empty_state, Component, InputRule, State, ZFComponentOutput, ZFComponentOutputRule,
    ZFOperatorTrait,
};
use zenoh_flow::{Context, Data, PortId};
use zenoh_flow_examples::{ZFString, ZFUsize};

struct FizzOperator;

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl InputRule for FizzOperator {
    fn input_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, inputs)
    }
}

impl Component for FizzOperator {
    fn initialize(&self, _configuration: &Option<HashMap<String, String>>) -> Box<dyn State> {
        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn State>) -> ZFResult<()> {
        Ok(())
    }
}

impl ZFOperatorTrait for FizzOperator {
    fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, ZFDataMessage>,
    ) -> ZFResult<HashMap<zenoh_flow::PortId, Arc<dyn zenoh_flow::Data>>> {
        let mut results = HashMap::<PortId, Arc<dyn Data>>::with_capacity(2);

        let mut fizz = ZFString::from("");

        let (_, zfusize) = get_input!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        if zfusize.0 % 2 == 0 {
            fizz = ZFString::from("Fizz");
        }

        results.insert(LINK_ID_OUTPUT_INT.into(), zf_data!(zfusize));
        results.insert(LINK_ID_OUTPUT_STR.into(), zf_data!(fizz));

        Ok(results)
    }
}

impl ZFComponentOutputRule for FizzOperator {
    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn State>,
        outputs: &HashMap<PortId, Arc<dyn Data>>,
    ) -> ZFResult<HashMap<zenoh_flow::PortId, ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

export_operator!(register);

fn register() -> ZFResult<Arc<dyn ZFOperatorTrait>> {
    Ok(Arc::new(FizzOperator) as Arc<dyn ZFOperatorTrait>)
}
