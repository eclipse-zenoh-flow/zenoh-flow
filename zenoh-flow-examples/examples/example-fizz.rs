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
use zenoh_flow::runtime::message::DataMessage;
use zenoh_flow::Token;
use zenoh_flow::{
    default_input_rule, default_output_rule, export_operator, get_input_from, types::ZFResult,
    zf_data, zf_empty_state, Node, NodeOutput, Operator, State,
};
use zenoh_flow::{Context, PortId, SerDeData};
use zenoh_flow_examples::{ZFString, ZFUsize};

struct FizzOperator;

static LINK_ID_INPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_INT: &str = "Int";
static LINK_ID_OUTPUT_STR: &str = "Str";

impl Node for FizzOperator {
    fn initialize(&self, _configuration: &Option<HashMap<String, String>>) -> Box<dyn State> {
        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn State>) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for FizzOperator {
    fn input_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, inputs)
    }

    fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, DataMessage>,
    ) -> ZFResult<HashMap<zenoh_flow::PortId, SerDeData>> {
        let mut results = HashMap::<PortId, SerDeData>::with_capacity(2);

        let mut fizz = ZFString::from("");

        let (_, zfusize) = get_input_from!(ZFUsize, String::from(LINK_ID_INPUT_INT), inputs)?;

        if zfusize.0 % 2 == 0 {
            fizz = ZFString::from("Fizz");
        }

        results.insert(LINK_ID_OUTPUT_INT.into(), zf_data!(zfusize));
        results.insert(LINK_ID_OUTPUT_STR.into(), zf_data!(fizz));

        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn State>,
        outputs: HashMap<PortId, SerDeData>,
    ) -> ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        default_output_rule(state, outputs)
    }
}

export_operator!(register);

fn register() -> ZFResult<Arc<dyn Operator>> {
    Ok(Arc::new(FizzOperator) as Arc<dyn Operator>)
}
