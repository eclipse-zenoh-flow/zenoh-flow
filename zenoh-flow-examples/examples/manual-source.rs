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

use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc, usize};
use zenoh_flow::{
    zf_data, zf_empty_state, Context, ZFComponent, ZFComponentOutputRule, ZFDataTrait, ZFError,
    ZFPortID, ZFResult, ZFSourceTrait, ZFStateTrait,
};
use zenoh_flow_examples::ZFUsize;

struct ManualSource;

static LINK_ID_INPUT_INT: &str = "Int";

#[async_trait]
impl ZFSourceTrait for ManualSource {
    async fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn ZFStateTrait>,
    ) -> ZFResult<HashMap<ZFPortID, Arc<dyn ZFDataTrait>>> {
        let mut results: HashMap<String, Arc<dyn ZFDataTrait>> = HashMap::with_capacity(1);

        println!("> Please input a number: ");
        let mut number = String::new();
        async_std::io::stdin()
            .read_line(&mut number)
            .await
            .expect("Could not read number.");

        let value: usize = match number.trim().parse() {
            Ok(value) => value,
            Err(_) => return Err(ZFError::GenericError),
        };

        results.insert(String::from(LINK_ID_INPUT_INT), zf_data!(ZFUsize(value)));

        Ok(results)
    }
}

impl ZFComponent for ManualSource {
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

impl ZFComponentOutputRule for ManualSource {
    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<ZFPortID, zenoh_flow::ZFComponentOutput>> {
        zenoh_flow::default_output_rule(state, outputs)
    }
}

zenoh_flow::export_source!(register);

fn register() -> ZFResult<Arc<dyn ZFSourceTrait>> {
    Ok(Arc::new(ManualSource) as Arc<dyn ZFSourceTrait>)
}
