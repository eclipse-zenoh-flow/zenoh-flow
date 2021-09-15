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
use async_trait::async_trait;
use std::collections::HashMap;
use zenoh_flow::{
    default_output_rule, types::ZFResult, zf_data, zf_empty_state, Component, Context, Data,
    OutputRule, PortId, State, ZFSourceTrait,
};
use zenoh_flow_examples::ZFUsize;

static SOURCE: &str = "Random";

#[derive(Debug)]
struct ExampleRandomSource;

impl Component for ExampleRandomSource {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::State> {
        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn State>) -> ZFResult<()> {
        Ok(())
    }
}

impl OutputRule for ExampleRandomSource {
    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn zenoh_flow::State>,
        outputs: &HashMap<PortId, Arc<dyn zenoh_flow::Data>>,
    ) -> ZFResult<HashMap<PortId, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

#[async_trait]
impl ZFSourceTrait for ExampleRandomSource {
    async fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn zenoh_flow::State>,
    ) -> ZFResult<HashMap<PortId, Arc<dyn Data>>> {
        let mut results: HashMap<PortId, Arc<dyn Data>> = HashMap::with_capacity(1);
        results.insert(SOURCE.into(), zf_data!(ZFUsize(rand::random::<usize>())));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

// Also generated by macro
zenoh_flow::export_source!(register);

fn register() -> ZFResult<Arc<dyn ZFSourceTrait>> {
    Ok(Arc::new(ExampleRandomSource) as Arc<dyn ZFSourceTrait>)
}
