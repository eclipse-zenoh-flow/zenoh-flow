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
use std::sync::atomic::{AtomicUsize, Ordering};
use zenoh_flow::{default_output_rule, Component, OutputRule, PortId, ZFSourceTrait};
use zenoh_flow::{
    types::ZFResult, zenoh_flow_derive::ZFState, zf_data, zf_empty_state, Data, State,
};
use zenoh_flow_examples::ZFUsize;

static SOURCE: &str = "Counter";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, ZFState)]
struct CountSource;

#[async_trait]
impl ZFSourceTrait for CountSource {
    async fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut Box<dyn zenoh_flow::State>,
    ) -> ZFResult<HashMap<zenoh_flow::PortId, Arc<dyn Data>>> {
        let mut results: HashMap<PortId, Arc<dyn Data>> = HashMap::new();
        let d = ZFUsize(COUNTER.fetch_add(1, Ordering::AcqRel));
        results.insert(SOURCE.into(), zf_data!(d));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

impl OutputRule for CountSource {
    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut Box<dyn zenoh_flow::State>,
        outputs: &HashMap<PortId, Arc<dyn Data>>,
    ) -> ZFResult<HashMap<zenoh_flow::PortId, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl Component for CountSource {
    fn initialize(
        &self,
        configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::State> {
        if let Some(conf) = configuration {
            let initial = conf.get("initial").unwrap().parse::<usize>().unwrap();
            COUNTER.store(initial, Ordering::SeqCst);
        }

        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn State>) -> ZFResult<()> {
        Ok(())
    }
}

zenoh_flow::export_source!(register);

fn register() -> ZFResult<Arc<dyn ZFSourceTrait>> {
    Ok(Arc::new(CountSource) as Arc<dyn ZFSourceTrait>)
}
