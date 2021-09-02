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
use zenoh_flow::{default_output_rule, ZFComponentOutputRule, ZFComponentState, ZFSourceTrait};
use zenoh_flow::{
    types::ZFResult, zenoh_flow_derive::ZFState, zf_data, zf_empty_state, ZFDataTrait,
};
use zenoh_flow_examples::ZFUsize;

static SOURCE: &str = "Counter";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, ZFState)]
struct CountSource;

impl CountSource {
    fn new(configuration: Option<HashMap<String, String>>) -> Self {
        match configuration {
            Some(conf) => {
                let initial = conf.get("initial").unwrap().parse::<usize>().unwrap();
                COUNTER.store(initial, Ordering::SeqCst);
                CountSource {}
            }
            None => CountSource {},
        }
    }
}

#[async_trait]
impl ZFSourceTrait for CountSource {
    async fn run(
        &self,
        _state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn ZFDataTrait>>> {
        let mut results: HashMap<String, Arc<dyn ZFDataTrait>> = HashMap::new();
        let d = ZFUsize(COUNTER.fetch_add(1, Ordering::AcqRel));
        results.insert(String::from(SOURCE), zf_data!(d));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

impl ZFComponentOutputRule for CountSource {
    fn output_rule(
        &self,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl ZFComponentState for CountSource {
    fn initial_state(&self) -> Box<dyn zenoh_flow::ZFStateTrait> {
        zf_empty_state!()
    }
}

zenoh_flow::export_source!(register);

fn register(
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn ZFSourceTrait + Send>> {
    Ok(Box::new(CountSource::new(configuration)) as Box<dyn ZFSourceTrait + Send>)
}
