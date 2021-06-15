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

use std::{collections::HashMap, io, sync::Arc, usize};

use zenoh_flow::{
    operator::{DataTrait, FnSourceRun, RunResult, SourceTrait, StateTrait},
    ZFContext, ZFError, ZFLinkId,
};
use zenoh_flow_examples::ZFUsize;

struct ManualSource;

static LINK_ID_INPUT_INT: &str = "Int";

impl ManualSource {
    fn run(_ctx: &mut ZFContext) -> RunResult {
        let mut results: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::with_capacity(1);

        println!("> Please input a number: ");
        let mut number = String::new();
        io::stdin()
            .read_line(&mut number)
            .expect("Could not read number.");

        let value: usize = match number.trim().parse() {
            Ok(value) => value,
            Err(_) => return Err(ZFError::GenericError),
        };

        results.insert(
            String::from(LINK_ID_INPUT_INT),
            Arc::new(Box::new(ZFUsize(value))),
        );

        Ok(results)
    }
}

impl SourceTrait for ManualSource {
    fn get_run(&self, _ctx: &ZFContext) -> Box<FnSourceRun> {
        Box::new(Self::run)
    }

    fn get_state(&self) -> Option<Box<dyn StateTrait>> {
        None
    }
}

zenoh_flow::export_source!(register);

extern "C" fn register(registrar: &mut dyn zenoh_flow::loader::ZFSourceRegistrarTrait) {
    registrar.register_zfsource(
        "sender",
        Box::new(ManualSource {}) as Box<dyn zenoh_flow::operator::SourceTrait + Send>,
    );
}
