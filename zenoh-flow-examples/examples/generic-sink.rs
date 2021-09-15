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
use std::collections::HashMap;
use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::runtime::message::ZFDataMessage;
use zenoh_flow::zenoh_flow_derive::ZFState;
use zenoh_flow::Token;
use zenoh_flow::{
    default_input_rule, downcast, downcast_mut, export_sink, types::ZFResult, Component, State,
    ZFComponentInputRule,
};
use zenoh_flow::{Context, PortId, ZFSinkTrait};

use std::fs::File;
use std::io::Write;

struct GenericSink;

#[derive(ZFState, Clone, Debug)]
struct SinkState {
    pub file: Option<Arc<Mutex<File>>>,
}

impl SinkState {
    pub fn new(configuration: &Option<HashMap<String, String>>) -> Self {
        let file = match configuration {
            Some(c) => {
                let f = File::create(c.get("file").unwrap()).unwrap();
                Some(Arc::new(Mutex::new(f)))
            }
            None => None,
        };
        Self { file }
    }
}

#[async_trait]
impl ZFSinkTrait for GenericSink {
    async fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, ZFDataMessage>,
    ) -> ZFResult<()> {
        let state = downcast!(SinkState, _state).unwrap();

        match &state.file {
            None => {
                println!("#######");
                for (k, v) in inputs {
                    println!("Example Generic Sink Received on LinkId {:?} -> {:?}", k, v);
                }
                println!("#######");
                Ok(())
            }
            Some(f) => {
                let mut guard = f.lock().await;
                writeln!(&mut guard, "#######").unwrap();
                for (k, v) in inputs {
                    writeln!(
                        &mut guard,
                        "Example Generic Sink Received on LinkId {:?} -> {:?}",
                        k, v
                    )
                    .unwrap();
                }
                writeln!(&mut guard, "#######").unwrap();
                guard.sync_all().unwrap();
                Ok(())
            }
        }
    }
}

impl Component for GenericSink {
    fn initialize(&self, configuration: &Option<HashMap<String, String>>) -> Box<dyn State> {
        Box::new(SinkState::new(configuration))
    }

    fn clean(&self, _state: &mut Box<dyn State>) -> ZFResult<()> {
        let state = downcast_mut!(SinkState, _state).unwrap();

        match &mut state.file {
            None => Ok(()),
            Some(_) => {
                state.file = None;
                Ok(())
            }
        }
    }
}

impl ZFComponentInputRule for GenericSink {
    fn input_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn State>,
        tokens: &mut HashMap<PortId, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

export_sink!(register);

fn register() -> ZFResult<Arc<dyn ZFSinkTrait>> {
    Ok(Arc::new(GenericSink) as Arc<dyn ZFSinkTrait>)
}
