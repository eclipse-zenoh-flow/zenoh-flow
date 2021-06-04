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

use crate::link::{link, ZFLinkReceiver, ZFLinkSender};
use crate::message::{ZFMessage, ZFMsg};
use crate::types::{OperatorResult, ZFLinkId};
use crate::{OperatorRun, ZFContext, ZFOperator, ZFOperatorId};
use async_std::sync::Arc;
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use std::io;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub unsafe fn load_operator(path: String) -> Result<(ZFOperatorId, ZFOperatorRunner), io::Error> {
    // This is unsafe because has to dynamically load a library
    let library = Arc::new(Library::new(path.clone()).unwrap());
    let decl = library
        .get::<*mut ZFOperatorDeclaration>(b"zfoperator_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(io::Error::new(io::ErrorKind::Other, "Version mismatch"));
    }
    let mut registrar = ZFOperatorRegistrar::new(Arc::clone(&library));

    (decl.register)(&mut registrar);

    let (operator_id, proxy) = registrar.operator.unwrap();

    let runner = ZFOperatorRunner::new(proxy, library);
    Ok((operator_id, runner))
}

pub struct ZFOperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(&mut dyn ZFOperatorRegistrarTrait),
}

pub trait ZFOperatorRegistrarTrait {
    fn register_zfoperator(&mut self, name: &str, operator: Box<dyn ZFOperator + Send>);
}

pub struct ZFOperatorProxy {
    operator: Box<dyn ZFOperator + Send>,
    _lib: Arc<Library>,
}

impl ZFOperator for ZFOperatorProxy {
    fn make_run(&self, ctx: &mut ZFContext) -> Box<OperatorRun> {
        self.operator.make_run(ctx)
    }

    fn get_serialized_state(&self) -> Vec<u8> {
        self.get_serialized_state()
    }
}

pub struct ZFOperatorRegistrar {
    operator: Option<(ZFOperatorId, ZFOperatorProxy)>,
    lib: Arc<Library>,
}

impl ZFOperatorRegistrar {
    fn new(lib: Arc<Library>) -> Self {
        Self {
            lib,
            operator: None,
        }
    }
}

impl ZFOperatorRegistrarTrait for ZFOperatorRegistrar {
    fn register_zfoperator(&mut self, name: &str, operator: Box<dyn ZFOperator + Send>) {
        let proxy = ZFOperatorProxy {
            operator,
            _lib: Arc::clone(&self.lib),
        };
        self.operator = Some((name.to_string(), proxy));
    }
}

pub struct ZFOperatorRunner {
    pub operator: ZFOperatorProxy,
    pub lib: Arc<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
    // pub kind: OperatorKind,
}

impl ZFOperatorRunner {
    pub fn new(operator: ZFOperatorProxy, lib: Arc<Library>) -> Self {
        Self {
            operator,
            lib,
            inputs: vec![],
            outputs: vec![],
            // kind,
        }
    }
}

// impl ZFOperatorRunner {
//     pub async fn run(&mut self) {
//         // WIP empty context
//         let mut ctx = ZFContext {
//             mode: 0,
//             state: self.operator.get_serialized_state(),
//         };

//         // we should start from an HashMap with all ZFLinkId and None
//         let mut msgs: HashMap<ZFLinkId, Option<Arc<ZFMessage>>> = HashMap::new();

//         for i in &self.inputs {
//             msgs.insert(i.id(), None);
//         }

//         loop {
//             // here we should get the run function from the proxy
//             let run_function = self.operator.make_run(&mut ctx);

//             //println!("Op Runner Self: {:?}", self);

//             let mut futs = Vec::new();

//             for rx in &mut self.inputs {
//                 //println!("Rx: {:?} Receivers: {:?}", rx.inner.rx, rx.inner.rx.sender_count());
//                 futs.push(rx.peek());
//             }

//             while !futs.is_empty() {
//                 match future::select_all(futs).await {
//                     //this could be "slow" as suggested by LC
//                     (Ok((id, msg)), _i, remaining) => {
//                         // store the message in the hashmap
//                         msgs.insert(id, Some(msg));

//                         // should operator run take an hashmap of inputs?

//                         match run_function(&mut ctx, &msgs) {
//                             OperatorResult::InResult(Ok((exec, actions))) => {
//                                 match exec {
//                                     true => {
//                                         // If it is true it will not leave the run_function
//                                         unreachable!(
//                                             "Why you are here?? This should never happen!!!"
//                                         )
//                                     }
//                                     false => {
//                                         // here we should verify the tokens and change the futures
//                                         ()
//                                     }
//                                 }
//                             }
//                             OperatorResult::InResult(Err(e)) => {
//                                 panic!(e)
//                             }
//                             OperatorResult::RunResult(e) => {
//                                 panic!(e)
//                             }
//                             OperatorResult::OutResult(Ok(outputs)) => {
//                                 // here we should send downstream

//                                 for (id, zf_msg) in outputs {
//                                     let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
//                                     //println!("Tx: {:?} Receivers: {:?}", tx.inner.tx, tx.inner.tx.receiver_count());
//                                     match zf_msg.msg {
//                                         ZFMsg::Data(_) => {
//                                             tx.send(Arc::new(zf_msg)).await;
//                                         }
//                                         ZFMsg::Ctrl(_) => {
//                                             // here we process should process control messages (eg. change mode)
//                                             //tx.send(zf_msg);
//                                         }
//                                     }
//                                 }

//                                 // tokens are consumed only after the outputs have been sent
//                             }
//                             OperatorResult::OutResult(Err(e)) => {
//                                 panic!(e)
//                             }
//                         }

//                         futs = remaining;
//                     }
//                     (Err(e), i, remaining) => {
//                         println!("Link index {:?} has got error {:?}", i, e);
//                         futs = remaining;
//                     }
//                 }
//             }

//             // let mut result = self.operator.run(data).unwrap();

//             // for tx in &self.outputs {
//             //     //println!("Tx: {:?} Receivers: {:?}", tx.inner.tx, tx.inner.tx.receiver_count());
//             //     tx.write(result.remove(0));
//             // }
//         }
//     }

//     pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
//         self.inputs.push(input);
//     }

//     pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
//         self.outputs.push(output);
//     }
// }
