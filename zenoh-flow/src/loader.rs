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
use crate::message::{ZFMessage, ZFMsg, Message};
use crate::types::{OperatorResult, ZFLinkId, ZFResult, Token, ZFError};
use crate::{OperatorRun, ZFContext, ZFOperator, ZFOperatorId};
use crate::operator::{OperatorTrait, FnInputRule, FnOutputRule, FnRun, StateTrait, DataTrait, SourceTrait, SinkTrait, FnSourceRun, FnSinkRun};
use async_std::sync::Arc;
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use std::io;
use async_trait::async_trait;
use std::pin::Pin;
use std::any::Any;
use crate::downcast;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");


// OPERATOR

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
    fn register_zfoperator(&mut self, name: &str, operator: Box<dyn OperatorTrait + Send>);
}

pub struct ZFOperatorProxy {
    operator: Box<dyn OperatorTrait + Send>,
    _lib: Arc<Library>,
}

impl OperatorTrait for ZFOperatorProxy {


    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule> {
        self.operator.get_input_rule(ctx)
    }

    fn get_output_rule(&self, ctx: &ZFContext) -> Box<FnOutputRule> {
        self.operator.get_output_rule(ctx)
    }

    fn get_run(&self, ctx: &ZFContext) -> Box<FnRun> {
        self.operator.get_run(ctx)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        self.operator.get_state()
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
    fn register_zfoperator(&mut self, name: &str, operator: Box<dyn OperatorTrait + Send>) {
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

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.outputs.push(output);
    }

    pub async fn run(&mut self) -> ZFResult<()>{
        // WIP empty context
        let mut ctx = ZFContext {
            mode: 0,
            state: Some(self.operator.get_state()),
        };


        loop {

            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in &self.inputs {
                msgs.insert(i.id(), Token::new_not_ready(0));
            }

            let ir_fn = self.operator.get_input_rule(&ctx);


            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input
            while !futs.is_empty() {
                match future::select_all(futs).await {
                    //this could be "slow" as suggested by LC
                    (Ok((id,msg)), _i, remaining) => {
                        match &msg.msg {
                            ZFMsg::Data(data_msg) => { //data message
                                match data_msg {
                                    Message::Deserialized(data) => {
                                        msgs.insert(id, Token::new_ready(0,data.clone()));
                                    },
                                    _ => ()
                                };
                                match ir_fn(&mut ctx, &mut msgs) {
                                    Ok(true) => { // we can run
                                        futs = vec![]; // this makes the while loop to end
                                    }
                                    Ok(false) => { //we cannot run, we should update the list of futures
                                        futs = remaining;
                                    }
                                    Err(_) => { // we got an error on the input rules, we should recover/update list of futures
                                        futs = remaining;
                                    },
                                }
                            }
                            ZFMsg::Ctrl(_) => { //control message receiver, we should handle it
                                futs = remaining;
                            }
                        };
                    }
                    (Err(e), i, remaining) => {
                        println!("Link index {:?} has got error {:?}", i, e);
                        futs = remaining;
                    }
                }
            }
            drop(futs);

            // Running
            let run_fn = self.operator.get_run(&ctx);
            let mut data: HashMap<ZFLinkId, Arc<dyn DataTrait>> = HashMap::new();

            for (id,v) in msgs {
                let (d, _) = v.split();
                data.insert(id, d.unwrap());
            }

            let outputs = run_fn(&mut ctx, data)?;

            //Output
            let out_fn = self.operator.get_output_rule(&ctx);

            let out_msgs = out_fn(&mut ctx, outputs)?;

            // Send to Links
            for (id, zf_msg) in out_msgs {
                //getting link
                let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
                //println!("Tx: {:?} Receivers: {:?}", tx.inner.tx, tx.inner.tx.receiver_count());
                match zf_msg.msg {
                    ZFMsg::Data(_) => {
                        tx.send(zf_msg).await?;
                    }
                    ZFMsg::Ctrl(_) => {
                        // here we process should process control messages (eg. change mode)
                        //tx.send(zf_msg);
                    }
                }
            }

            // This depends on the Tokens...
            for rx in self.inputs.iter_mut() {
                rx.drop()?;
            }
        }
    }
}


// SOURCE

pub unsafe fn load_source(path: String) -> Result<(ZFOperatorId, ZFSourceRunner), io::Error> {
    // This is unsafe because has to dynamically load a library
    let library = Arc::new(Library::new(path.clone()).unwrap());
    let decl = library
        .get::<*mut ZFSourceDeclaration>(b"zfsource_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(io::Error::new(io::ErrorKind::Other, "Version mismatch"));
    }
    let mut registrar = ZFSourceRegistrar::new(Arc::clone(&library));

    (decl.register)(&mut registrar);

    let (operator_id, proxy) = registrar.operator.unwrap();

    let runner = ZFSourceRunner::new(proxy, library);
    Ok((operator_id, runner))
}

pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(&mut dyn ZFSourceRegistrarTrait),
}

pub trait ZFSourceRegistrarTrait {
    fn register_zfsource(&mut self, name: &str, operator: Box<dyn SourceTrait  + Send>);
}


pub struct ZFSourceProxy {
    operator: Box<dyn SourceTrait + Send>,
    _lib: Arc<Library>,
}


impl SourceTrait for ZFSourceProxy {

    fn get_run(&self, ctx: &ZFContext) -> Box<FnSourceRun> {
        self.operator.get_run(ctx)
    }
}

pub struct ZFSourceRegistrar {
    operator: Option<(ZFOperatorId, ZFSourceProxy)>,
    lib: Arc<Library>,
}

impl ZFSourceRegistrar {
    fn new(lib: Arc<Library>) -> Self {
        Self {
            lib,
            operator: None,
        }
    }
}

impl ZFSourceRegistrarTrait for ZFSourceRegistrar {
    fn register_zfsource(&mut self, name: &str, operator: Box<dyn SourceTrait + Send>) {
        let proxy = ZFSourceProxy {
            operator,
            _lib: Arc::clone(&self.lib),
        };
        self.operator = Some((name.to_string(), proxy));
    }
}


pub struct ZFSourceRunner {
    pub operator: ZFSourceProxy,
    pub lib: Arc<Library>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
}

impl ZFSourceRunner {
    pub fn new(operator: ZFSourceProxy, lib: Arc<Library>) -> Self {
        Self {
            operator,
            lib,
            outputs: vec![],
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.outputs.push(output);
    }

    pub async fn run(&mut self) -> ZFResult<()>{
        // WIP empty context
        let mut ctx = ZFContext {
            mode: 0,
            state: None, //self.operator.get_state(),
        };


        loop {
            // Running
            let run_fn = self.operator.get_run(&ctx);
            let outputs = run_fn(&mut ctx)?;
            // Send to links
            for (id, data) in outputs {
                let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
                let msg = Arc::new(ZFMessage::new_deserialized(0, data));
                tx.send(msg).await?;
            }

        }
    }
}

// SINK

pub unsafe fn load_sink(path: String) -> Result<(ZFOperatorId, ZFSinkRunner), io::Error> {
    // This is unsafe because has to dynamically load a library
    let library = Arc::new(Library::new(path.clone()).unwrap());
    let decl = library
        .get::<*mut ZFSinkDeclaration>(b"zfsink_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(io::Error::new(io::ErrorKind::Other, "Version mismatch"));
    }
    let mut registrar = ZFSinkRegistrar::new(Arc::clone(&library));

    (decl.register)(&mut registrar);

    let (operator_id, proxy) = registrar.operator.unwrap();

    let runner = ZFSinkRunner::new(proxy, library);
    Ok((operator_id, runner))
}

pub struct ZFSinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(&mut dyn ZFSinkRegistrarTrait),
}

pub trait ZFSinkRegistrarTrait {
    fn register_zfsink(&mut self, name: &str, operator: Box<dyn SinkTrait  + Send>);
}

pub struct ZFSinkProxy {
    operator: Box<dyn SinkTrait + Send>,
    _lib: Arc<Library>,
}

impl SinkTrait for ZFSinkProxy {

    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule> {
        self.operator.get_input_rule(ctx)
    }

    fn get_run(&self, ctx: &ZFContext) -> Box<FnSinkRun> {
        self.operator.get_run(ctx)
    }

}


pub struct ZFSinkRegistrar {
    operator: Option<(ZFOperatorId, ZFSinkProxy)>,
    lib: Arc<Library>,
}

impl ZFSinkRegistrar {
    fn new(lib: Arc<Library>) -> Self {
        Self {
            lib,
            operator: None,
        }
    }
}

impl ZFSinkRegistrarTrait for ZFSinkRegistrar {
    fn register_zfsink(&mut self, name: &str, operator: Box<dyn SinkTrait + Send>) {
        let proxy = ZFSinkProxy {
            operator,
            _lib: Arc::clone(&self.lib),
        };
        self.operator = Some((name.to_string(), proxy));
    }
}

pub struct ZFSinkRunner {
    pub operator: ZFSinkProxy,
    pub lib: Arc<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
}

impl ZFSinkRunner {
    pub fn new(operator: ZFSinkProxy, lib: Arc<Library>) -> Self {
        Self {
            operator,
            lib,
            inputs: vec![],
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub async fn run(&mut self) -> ZFResult<()>{
        // WIP empty context
        let mut ctx = ZFContext {
            mode: 0,
            state: None,//Box::new(self.operator.get_state()),
        };
        loop {

            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in self.inputs.iter() {
                msgs.insert(i.id(), Token::new_not_ready(0));
            }

            let ir_fn = self.operator.get_input_rule(&ctx);



            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input
            while !futs.is_empty() {
                match future::select_all(futs).await {
                    //this could be "slow" as suggested by LC
                    (Ok((id,msg)), _i, remaining) => {
                        //println!("Received from link {:?} -> {:?}", id, msg);
                        match &msg.msg {
                            ZFMsg::Data(data_msg) => { //data message
                                match data_msg {
                                    Message::Deserialized(data) => {
                                        msgs.insert(id, Token::new_ready(0,data.clone()));
                                    },
                                    _ => ()
                                };
                                match ir_fn(&mut ctx, &mut msgs) {
                                    Ok(true) => { // we can run
                                        futs = vec![]; // this makes the while loop to end
                                    }
                                    Ok(false) => { //we cannot run, we should update the list of futures
                                        futs = remaining;
                                        //()
                                    }
                                    Err(err) => { // we got an error on the input rules, we should recover/update list of futures
                                        println!("Got error from IR: {:?}", err);
                                        futs = remaining;
                                        //()
                                    },
                                }
                            }
                            ZFMsg::Ctrl(_) => { //control message receiver, we should handle it
                                futs = remaining;
                                //()
                            }
                        };
                    }
                    (Err(e), i, remaining) => {
                        println!("Link index {:?} has got error {:?}", i, e);
                        futs = remaining;
                        //()
                    }
                }
            }
            drop(futs);

            // Running
            let run_fn = self.operator.get_run(&ctx);
            let mut data: HashMap<ZFLinkId, Arc<dyn DataTrait>> = HashMap::new();

            for (id,v) in msgs {
                let (d, _) = v.split();
                data.insert(id, d.unwrap());
            }

            let _ = run_fn(&mut ctx, data);

            //This depends on the Tokens...
            for rx in self.inputs.iter_mut() {
                rx.drop()?;
            }
        }
    }

}