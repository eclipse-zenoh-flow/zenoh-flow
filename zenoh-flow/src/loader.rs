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

use crate::async_std::sync::Arc;
use crate::link::{ZFLinkReceiver, ZFLinkSender};
use crate::message::{Message, ZFMessage, ZFMsg};
use crate::operator::{
    DataTrait, FnInputRule, FnOutputRule, FnRun, FnSinkRun, FnSourceRun, OperatorTrait, SinkTrait,
    SourceTrait, StateTrait,
};
use crate::types::{Token, ZFError, ZFLinkId, ZFResult};
use crate::{ZFContext, ZFOperatorId};
use futures::future;
use libloading::Library;
use std::collections::HashMap;
use zenoh::net::Session;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

// OPERATOR

/// # Safety
///
/// TODO remove all copy-pasted code, make macros/functions instead
pub unsafe fn load_operator(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFOperatorRunner)> {
    // This is unsafe because has to dynamically load a library
    let library = Arc::new(Library::new(path).unwrap());
    let decl = library
        .get::<*mut ZFOperatorDeclaration>(b"zfoperator_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }
    let mut registrar = ZFOperatorRegistrar::new(Arc::clone(&library));

    (decl.register)(&mut registrar, configuration)?;

    let (operator_id, proxy) = registrar.operator.unwrap();

    let runner = ZFOperatorRunner::new_dynamic(proxy, library);
    Ok((operator_id, runner))
}

pub struct ZFOperatorDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        &mut dyn ZFOperatorRegistrarTrait,
        Option<HashMap<String, String>>,
    ) -> ZFResult<()>,
}

pub trait ZFOperatorRegistrarTrait {
    fn register_zfoperator(&mut self, name: &str, operator: Box<dyn OperatorTrait + Send>);
}

pub struct ZFOperatorProxy {
    operator: Box<dyn OperatorTrait + Send>,
    _lib: Arc<Library>,
}

impl OperatorTrait for ZFOperatorProxy {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule> {
        self.operator.get_input_rule(ctx.clone())
    }

    fn get_output_rule(&self, ctx: ZFContext) -> Box<FnOutputRule> {
        self.operator.get_output_rule(ctx.clone())
    }

    fn get_run(&self, ctx: ZFContext) -> Box<FnRun> {
        self.operator.get_run(ctx.clone())
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

pub enum ZFOperatorRunner {
    Dynamic(ZFOperatorRunnerDynamic),
    Static(ZFOperatorRunnerStatic),
}

impl ZFOperatorRunner {
    pub fn new_dynamic(operator: ZFOperatorProxy, lib: Arc<Library>) -> Self {
        Self::Dynamic(ZFOperatorRunnerDynamic::new(operator, lib))
    }

    pub fn new_static(operator: Box<dyn OperatorTrait + Send>) -> Self {
        Self::Static(ZFOperatorRunnerStatic::new(operator))
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        match self {
            ZFOperatorRunner::Dynamic(dyn_op) => dyn_op.add_input(input),
            ZFOperatorRunner::Static(sta_op) => sta_op.add_input(input),
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        match self {
            ZFOperatorRunner::Dynamic(dyn_op) => dyn_op.add_output(output),
            ZFOperatorRunner::Static(sta_op) => sta_op.add_output(output),
        }
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            ZFOperatorRunner::Dynamic(dyn_op) => dyn_op.run().await,
            ZFOperatorRunner::Static(sta_op) => sta_op.run().await,
        }
    }
}

pub struct ZFOperatorRunnerDynamic {
    pub operator: ZFOperatorProxy,
    pub lib: Arc<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
    // pub kind: OperatorKind,
}

impl ZFOperatorRunnerDynamic {
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

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in &self.inputs {
                msgs.insert(i.id(), Token::new_not_ready(0));
            }

            let ir_fn = self.operator.get_input_rule(ctx.clone());

            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input
            while !futs.is_empty() {
                match future::select_all(futs).await {
                    // this could be "slow" as suggested by LC
                    (Ok((id, msg)), _i, remaining) => {
                        match &msg.msg {
                            ZFMsg::Data(data_msg) => {
                                //data message
                                match data_msg {
                                    Message::Deserialized(data) => {
                                        log::debug!("Channel {:?} received: {:?}", id, data);
                                        msgs.insert(id, Token::new_ready(0, data.clone()));
                                    }
                                    _ => (),
                                };

                                match ir_fn(ctx.clone(), &mut msgs) {
                                    Ok(true) => {
                                        // we can run
                                        log::debug!("IR: OK");
                                        futs = vec![]; // this makes the while loop to end
                                    }
                                    Ok(false) => {
                                        //we cannot run, we should update the list of futures
                                        log::debug!("IR: Not OK");
                                        futs = remaining;
                                    }
                                    Err(_) => {
                                        // we got an error on the input rules, we should recover/update list of futures
                                        log::debug!("IR: received an error");
                                        futs = remaining;
                                    }
                                }
                            }
                            ZFMsg::Ctrl(_) => {
                                //control message receiver, we should handle it
                                futs = remaining;
                            }
                        };
                    }
                    (Err(e), i, remaining) => {
                        log::debug!("Link index {:?} has got error {:?}", i, e);
                        futs = remaining;
                    }
                }
            }
            drop(futs);

            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let mut data: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::new();

            for (id, v) in msgs {
                let (d, _) = v.split();
                data.insert(id, d.unwrap());
            }

            let outputs = run_fn(ctx.clone(), data)?;

            //Output
            let out_fn = self.operator.get_output_rule(ctx.clone());

            let out_msgs = out_fn(ctx.clone(), outputs)?;

            // Send to Links
            for (id, zf_msg) in out_msgs {
                //getting link
                log::debug!("id: {:?}, zf_msg: {:?}", id, zf_msg);
                let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
                log::debug!("Sending on: {:?}", tx);
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

pub struct ZFOperatorRunnerStatic {
    pub operator: Box<dyn OperatorTrait + Send>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
}

impl ZFOperatorRunnerStatic {
    pub fn new(operator: Box<dyn OperatorTrait + Send>) -> Self {
        Self {
            operator,
            inputs: vec![],
            outputs: vec![],
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.outputs.push(output);
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in &self.inputs {
                msgs.insert(i.id(), Token::new_not_ready(0));
            }

            let ir_fn = self.operator.get_input_rule(ctx.clone());

            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input
            while !futs.is_empty() {
                match future::select_all(futs).await {
                    //this could be "slow" as suggested by LC
                    (Ok((id, msg)), _i, remaining) => {
                        match &msg.msg {
                            ZFMsg::Data(data_msg) => {
                                //data message
                                match data_msg {
                                    Message::Deserialized(data) => {
                                        msgs.insert(id, Token::new_ready(0, data.clone()));
                                    }
                                    _ => (),
                                };
                                match ir_fn(ctx.clone(), &mut msgs) {
                                    Ok(true) => {
                                        // we can run
                                        futs = vec![]; // this makes the while loop to end
                                    }
                                    Ok(false) => {
                                        //we cannot run, we should update the list of futures
                                        futs = remaining;
                                    }
                                    Err(_) => {
                                        // we got an error on the input rules, we should recover/update list of futures
                                        futs = remaining;
                                    }
                                }
                            }
                            ZFMsg::Ctrl(_) => {
                                //control message receiver, we should handle it
                                futs = remaining;
                            }
                        };
                    }
                    (Err(e), i, remaining) => {
                        log::debug!("Link index {:?} has got error {:?}", i, e);
                        futs = remaining;
                    }
                }
            }
            drop(futs);

            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let mut data: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::new();

            for (id, v) in msgs {
                let (d, _) = v.split();
                data.insert(id, d.unwrap());
            }

            let outputs = run_fn(ctx.clone(), data)?;

            //Output
            let out_fn = self.operator.get_output_rule(ctx.clone());

            let out_msgs = out_fn(ctx.clone(), outputs)?;

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
pub unsafe fn load_zenoh_receiver(
    path: String,
    session: Arc<Session>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSourceRunner)> {
    load_source_or_receiver(path, Some(session), configuration)
}

pub unsafe fn load_source(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSourceRunner)> {
    load_source_or_receiver(path, None, configuration)
}

unsafe fn load_source_or_receiver(
    path: String,
    zenoh_session: Option<Arc<Session>>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSourceRunner)> {
    // This is unsafe because has to dynamically load a library
    let library = Arc::new(Library::new(path).unwrap());
    let mut registrar = ZFSourceRegistrar::new(Arc::clone(&library));

    if let Some(session) = zenoh_session {
        let decl = library
            .get::<*mut ZFZenohReceiverDeclaration>(b"zfsource_declaration\0")
            .unwrap()
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        (decl.register)(&mut registrar, session, configuration)?;
    } else {
        let decl = library
            .get::<*mut ZFSourceDeclaration>(b"zfsource_declaration\0")
            .unwrap()
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        (decl.register)(&mut registrar, configuration)?;
    }

    let (operator_id, proxy) = registrar.operator.unwrap();

    let runner = ZFSourceRunner::new_dynamic(proxy, library);
    Ok((operator_id, runner))
}

pub struct ZFSourceDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        &mut dyn ZFSourceRegistrarTrait,
        Option<HashMap<String, String>>,
    ) -> ZFResult<()>,
}

pub struct ZFZenohReceiverDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        &mut dyn ZFSourceRegistrarTrait,
        Arc<Session>,
        Option<HashMap<String, String>>,
    ) -> ZFResult<()>,
}

pub trait ZFSourceRegistrarTrait {
    fn register_zfsource(&mut self, name: &str, operator: Box<dyn SourceTrait + Send>);
}

pub struct ZFSourceProxy {
    operator: Box<dyn SourceTrait + Send>,
    _lib: Arc<Library>,
}

impl SourceTrait for ZFSourceProxy {
    fn get_run(&self, ctx: ZFContext) -> FnSourceRun {
        self.operator.get_run(ctx.clone())
    }

    fn get_output_rule(&self, ctx: ZFContext) -> Box<FnOutputRule> {
        self.operator.get_output_rule(ctx)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        self.operator.get_state()
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

pub enum ZFSourceRunner {
    Dynamic(ZFSourceRunnerDynamic),
    Static(ZFSourceRunnerStatic),
}

impl ZFSourceRunner {
    pub fn new_dynamic(operator: ZFSourceProxy, lib: Arc<Library>) -> Self {
        Self::Dynamic(ZFSourceRunnerDynamic::new(operator, lib))
    }

    pub fn new_static(operator: Box<dyn SourceTrait + Send>) -> Self {
        Self::Static(ZFSourceRunnerStatic::new(operator))
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        match self {
            ZFSourceRunner::Dynamic(dyn_op) => dyn_op.add_output(output),
            ZFSourceRunner::Static(sta_op) => sta_op.add_output(output),
        }
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            ZFSourceRunner::Dynamic(dyn_op) => dyn_op.run().await,
            ZFSourceRunner::Static(sta_op) => sta_op.run().await,
        }
    }
}

pub struct ZFSourceRunnerDynamic {
    pub operator: ZFSourceProxy,
    pub lib: Arc<Library>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
}

impl ZFSourceRunnerDynamic {
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

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let outputs = run_fn(ctx.clone()).await?;

            //Output
            let out_fn = self.operator.get_output_rule(ctx.clone());

            let out_msgs = out_fn(ctx.clone(), outputs)?;
            log::debug!("Outputs: {:?}", self.outputs);

            // Send to Links
            for (id, zf_msg) in out_msgs {
                log::debug!("Sending on {:?} data: {:?}", id, zf_msg);
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
        }
    }
}

pub struct ZFSourceRunnerStatic {
    pub operator: Box<dyn SourceTrait + Send>,
    pub outputs: Vec<ZFLinkSender<ZFMessage>>,
}

impl ZFSourceRunnerStatic {
    pub fn new(operator: Box<dyn SourceTrait + Send>) -> Self {
        Self {
            operator,
            outputs: vec![],
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.outputs.push(output);
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let outputs = run_fn(ctx.clone()).await?;
            // Send to links
            for (id, data) in outputs {
                let tx = self.outputs.iter().find(|&x| x.id() == id).unwrap();
                let msg = Arc::new(ZFMessage::new_deserialized(0, data));
                tx.send(msg).await?;
            }
        }
    }
}

// CONNECTOR SENDER — Similar to a SINK (see below)
pub unsafe fn load_zenoh_sender(
    path: String,
    session: Arc<Session>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSinkRunner)> {
    load_sink_or_sender(path, Some(session), configuration)
}

// SINK

pub unsafe fn load_sink(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSinkRunner)> {
    load_sink_or_sender(path, None, configuration)
}

unsafe fn load_sink_or_sender(
    path: String,
    zenoh_session: Option<Arc<Session>>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSinkRunner)> {
    // This is unsafe because has to dynamically load a library
    log::debug!("Loading {}", path);
    let library = Arc::new(Library::new(path).unwrap());
    let mut registrar = ZFSinkRegistrar::new(Arc::clone(&library));

    if let Some(session) = zenoh_session {
        let decl = library
            .get::<*mut ZFZenohSenderDeclaration>(b"zfsink_declaration\0")
            .unwrap()
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        (decl.register)(&mut registrar, session, configuration)?;
    } else {
        let decl = library
            .get::<*mut ZFSinkDeclaration>(b"zfsink_declaration\0")
            .unwrap()
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
            return Err(ZFError::VersionMismatch);
        }

        (decl.register)(&mut registrar, configuration)?;
    }

    let (operator_id, proxy) = registrar.operator.unwrap();

    let runner = ZFSinkRunner::new_dynamic(proxy, library);
    Ok((operator_id, runner))
}

pub struct ZFZenohSenderDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        &mut dyn ZFSinkRegistrarTrait,
        Arc<Session>,
        Option<HashMap<String, String>>,
    ) -> ZFResult<()>,
}

pub struct ZFSinkDeclaration {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub register: unsafe extern "C" fn(
        &mut dyn ZFSinkRegistrarTrait,
        Option<HashMap<String, String>>,
    ) -> ZFResult<()>,
}

pub trait ZFSinkRegistrarTrait {
    fn register_zfsink(&mut self, name: &str, operator: Box<dyn SinkTrait + Send>);
}

pub struct ZFSinkProxy {
    operator: Box<dyn SinkTrait + Send>,
    _lib: Arc<Library>,
}

impl SinkTrait for ZFSinkProxy {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule> {
        self.operator.get_input_rule(ctx.clone())
    }

    fn get_run(&self, ctx: ZFContext) -> FnSinkRun {
        self.operator.get_run(ctx.clone())
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        self.operator.get_state()
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

pub enum ZFSinkRunner {
    Dynamic(ZFSinkRunnerDynamic),
    Static(ZFSinkRunnerStatic),
}

impl ZFSinkRunner {
    pub fn new_dynamic(operator: ZFSinkProxy, lib: Arc<Library>) -> Self {
        Self::Dynamic(ZFSinkRunnerDynamic::new(operator, lib))
    }

    pub fn new_static(operator: Box<dyn SinkTrait + Send>) -> Self {
        Self::Static(ZFSinkRunnerStatic::new(operator))
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        match self {
            ZFSinkRunner::Dynamic(dyn_op) => dyn_op.add_input(input),
            ZFSinkRunner::Static(sta_op) => sta_op.add_input(input),
        }
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            ZFSinkRunner::Dynamic(dyn_op) => dyn_op.run().await,
            ZFSinkRunner::Static(sta_op) => sta_op.run().await,
        }
    }
}

pub struct ZFSinkRunnerDynamic {
    pub operator: ZFSinkProxy,
    pub lib: Arc<Library>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
}

impl ZFSinkRunnerDynamic {
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

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in self.inputs.iter() {
                msgs.insert(i.id(), Token::new_not_ready(0));
            }

            let ir_fn = self.operator.get_input_rule(ctx.clone());

            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input
            while !futs.is_empty() {
                match future::select_all(futs).await {
                    //this could be "slow" as suggested by LC
                    (Ok((id, msg)), _i, remaining) => {
                        //println!("Received from link {:?} -> {:?}", id, msg);
                        match &msg.msg {
                            ZFMsg::Data(data_msg) => {
                                //data message
                                match data_msg {
                                    Message::Deserialized(data) => {
                                        msgs.insert(id, Token::new_ready(0, data.clone()));
                                    }
                                    _ => (),
                                };
                                match ir_fn(ctx.clone(), &mut msgs) {
                                    Ok(true) => {
                                        // we can run
                                        futs = vec![]; // this makes the while loop to end
                                    }
                                    Ok(false) => {
                                        //we cannot run, we should update the list of futures
                                        futs = remaining;
                                        //()
                                    }
                                    Err(err) => {
                                        // we got an error on the input rules, we should recover/update list of futures
                                        log::debug!("Got error from IR: {:?}", err);
                                        futs = remaining;
                                        //()
                                    }
                                }
                            }
                            ZFMsg::Ctrl(_) => {
                                //control message receiver, we should handle it
                                futs = remaining;
                                //()
                            }
                        };
                    }
                    (Err(e), i, remaining) => {
                        log::debug!("Link index {:?} has got error {:?}", i, e);
                        futs = remaining;
                        //()
                    }
                }
            }
            drop(futs);

            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let mut data: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::new();

            for (id, v) in msgs {
                log::debug!("[SINK] Sending data to run: {:?}", v);
                let (d, _) = v.split();
                if d.is_none() {
                    continue;
                }
                data.insert(id, d.unwrap());
            }

            run_fn(ctx.clone(), data).await?;

            //This depends on the Tokens...
            for rx in self.inputs.iter_mut() {
                rx.drop()?;
            }
        }
    }
}

pub struct ZFSinkRunnerStatic {
    pub operator: Box<dyn SinkTrait + Send>,
    pub inputs: Vec<ZFLinkReceiver<ZFMessage>>,
}

impl ZFSinkRunnerStatic {
    pub fn new(operator: Box<dyn SinkTrait + Send>) -> Self {
        Self {
            operator,
            inputs: vec![],
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.inputs.push(input);
    }

    pub async fn run(&mut self) -> ZFResult<()> {
        // WIP empty context
        let ctx = ZFContext::new(self.operator.get_state(), 0);

        loop {
            // we should start from an HashMap with all ZFLinkId and not ready tokens
            let mut msgs: HashMap<ZFLinkId, Token> = HashMap::new();

            for i in self.inputs.iter() {
                msgs.insert(i.id(), Token::new_not_ready(0));
            }

            let ir_fn = self.operator.get_input_rule(ctx.clone());

            let mut futs = vec![];
            for rx in self.inputs.iter_mut() {
                futs.push(rx.recv()); // this should be peek(), but both requires mut
            }

            // Input
            while !futs.is_empty() {
                match future::select_all(futs).await {
                    //this could be "slow" as suggested by LC
                    (Ok((id, msg)), _i, remaining) => {
                        //println!("Received from link {:?} -> {:?}", id, msg);
                        match &msg.msg {
                            ZFMsg::Data(data_msg) => {
                                //data message
                                match data_msg {
                                    Message::Deserialized(data) => {
                                        msgs.insert(id, Token::new_ready(0, data.clone()));
                                    }
                                    _ => (),
                                };
                                match ir_fn(ctx.clone(), &mut msgs) {
                                    Ok(true) => {
                                        // we can run
                                        futs = vec![]; // this makes the while loop to end
                                    }
                                    Ok(false) => {
                                        //we cannot run, we should update the list of futures
                                        futs = remaining;
                                        //()
                                    }
                                    Err(err) => {
                                        // we got an error on the input rules, we should recover/update list of futures
                                        log::debug!("Got error from IR: {:?}", err);
                                        futs = remaining;
                                        //()
                                    }
                                }
                            }
                            ZFMsg::Ctrl(_) => {
                                //control message receiver, we should handle it
                                futs = remaining;
                                //()
                            }
                        };
                    }
                    (Err(e), i, remaining) => {
                        log::debug!("Link index {:?} has got error {:?}", i, e);
                        futs = remaining;
                        //()
                    }
                }
            }
            drop(futs);

            // Running
            let run_fn = self.operator.get_run(ctx.clone());
            let mut data: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::new();

            for (id, v) in msgs {
                let (d, _) = v.split();
                data.insert(id, d.unwrap());
            }

            run_fn(ctx.clone(), data).await?;

            //This depends on the Tokens...
            for rx in self.inputs.iter_mut() {
                rx.drop()?;
            }
        }
    }
}
