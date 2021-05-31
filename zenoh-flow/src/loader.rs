use crate::{OperatorRun, ZFContext, ZFOperator, ZFOperatorId};
use libloading::Library;
use std::io;
use std::sync::Arc;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub unsafe fn load_operator(path: String) -> Result<(ZFOperatorId, ZFOperatorRunner), io::Error> { // This is unsafe because has to dynamically load a library
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
    // pub inputs: Vec<InputStream>,
    // pub outputs: Vec<OutputStream>,
    // pub kind: OperatorKind,
}

impl ZFOperatorRunner {
    pub fn new(operator: ZFOperatorProxy, lib: Arc<Library>) -> Self {
        Self {
            operator,
            lib,
            // inputs: vec![],
            // outputs: vec![],
            // kind,
        }
    }
}

// impl ZFOperatorRunner {
//     pub fn run(&self) {
//         // waiting all inputs blocking

//         loop {
//             //println!("Op Runner Self: {:?}", self);

//             let mut data = Vec::new();
//             for rx in &self.inputs {
//                 //println!("Rx: {:?} Receivers: {:?}", rx.inner.rx, rx.inner.rx.sender_count());
//                 data.push(rx.read());
//             }

//             let mut result = self.operator.run(data).unwrap();

//             for tx in &self.outputs {
//                 //println!("Tx: {:?} Receivers: {:?}", tx.inner.tx, tx.inner.tx.receiver_count());
//                 tx.write(result.remove(0));
//             }
//         }
//     }

//     pub fn add_input(&mut self, input: InputStream) {
//         self.inputs.push(input);
//     }

//     pub fn add_output(&mut self, output: OutputStream) {
//         self.outputs.push(output);
//     }
// }
