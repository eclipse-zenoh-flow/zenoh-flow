//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

/// This macros should be used in order to provide the symbols
/// for the dynamic load of an Operator. Along with a register function
///
/// Example:
///
/// ```no_run
/// use std::collections::HashMap;
/// use async_std::sync::Arc;
/// use zenoh_flow::{ZFResult, Operator, export_operator, Node, zf_empty_state,
///     State, Configuration, Context, DataMessage, default_input_rule,
///     default_output_rule, PortId, InputToken, LocalDeadlineMiss,
///     NodeOutput, Data};
///
/// pub struct MyOperator;
///
/// impl Node for MyOperator {
///     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
///         zf_empty_state!()
///     }
///
///     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
///         Ok(())
///     }
/// }
///
/// impl Operator for MyOperator {
///     fn input_rule(
///         &self,
///         _context: &mut Context,
///         state: &mut State,
///         tokens: &mut HashMap<PortId, InputToken>
///     ) -> ZFResult<bool> {
///         default_input_rule(state, tokens)
///     }
///
///     fn run(
///         &self,
///         context: &mut Context,
///         state: &mut State,
///         inputs: &mut HashMap<PortId, DataMessage>,
///      ) -> ZFResult<HashMap<PortId, Data>> {
///         panic!("Not implemented...")
///      }
///
///     fn output_rule(
///        &self,
///        _context: &mut Context,
///        state: &mut State,
///        outputs: HashMap<PortId, Data>,
///        _deadline_miss: Option<LocalDeadlineMiss>,
///      ) -> ZFResult<HashMap<PortId, NodeOutput>> {
///         default_output_rule(state, outputs)
///      }
/// }
///
/// export_operator!(register);
///
///
///
/// fn register() -> ZFResult<Arc<dyn Operator>> {
///    Ok(Arc::new(MyOperator) as Arc<dyn Operator>)
/// }
///
/// ```
///
#[macro_export]
macro_rules! export_operator {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfoperator_declaration: $crate::runtime::dataflow::loader::OperatorDeclaration =
            $crate::runtime::dataflow::loader::OperatorDeclaration {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: $register,
            };
    };
}

/// This macros should be used in order to provide the symbols
/// for the dynamic load of an Source. Along with a register function
///
/// Example:
///
/// ```no_run
/// use async_trait::async_trait;
/// use async_std::sync::Arc;
/// use zenoh_flow::{ZFResult, Source, export_source, Node, zf_empty_state,
///     State, Configuration, Context, Data};
///
/// pub struct MySource;
///
/// impl Node for MySource {
///     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
///         zf_empty_state!()
///     }
///
///     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
///         Ok(())
///     }
/// }
///
/// #[async_trait]
/// impl Source for MySource {
///     async fn run(&self, _context: &mut Context, _state: &mut State) -> ZFResult<Data> {
///         panic!("Not implemented...")
///     }
/// }
///
/// export_source!(register);
///
///
/// fn register() -> ZFResult<Arc<dyn Source>> {
///    Ok(Arc::new(MySource) as Arc<dyn Source>)
/// }
///
/// ```
///
#[macro_export]
macro_rules! export_source {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsource_declaration: $crate::runtime::dataflow::loader::SourceDeclaration =
            $crate::runtime::dataflow::loader::SourceDeclaration {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: $register,
            };
    };
}

/// This macros should be used in order to provide the symbols
/// for the dynamic load of a Sink. Along with a register function
///
/// Example:
///
/// ```no_run
/// use async_trait::async_trait;
/// use async_std::sync::Arc;
/// use zenoh_flow::{ZFResult, Sink, export_sink, Node, zf_empty_state,
///     State, Configuration, Context, DataMessage};
///
/// pub struct MySink;
///
/// impl Node for MySink {
///     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
///         zf_empty_state!()
///     }
///
///     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
///         Ok(())
///     }
/// }
///
/// #[async_trait]
/// impl Sink for MySink {
///     async fn run(&self, _context: &mut Context, _state: &mut State, _input : DataMessage)  -> ZFResult<()> {
///         Ok(())
///     }
/// }
///
/// export_sink!(register);
///
///
/// fn register() -> ZFResult<Arc<dyn Sink>> {
///    Ok(Arc::new(MySink) as Arc<dyn Sink>)
/// }
///
/// ```
///
#[macro_export]
macro_rules! export_sink {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsink_declaration: $crate::runtime::dataflow::loader::SinkDeclaration =
            $crate::runtime::dataflow::loader::SinkDeclaration {
                rustc_version: $crate::runtime::dataflow::loader::RUSTC_VERSION,
                core_version: $crate::runtime::dataflow::loader::CORE_VERSION,
                register: $register,
            };
    };
}

/// Spin lock over an [`async_std::sync::Mutex`](`async_std::sync::Mutex`)
/// Note: This is intended for internal usage.
#[macro_export]
macro_rules! zf_spin_lock {
    ($val : expr) => {
        loop {
            match $val.try_lock() {
                Some(x) => break x,
                None => std::hint::spin_loop(),
            }
        }
    };
}

/// This macro is a helper if your node does not need any state.
/// It can be used inside your implementation of `Node::intialize`
///
/// Example:
///
/// ```no_run
/// use zenoh_flow::{zf_empty_state, Node, ZFResult, State, Configuration};
///
/// struct MyOp;
///
///
///  impl Node for MyOp {
///     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
///         zf_empty_state!()
///     }
///
///     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
///         Ok(())
///     }
///  }
///
/// ```
#[macro_export]
macro_rules! zf_empty_state {
    () => {
        Ok(zenoh_flow::State::from::<zenoh_flow::EmptyState>(
            zenoh_flow::EmptyState {},
        ))
    };
}
