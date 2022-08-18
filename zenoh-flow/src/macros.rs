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
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// pub struct MyOperator;
///
///
/// #[async_trait]
/// impl Operator for MyOperator {
///     async fn setup(
///         &self,
///         context: &mut Context,
///         configuration: &Option<Configuration>,
///         mut inputs: Inputs,
///         mut outputs: Outputs,
///     ) -> Result<Option<Arc<dyn AsyncIteration>>> {
///         todo!()
///     }
/// }
///
/// export_operator!(register);
///
/// fn register() -> Result<Arc<dyn Operator>> {
///    Ok(Arc::new(MyOperator) as Arc<dyn Operator>)
/// }
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
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// pub struct MySource;
///
///
/// #[async_trait]
/// impl Source for MySource {
///     async fn setup(
///         &self,
///         _context: &mut Context,
///         configuration: &Option<Configuration>,
///         mut outputs: Outputs,
///     ) -> Result<Option<Arc<dyn AsyncIteration>>> {
///         todo!()
///     }
/// }
///
/// export_source!(register);
///
/// fn register() -> Result<Arc<dyn Source>> {
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
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// pub struct MySink;
///
///
/// #[async_trait]
/// impl Sink for MySink {
///     async fn setup(
///         &self,
///         _context: &mut Context,
///         configuration: &Option<Configuration>,
///         mut inputs: Inputs,
///     ) -> Result<Option<Arc<dyn AsyncIteration>>> {
///         todo!()
///     }
/// }
///
/// export_sink!(register);
///
///
/// fn register() -> Result<Arc<dyn Sink>> {
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
