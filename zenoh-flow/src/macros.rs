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
/// impl OperatorFactoryTrait for MyOperator {
/// async fn new_operator(
///     &self,
///     context: &mut Context,
///     configuration: &Option<Configuration>,
///     inputs: Inputs,
///     outputs: Outputs,
/// ) -> Result<Option<Arc<dyn Node>>> {
///         todo!()
///     }
/// }
///
/// export_operator!(register);
///
/// fn register() -> Result<Arc<dyn OperatorFactoryTrait>> {
///    Ok(Arc::new(MyOperator) as Arc<dyn OperatorFactoryTrait>)
/// }
/// ```
///
#[macro_export]
macro_rules! export_operator {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfoperator_declaration: $crate::runtime::dataflow::loader::NodeDeclaration<
            OperatorFactoryTrait,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<OperatorFactoryTrait> {
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
/// impl SourceFactoryTrait for MySource {
///   async fn new_source(
///       &self,
///       context: &mut Context,
///       configuration: &Option<Configuration>,
///       outputs: Outputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///         todo!()
///     }
/// }
///
/// export_source!(register);
///
/// fn register() -> Result<Arc<dyn SourceFactoryTrait>> {
///    Ok(Arc::new(MySource) as Arc<dyn SourceFactoryTrait>)
/// }
///
/// ```
///
#[macro_export]
macro_rules! export_source {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsource_declaration: $crate::runtime::dataflow::loader::NodeDeclaration<
            SourceFactoryTrait,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<SourceFactoryTrait> {
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
/// impl SinkFactoryTrait for MySink {
///   async fn new_sink(
///       &self,
///       context: &mut Context,
///       configuration: &Option<Configuration>,
///       inputs: Inputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///         todo!()
///     }
/// }
///
/// export_sink!(register);
///
///
/// fn register() -> Result<Arc<dyn SinkFactoryTrait>> {
///    Ok(Arc::new(MySink) as Arc<dyn SinkFactoryTrait>)
/// }
///
/// ```
///
#[macro_export]
macro_rules! export_sink {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsink_declaration: $crate::runtime::dataflow::loader::NodeDeclaration<
            SinkFactoryTrait,
        > = $crate::runtime::dataflow::loader::NodeDeclaration::<SinkFactoryTrait> {
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
