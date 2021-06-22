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

pub use ::zenoh_flow_macros;

pub use ::async_std;
pub use ::bincode;
pub use ::paste;
pub use ::serde;
pub use ::typetag;

pub mod graph;
pub mod link;
pub mod loader;
pub mod message;
pub mod operator;

pub mod types;
pub use types::*;

/// This trait will be implemented by the zfoperator proc_macro
pub trait ZFOperator {
    fn make_run(&self, ctx: &mut ZFContext) -> Box<OperatorRun>;

    // TODO Add function to get the name of the ports.
    // TODO Add function to get the port by name.

    // TODO Inputs and Outputs become HashMaps<String, T>

    fn get_serialized_state(&self) -> Vec<u8>;
}

pub trait ZFSource {
    fn make_source(&self, ctx: &mut ZFContext) -> Box<ZFSourceRun>;
}

pub trait ZFSink {
    fn make_sink(&self, ctx: &mut ZFContext) -> Box<ZFSinkRun>;
}

#[macro_export]
macro_rules! export_operator {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfoperator_declaration: $crate::loader::ZFOperatorDeclaration =
            $crate::loader::ZFOperatorDeclaration {
                rustc_version: $crate::loader::RUSTC_VERSION,
                core_version: $crate::loader::CORE_VERSION,
                register: $register,
            };
    };
}

#[macro_export]
macro_rules! export_source {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsource_declaration: $crate::loader::ZFSourceDeclaration =
            $crate::loader::ZFSourceDeclaration {
                rustc_version: $crate::loader::RUSTC_VERSION,
                core_version: $crate::loader::CORE_VERSION,
                register: $register,
            };
    };
}

#[macro_export]
macro_rules! export_sink {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsink_declaration: $crate::loader::ZFSinkDeclaration =
            $crate::loader::ZFSinkDeclaration {
                rustc_version: $crate::loader::RUSTC_VERSION,
                core_version: $crate::loader::CORE_VERSION,
                register: $register,
            };
    };
}

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

#[macro_export]
macro_rules! zf_data {
    ($val : expr) => {
        zenoh_flow::async_std::sync::Arc::new(Box::new($val))
    };
}
