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

#[macro_export]
macro_rules! export_operator {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfoperator_declaration: $crate::runtime::runners::operator::OperatorDeclaration =
            $crate::runtime::runners::operator::OperatorDeclaration {
                rustc_version: $crate::runtime::loader::RUSTC_VERSION,
                core_version: $crate::runtime::loader::CORE_VERSION,
                register: $register,
            };
    };
}

#[macro_export]
macro_rules! export_source {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsource_declaration: $crate::runtime::runners::source::SourceDeclaration =
            $crate::runtime::runners::source::SourceDeclaration {
                rustc_version: $crate::runtime::loader::RUSTC_VERSION,
                core_version: $crate::runtime::loader::CORE_VERSION,
                register: $register,
            };
    };
}

#[macro_export]
macro_rules! export_sink {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfsink_declaration: $crate::runtime::runners::sink::SinkDeclaration =
            $crate::runtime::runners::sink::SinkDeclaration {
                rustc_version: $crate::runtime::loader::RUSTC_VERSION,
                core_version: $crate::runtime::loader::CORE_VERSION,
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
macro_rules! downcast {
    ($ident : ident, $val : expr) => {
        $val.as_any().downcast_ref::<$ident>()
    };
}

#[macro_export]
macro_rules! downcast_mut {
    ($ident : ident, $val : expr) => {
        $val.as_mut_any().downcast_mut::<$ident>()
    };
}

#[macro_export]
macro_rules! take_state {
    ($ident : ident, $ctx : expr) => {
        match $ctx.take_state() {
            Some(mut state) => match zenoh_flow::downcast_mut!($ident, state) {
                Some(mut data) => Ok((state, data)),
                None => Err(zenoh_flow::types::ZFError::InvalidState),
            },
            None => Err(zenoh_flow::types::ZFError::MissingState),
        }
    };
}

#[macro_export]
macro_rules! get_state {
    ($ident : ident, $ctx : expr) => {
        match $ctx.get_state() {
            Some(state) => match zenoh_flow::downcast!($ident, state) {
                Some(data) => Ok((state, data)),
                None => Err(zenoh_flow::types::ZFError::InvalidState),
            },
            None => Err(zenoh_flow::types::ZFError::MissingState),
        }
    };
}

#[macro_export]
macro_rules! zf_empty_state {
    () => {
        Box::new(zenoh_flow::EmptyState {})
    };
}

#[macro_export]
macro_rules! zf_source_result {
    ($result : expr) => {
        Box::pin($result)
    };
}
