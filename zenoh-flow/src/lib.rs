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

pub use ::zenoh_flow_derive;

pub use ::async_std;
pub use ::bincode;
pub use ::paste;
pub use ::serde;
pub use ::typetag;

pub mod model;
pub mod operator;
pub mod runtime;
pub mod types;
pub use types::*;

pub mod macros;
pub use macros::*;

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
