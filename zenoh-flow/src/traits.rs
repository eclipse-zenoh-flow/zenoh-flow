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

use crate::{Configuration, Input, Output, PortId, ZFResult};
use async_trait::async_trait;
use futures::Future;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

// /// This trait is used to ensure the data can donwcast to [`Any`](`Any`)
// /// NOTE: This trait is separate from `ZFDataTrait` so that we can provide
// /// a `#derive` macro to automatically implement it for the users.
// ///
// /// This can be derived using the `#derive(ZFData)`
// ///
// /// Example::
// /// ```no_run
// /// use zenoh_flow::zenoh_flow_derive::ZFData;
// /// #[derive(Debug, Clone, ZFData)]
// /// pub struct MyString(pub String);
// /// ```
// pub trait DowncastAny {
//     /// Donwcast as a reference to [`Any`](`Any`)
//     fn as_any(&self) -> &dyn Any;

//     /// Donwcast as a mutable reference to [`Any`](`Any`)
//     fn as_mut_any(&mut self) -> &mut dyn Any;
// }

// /// This trait abstracts the user's data type inside Zenoh Flow.
// ///
// /// User types should implement this trait otherwise Zenoh Flow will
// /// not be able to handle the data, and serialize them when needed.
// ///
// /// Example:
// /// ```no_run
// /// use zenoh_flow::zenoh_flow_derive::ZFData;
// /// use zenoh_flow::ZFData;
// ///
// /// #[derive(Debug, Clone, ZFData)]
// /// pub struct MyString(pub String);
// /// impl ZFData for MyString {
// ///     fn try_serialize(&self) -> zenoh_flow::ZFResult<Vec<u8>> {
// ///         Ok(self.0.as_bytes().to_vec())
// ///     }
// /// }
// /// ```
// pub trait ZFData: DowncastAny + Debug + Send + Sync {
//     /// Tries to serialize the data as `Vec<u8>`
//     ///
//     /// # Errors
//     /// If it fails to serialize an error variant will be returned.
//     fn try_serialize(&self) -> ZFResult<Vec<u8>>;
// }

// /// This trait abstract user's type deserialization.
// ///
// /// User types should implement this trait otherwise Zenoh Flow will
// /// not be able to handle the data, and deserialize them when needed.
// ///
// /// Example:
// /// ```no_run
// ///
// /// use zenoh_flow::{Deserializable, ZFResult, ZFError};
// /// use zenoh_flow::zenoh_flow_derive::ZFData;
// ///
// /// #[derive(Debug, Clone, ZFData)]
// /// pub struct MyString(pub String);
// ///
// /// impl Deserializable for MyString {
// ///     fn try_deserialize(bytes: &[u8]) -> ZFResult<MyString>
// ///     where
// ///         Self: Sized,
// ///     {
// ///         Ok(MyString(
// ///             String::from_utf8(bytes.to_vec()).map_err(|_| ZFError::DeseralizationError)?,
// ///         ))
// ///     }
// /// }
// /// ```
// pub trait Deserializable {
//     /// Tries to deserialize from a slice of `u8`.
//     ///
//     /// # Errors
//     /// If it fails to deserialize an error variant will be returned.
//     fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
//     where
//         Self: Sized;
// }

// /// This trait abstracts the user's state type inside Zenoh Flow.
// ///
// /// User types should implement this trait otherwise Zenoh Flow will
// /// not be able to handle the state.
// ///
// /// It can be easily derived.
// ///
// /// Example:
// ///
// /// ```no_run
// /// use zenoh_flow::zenoh_flow_derive::ZFState;
// /// #[derive(Debug, Clone, ZFState)]
// /// pub struct MyState;
// /// ```
// ///

// pub trait ZFState: Debug + Send + Sync {
//     /// Donwcast as reference to [`Any`](`Any`)
//     fn as_any(&self) -> &dyn Any;

//     /// Donwcast as mutable reference to [`Any`](`Any`)
//     fn as_mut_any(&mut self) -> &mut dyn Any;
// }

// /// The `Node` trait represents a generic node in the data flow graph.
// /// It contains functions that are common between Operator, Sink and Source.
// /// It has to be implemented for each node within a graph.
// pub trait Node {
//     /// This method is used to initialize the state of the node.
//     /// It is called by the Zenoh Flow runtime when initializing the data flow
//     /// graph.
//     /// An example of node state is files that should be opened, connection
//     /// to devices or internal configuration.
//     ///
//     /// # Errors
//     /// If it fails to initialize an error variant will be returned.
//     fn initialize(&self, configuration: &Option<Configuration>) -> ZFResult<State>;

//     /// This method is used to finalize the state of the node.
//     /// It is called by the Zenoh Flow runtime when tearing down the data
//     /// flow graph.
//     ///
//     /// An example of node state to finalize is files to be closed,
//     /// clean-up of libraries, or devices.
//     ///
//     /// # Errors
//     /// If it fails to finalize an error variant will be returned.
//     fn finalize(&self, state: &mut State) -> ZFResult<()>;
// }

// /// The `Operator` trait represents an Operator inside Zenoh Flow.
// pub trait Operator: Node + Send + Sync {
//     /// This method is called when data is received on one or more inputs.
//     /// The result of this method is use as discriminant to trigger the
//     /// operator's run function.
//     /// An operator can access its context and state during
//     /// the execution of this function.
//     ///
//     /// The received data is provided as [`InputToken`](`InputToken`) that
//     /// represents the state of the associated port.
//     /// Based on the tokens and on the data users can decide to trigger
//     /// the run or not.
//     /// The commodity function [`default_input_rule`](`default_input_rule`)
//     /// can be used if the operator should be triggered based on KPN rules.
//     ///
//     /// # Errors
//     /// If something goes wrong during execution an error
//     /// variant will be returned.
//     fn input_rule(
//         &self,
//         context: &mut Context,
//         state: &mut State,
//         tokens: &mut HashMap<PortId, InputToken>,
//     ) -> ZFResult<bool>;

//     /// This method is the actual one processing the data.
//     /// It is triggered based on the result of the `input_rule`.
//     /// As operators are computing over data,
//     /// *I/O should not be done in the run*.
//     ///
//     /// An operator can access its context and state
//     /// during the execution of this function.
//     /// The result of a computation can also not provide any output.
//     /// When it does provide output the `PortId` used should match the one
//     /// defined in the descriptor for the operator. Any not matching `PortId`
//     /// will be dropped.
//     ///
//     /// # Errors
//     /// If something goes wrong during execution an error
//     /// variant will be returned.
//     fn run(
//         &self,
//         context: &mut Context,
//         state: &mut State,
//         inputs: &mut HashMap<PortId, DataMessage>,
//     ) -> ZFResult<HashMap<PortId, Data>>;

//     /// This method is called after the run, its main purpose is to check
//     /// for missed local deadlines.
//     /// It can also be used for further analysis and
//     /// adjustment over the computed data.
//     /// E.g. flooring a value to a specified MAX, or check if it is within
//     /// a given range.
//     ///
//     /// An operator can access its context and
//     /// state during the execution of this function.
//     /// The commodity function [`default_output_rule`](`default_output_rule`)
//     /// can be used if the operator does not need any post-processing.
//     ///
//     /// # Errors
//     /// If something goes wrong during execution an error
//     /// variant will be returned.
//     fn output_rule(
//         &self,
//         context: &mut Context,
//         state: &mut State,
//         outputs: HashMap<PortId, Data>,
//     ) -> ZFResult<HashMap<PortId, NodeOutput>>;
// }

// /// The `Source` trait represents a Source inside Zenoh Flow
// #[async_trait]
// pub trait Source: Node + Send + Sync {
//     /// This method is the actual one producing the data.
//     /// It is triggered on a loop, and if the `period` is specified
//     /// in the descriptor it is triggered with the given period.
//     /// This method is `async` therefore I/O is possible, e.g. reading data
//     /// from a file/external device.
//     ///
//     /// The Source can access its state and context while executing.
//     ///
//     /// # Errors
//     /// If something goes wrong during execution an error
//     /// variant will be returned.
//     async fn run(&self, context: &mut Context, state: &mut State) -> ZFResult<Data>;
// }

// /// The `Sink` trait represents a Sink inside Zenoh Flow
// #[async_trait]
// pub trait Sink: Node + Send + Sync {
//     /// This method is the actual one consuming the data.
//     /// It is triggered whenever data arrives on the Sink input.
//     /// This method is `async` therefore I/O is possible, e.g. writing to
//     /// a file or interacting with an external device.
//     ///
//     /// The Sink can access its state and context while executing.
//     ///
//     /// # Errors
//     /// If something goes wrong during execution an error
//     /// variant will be returned.
//     async fn run(
//         &self,
//         context: &mut Context,
//         state: &mut State,
//         input: DataMessage,
//     ) -> ZFResult<()>;
// }

/// This trait is used to ensure the data can donwcast to [`Any`](`Any`)
/// NOTE: This trait is separate from `ZFDataTrait` so that we can provide
/// a `#derive` macro to automatically implement it for the users.
///
/// This can be derived using the `#derive(ZFData)`
///
/// Example::
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// ```
pub trait DowncastAny {
    /// Donwcast as a reference to [`Any`](`Any`)
    fn as_any(&self) -> &dyn Any;

    /// Donwcast as a mutable reference to [`Any`](`Any`)
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// This trait abstracts the user's data type inside Zenoh Flow.
///
/// User types should implement this trait otherwise Zenoh Flow will
/// not be able to handle the data, and serialize them when needed.
///
/// Example:
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFData;
/// use zenoh_flow::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// impl ZFData for MyString {
///     fn try_serialize(&self) -> zenoh_flow::ZFResult<Vec<u8>> {
///         Ok(self.0.as_bytes().to_vec())
///     }
/// }
/// ```
pub trait ZFData: DowncastAny + Debug + Send + Sync {
    /// Tries to serialize the data as `Vec<u8>`
    ///
    /// # Errors
    /// If it fails to serialize an error variant will be returned.
    fn try_serialize(&self) -> ZFResult<Vec<u8>>;
}

/// This trait abstract user's type deserialization.
///
/// User types should implement this trait otherwise Zenoh Flow will
/// not be able to handle the data, and deserialize them when needed.
///
/// Example:
/// ```no_run
///
/// use zenoh_flow::{Deserializable, ZFResult, ZFError};
/// use zenoh_flow::zenoh_flow_derive::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
///
/// impl Deserializable for MyString {
///     fn try_deserialize(bytes: &[u8]) -> ZFResult<MyString>
///     where
///         Self: Sized,
///     {
///         Ok(MyString(
///             String::from_utf8(bytes.to_vec()).map_err(|_| ZFError::DeseralizationError)?,
///         ))
///     }
/// }
/// ```
pub trait Deserializable {
    /// Tries to deserialize from a slice of `u8`.
    ///
    /// # Errors
    /// If it fails to deserialize an error variant will be returned.
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized;
}

/// This trait abstracts the user's state type inside Zenoh Flow.
///
/// User types should implement this trait otherwise Zenoh Flow will
/// not be able to handle the state.
///
/// It can be easily derived.
///
/// Example:
///
/// ```no_run
/// use zenoh_flow::zenoh_flow_derive::ZFState;
/// #[derive(Debug, Clone, ZFState)]
/// pub struct MyState;
/// ```
///
pub trait ZFState: Debug + Send + Sync {
    /// Donwcast as reference to [`Any`](`Any`)
    fn as_any(&self) -> &dyn Any;

    /// Donwcast as mutable reference to [`Any`](`Any`)
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// The `Node` trait represents a generic node in the data flow graph.
/// It contains functions that are common between Operator, Sink and Source.
/// It has to be implemented for each node within a graph.
#[async_trait]
pub trait Node {
    // /// This method is used to initialize the state of the node.
    // /// It is called by the Zenoh Flow runtime when initializing the data flow
    // /// graph.
    // /// An example of node state is files that should be opened, connection
    // /// to devices or internal configuration.
    // ///
    // /// # Errors
    // /// If it fails to initialize an error variant will be returned.
    // fn initialize(&self, configuration: &Option<Configuration>) -> ZFResult<State>;

    /// This method is used to finalize a node. It is called by the Zenoh Flow
    /// runtime when tearing down the data flow graph.
    ///
    /// An example of node state to finalize is files to be closed, clean-up of
    /// libraries, or devices.
    ///
    /// # Errors
    /// If it fails to finalize an error variant will be returned.
    async fn finalize(&self) -> ZFResult<()>;
}

/// The `Source` trait represents a Source inside Zenoh Flow.
#[async_trait]
pub trait Source: Node + Send + Sync {
    async fn setup(
        &self,
        configuration: &Option<Configuration>,
        outputs: HashMap<PortId, Output>,
    ) -> ZFResult<Arc<dyn AsyncIteration>>;
}

/// The `Operator` trait represents an Operator inside Zenoh Flow.
#[async_trait]
pub trait Operator: Node + Send + Sync {
    async fn setup(
        &self,
        configuration: &Option<Configuration>,
        inputs: HashMap<PortId, Input>,
        outputs: HashMap<PortId, Output>,
    ) -> ZFResult<Arc<dyn AsyncIteration>>;
}

/// The `Sink` trait represents a Sink inside Zenoh Flow.
#[async_trait]
pub trait Sink: Node + Send + Sync {
    async fn setup(
        &self,
        configuration: &Option<Configuration>,
        inputs: HashMap<PortId, Input>,
    ) -> ZFResult<Arc<dyn AsyncIteration>>;
}

/// A `SourceSink` represents Nodes that access the same physical interface to
/// read and write.
#[async_trait]
pub trait SourceSink: Node + Send + Sync {
    async fn setup(
        &self,
        configuration: &Option<Configuration>,
        inputs: HashMap<PortId, Input>,
        outputs: HashMap<PortId, Output>,
    ) -> ZFResult<Arc<dyn AsyncIteration>>;
}

/// Trait wrapping an async closures for node iteration, it requires rust-nightly because of
/// https://github.com/rust-lang/rust/issues/62290
///
/// * Note: * not intended to be directly used by users.
pub trait AsyncIteration: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackTx for any async closure that returns
/// `ZFResult<()>`.
/// This "converts" any `async move { ... }` to `AsyncCallbackTx`
///
/// *Note:* It takes an `FnOnce` because of the `move` keyword. The closure
/// has to be `Clone` as we are going to call the closure more than once.
impl<Fut, Fun> AsyncIteration for Fun
where
    Fun: FnOnce() -> Fut + Sync + Send + Clone,
    Fut: Future<Output = ZFResult<()>> + Send + Sync + 'static,
{
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>> {
        Box::pin(self.clone()())
    }
}
