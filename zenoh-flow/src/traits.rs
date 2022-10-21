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

use crate::prelude::{Data, Inputs, Message, Outputs};
use crate::types::{Configuration, Context};
use crate::Result as ZFResult;
use async_trait::async_trait;
use futures::Future;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

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
/// use zenoh_flow::prelude::{ZFData, Result};
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
/// impl ZFData for MyString {
///     fn try_serialize(&self) -> Result<Vec<u8>> {
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
/// use zenoh_flow::prelude::*;
/// use zenoh_flow::zenoh_flow_derive::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct MyString(pub String);
///
/// impl Deserializable for MyString {
///     fn try_deserialize(bytes: &[u8]) -> Result<MyString>
///     where
///         Self: Sized,
///     {
///         Ok(MyString(
///             String::from_utf8(bytes.to_vec()).map_err(|e| zferror!(ErrorKind::DeseralizationError, e))?,
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

/// A `Node` is defined by its `iteration` that is repeatedly called by Zenoh-Flow.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Node trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///
/// # Example
///
/// ```no_run
/// extern crate async_trait;
///
/// use zenoh_flow::prelude::*;
///
/// pub struct MyNode {
///   input: Input,    // A Source would have no input
///   output: Output,  // A Sink would have no output
///   // The state could go in such structure.
///   // state: Arc<Mutex<T>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Node for MyNode {
///   async fn iteration(&self) -> Result<()> {
///     // To mutate the state, first lock it.
///     // let state = self.state.lock().await;
///     
///     if let Ok(Message::Data(mut message)) = self.input.recv_async().await {
///       let data = message.get_inner_data();
///       self.output.send_async(data.clone(), None).await?;
///     }
///     Ok(())
///   }
/// }
/// ```
#[async_trait]
pub trait Node: Send + Sync {
    async fn iteration(&self) -> ZFResult<()>;
}

/// For a `Context`, a `Configuration` and a set of `Outputs`, produce a new *Source*.
///
/// Sources only possess `Outputs` and their purpose is to fetch data from the external world.
///
/// Sources are **started last** when initiating a data flow. This is to prevent data loss: if a
/// Source is started before its downstream nodes then the data it would send before said downstream
/// nodes are up would be lost.
///
/// # Example
///
/// ```no_run
/// extern crate async_trait;
/// extern crate async_std;
///
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use zenoh_flow::prelude::*;
///
/// pub struct MySource {
///   output: Output,
///   
///   // If we read from a sensor we can keep a reference here.
///   // If we need to mutate it, it could go behind an `Arc<Mutex<T>>`.
///   // sensor: Sensor,
/// }
///
/// #[async_trait::async_trait]
/// impl Node for MySource {
///   async fn iteration(&self) -> Result<()> {
///     async_std::task::sleep(Duration::from_secs(1)).await;
///     
///     // We can read data from a sensor every second and send it.
///     // let data = self.sensor.read().await;
///     // self.output.send_async(data, None).await;
///
///     Ok(())
///   }
/// }
///
/// pub struct MySourceFactory;
///
/// #[async_trait::async_trait]
/// impl SourceFactoryTrait for MySourceFactory {
///   async fn new_source(
///     &self,
///     context: &mut Context,
///     configuration: &Option<Configuration>,
///     mut outputs: Outputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///      if let Some(configuration) = configuration {
///        // We can read the configuration here.
///        let sensor_file = configuration["sensor"].as_str().expect("No sensor in configuration");
///      }
///
///      // If we want to transform an Output in a callback, then we can leverage the Context.
///      // context.register_output_callback(output, Arc::new(move || { … }));
///
///      let output = outputs.take("out").expect("No output named 'out'");
///
///      Ok(Some(Arc::new(MySource { output })))
///   }
/// }
/// ```
#[async_trait]
pub trait SourceFactoryTrait: Send + Sync {
    async fn new_source(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        mut outputs: Outputs,
    ) -> ZFResult<Option<Arc<dyn Node>>>;
}

/// For a `Context`, a `Configuration`, a set of `Inputs` and `Outputs`, produce a new **Operator**.
///
/// Operators are at the heart of a data flow, they carry out computations on the data they receive
/// before sending them out to the next downstream node.
///
/// The Operators are started *before the Sources* such that they are active before the first data
/// are produced.
///
/// # Example
///
/// ```no_run
/// extern crate async_trait;
/// extern crate async_std;
///
/// use std::sync::Arc;
///
/// use zenoh_flow::prelude::*;
///
/// pub struct MyOperator {
///   input: Input,
///   output: Output,
///   // state: Arc<Mutex<T>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Node for MyOperator {
///   async fn iteration(&self) -> Result<()> {
///     // let state = self.state.lock().await;
///     
///     if let Ok(Message::Data(mut message)) = self.input.recv_async().await {
///       let mut data = message.get_inner_data().clone();
///       // Computation based on the data would be performed here. For instance:
///       // data += 1;
///       self.output.send_async(data, None).await?;
///     }
///     Ok(())
///   }
/// }
///
/// pub struct MyOperatorFactory;
///
/// #[async_trait::async_trait]
/// impl OperatorFactoryTrait for MyOperatorFactory {
///   async fn new_operator(
///     &self,
///     context: &mut Context,
///     configuration: &Option<Configuration>,
///     mut inputs: Inputs,
///     mut outputs: Outputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///      if let Some(configuration) = configuration {
///        // We can read the configuration here and set some default values.
///        // These values could be used to populate the state.
///      }
///
///      // If we want to transform an Output or an Input in a callback, then we can leverage the
///      // Context.
///      //
///      // context.register_input_callbac(input, Arc::new(move |message| { … }));
///      // context.register_output_callback(output, Arc::new(move || { … }));
///
///      let input = inputs.take("in").expect("No input named 'in'");
///      let output = outputs.take("out").expect("No output named 'out'");
///
///      Ok(Some(Arc::new(MyOperator { input, output })))
///   }
/// }
///
/// ```
#[async_trait]
pub trait OperatorFactoryTrait: Send + Sync {
    async fn new_operator(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> ZFResult<Option<Arc<dyn Node>>>;
}

/// For a `Context`, a `Configuration` and a set of `Inputs`, produce a new **Sink**.
///
/// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
/// external world.
///
/// Sinks are **started first** when initiating a data flow. As they are at the end of the chain of
/// computations, by starting them first we ensure that no data is lost.
///
/// # Example
///
/// ```no_run
/// extern crate async_trait;
/// extern crate async_std;
///
/// use std::sync::Arc;
///
/// use zenoh_flow::prelude::*;
///
/// pub struct MySink {
///   input: Input,
///   // state: Arc<Mutex<T>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Node for MySink {
///   async fn iteration(&self) -> Result<()> {
///     if let Ok(Message::Data(mut message)) = self.input.recv_async().await {
///       let mut data = message.get_inner_data().clone();
///       // Do something with that data, for instance write it to a file…
///       println!("Data: {:?}", data);
///     }
///     Ok(())
///   }
/// }
///
/// pub struct MySinkFactory;
///
/// #[async_trait::async_trait]
/// impl SinkFactoryTrait for MySinkFactory {
///   async fn new_sink(
///     &self,
///     context: &mut Context,
///     configuration: &Option<Configuration>,
///     mut inputs: Inputs,
///   ) -> Result<Option<Arc<dyn Node>>> {
///      if let Some(configuration) = configuration {
///        // We can read the configuration here.
///      }
///
///      // If we want to transform an Input into a callback, then we can leverage the Context.
///      // context.register_input_callback(output, Arc::new(move |message| { … }));
///
///      let input = inputs.take("in").expect("No input named 'in'");
///
///      Ok(Some(Arc::new(MySink { input })))
///   }
/// }
/// ```
#[async_trait]
pub trait SinkFactoryTrait: Send + Sync {
    async fn new_sink(
        &self,
        context: &mut Context,
        configuration: &Option<Configuration>,
        mut inputs: Inputs,
    ) -> ZFResult<Option<Arc<dyn Node>>>;
}

/// Trait wrapping an async closures for sender callback, it requires rust-nightly because of
/// https://github.com/rust-lang/rust/issues/62290
///
/// * Note: * not intended to be directly used by users.
type AsyncCallbackOutput = ZFResult<(Data, Option<u64>)>;

pub trait AsyncCallbackTx: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = AsyncCallbackOutput> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackTx for any async closure that returns
/// `ZFResult<()>`.
/// This "converts" any `async move { ... }` to `AsyncCallbackTx`
///
/// *Note:* It takes an `FnOnce` because of the `move` keyword. The closure
/// has to be `Clone` as we are going to call the closure more than once.
impl<Fut, Fun> AsyncCallbackTx for Fun
where
    Fun: Fn() -> Fut + Sync + Send,
    Fut: Future<Output = ZFResult<(Data, Option<u64>)>> + Send + Sync + 'static,
{
    fn call(
        &self,
    ) -> Pin<Box<dyn Future<Output = ZFResult<(Data, Option<u64>)>> + Send + Sync + 'static>> {
        Box::pin((self)())
    }
}

/// Trait describing the functions we are expecting to call upon receiving a message.
///
/// NOTE: Users are encouraged to provide a closure instead of implementing this trait.
pub trait InputCallback: Send + Sync {
    fn call(
        &self,
        arg: Message,
    ) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>>;
}

/// Implementation of InputCallback for any async closure that takes `Message` as parameter and
/// returns `ZFResult<()>`. This "converts" any `async move |msg| { ... Ok() }` into an
/// `InputCallback`.
///
/// This allows users to provide a closure instead of implementing the trait.
impl<Fut, Fun> InputCallback for Fun
where
    Fun: Fn(Message) -> Fut + Sync + Send,
    Fut: Future<Output = ZFResult<()>> + Send + Sync + 'static,
{
    fn call(
        &self,
        message: Message,
    ) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>> {
        Box::pin((self)(message))
    }
}

/// TODO Documentation: Output callback expects nothing (except for a trigger) and returns some
/// Data. As it’s a callback, Zenoh-Flow will take care of sending it.
pub trait OutputCallback: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<Data>> + Send + Sync + 'static>>;
}

/// TODO Documentation: implementation of OutputCallback for closures, it makes it easier to write
/// these functions.
impl<Fut, Fun> OutputCallback for Fun
where
    Fun: Fn() -> Fut + Sync + Send,
    Fut: Future<Output = ZFResult<Data>> + Send + Sync + 'static,
{
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<Data>> + Send + Sync + 'static>> {
        Box::pin((self)())
    }
}
