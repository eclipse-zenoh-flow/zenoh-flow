//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

use crate::io::{Inputs, Outputs};
use crate::Context;
use async_trait::async_trait;
use std::any::Any;
use zenoh_flow_commons::{Configuration, Result};

/// The `SendSyncAny` trait allows Zenoh-Flow to send data between nodes running in the same process
/// without serializing.
///
/// This trait is implemented for any type that has the `static` lifetime and implements `Send` and
/// `Sync`. These constraints are the same than for the typed `Input` and `Output` which means that
/// there is absolutely no need to manually implement it.
pub trait SendSyncAny: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn as_mut_any(&mut self) -> &mut dyn Any;
}

impl<T: 'static + Send + Sync> SendSyncAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

/// A `Node` is defined by its `iteration` that is repeatedly called by Zenoh-Flow.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Node trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///
/// For usage examples see: [`Operator`](`Operator`), [`Source`](`Source`) or [`Sink`](`Sink`)
/// traits.
#[async_trait]
pub trait Node: Send + Sync {
    async fn iteration(&self) -> Result<()>;
}

/// The `Source` trait represents a Source of data in Zenoh Flow. Sources only possess `Outputs` and
/// their purpose is to fetch data from the external world.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Source trait typically needs to keep a reference to the `Output` it
/// needs.
///
/// ## Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow_nodes::prelude::*;
///
/// // Use our provided macro to expose the symbol that Zenoh-Flow will look for when it will load
/// // the shared library.
/// #[export_source]
/// pub struct MySource {
///     output: Output<usize>,
///     // The state could go in such structure.
///     // state: Arc<Mutex<State>>,
/// }
///
/// #[async_trait::async_trait]
/// impl Source for MySource {
///     async fn new(
///         _context: Context,
///         _configuration: Option<Configuration>,
///         mut outputs: Outputs,
///     ) -> Result<Self> {
///         let output = outputs
///             .take("out")
///             .expect("No output called 'out' found")
///             .typed(|buffer, data| todo!("Provide your serializer here"));
///
///         Ok(Self { output })
///     }
/// }
///
/// #[async_trait::async_trait]
/// impl Node for MySource {
///     async fn iteration(&self) -> Result<()> {
///         // To mutate the state, first lock it.
///         //
///         // let state = self.state.lock().await;
///         //
///         // The state is a way for the Source to read information from the external world, i.e.,
///         // interacting with I/O devices. We mimick an asynchronous iteraction with a sleep.
///
///         self.output.send(10usize, None).await
///     }
/// }
/// ```
#[async_trait]
pub trait Source: Node + Send + Sync {
    /// For a `Context`, a `Configuration` and a set of `Outputs`, produce a new *Source*.
    ///
    /// Sources only possess `Outputs` and their purpose is to fetch data from the external world.
    ///
    /// Sources are **started last** when initiating a data flow. This is to prevent data loss: if a
    /// Source is started before its downstream nodes then the data it would send before said
    /// downstream nodes are up would be lost.
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        outputs: Outputs,
    ) -> Result<Self>
    where
        Self: Sized;
}

/// The `Operator` trait represents an Operator inside Zenoh-Flow.
///
/// Operators are at the heart of a data flow, they carry out computations on the data they receive
/// before sending them out to the next downstream node.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Operator trait typically needs to keep a reference to the `Input` and
/// `Output` it needs.
///
/// ## Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow_nodes::prelude::*;
///
/// // Use our provided macro to expose the symbol that Zenoh-Flow will look for when it will load
/// // the shared library.
/// #[export_operator]
/// struct NoOp {
///     input: Input<usize>,
///     output: Output<usize>,
/// }
///
/// #[async_trait]
/// impl Operator for NoOp {
///     async fn new(
///         _context: Context,
///         _configuration: Option<Configuration>,
///         mut inputs: Inputs,
///         mut outputs: Outputs,
///     ) -> Result<Self> {
///         Ok(NoOp {
///             input: inputs
///                 .take("in")
///                 .expect("No input called 'in' found")
///                 .typed(|bytes| todo!("Provide your deserializer here")),
///             output: outputs
///                 .take("out")
///                 .expect("No output called 'out' found")
///                 .typed(|buffer, data| todo!("Provide your serializer here")),
///         })
///     }
/// }
/// #[async_trait]
/// impl Node for NoOp {
///     async fn iteration(&self) -> Result<()> {
///         let (message, _timestamp) = self.input.recv().await?;
///         match message {
///             Message::Data(t) => self.output.send(*t, None).await?,
///             Message::Watermark => println!("Watermark"),
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Operator: Node + Send + Sync {
    /// For a `Context`, a `Configuration`, a set of `Inputs` and `Outputs`, produce a new
    /// **Operator**.
    ///
    /// Operators are at the heart of a data flow, they carry out computations on the data they
    /// receive before sending them out to the next downstream node.
    ///
    /// The Operators are started *before the Sources* such that they are active before the first
    /// data are produced.
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Result<Self>
    where
        Self: Sized;
}

/// The `Sink` trait represents a Sink of data in Zenoh Flow.
///
/// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
/// external world.
///
/// This trait takes an immutable reference to `self` so as to not impact performance. To keep a
/// state and to mutate it, the interior mutability pattern is necessary.
///
/// A struct implementing the Sink trait typically needs to keep a reference to the `Input` it
/// needs.
///
/// ## Example
///
/// ```no_run
/// use async_trait::async_trait;
/// use zenoh_flow_nodes::prelude::*;
///
/// // Use our provided macro to expose the symbol that Zenoh-Flow will look for when it will load
/// // the shared library.
/// #[export_sink]
/// struct GenericSink {
///     input: Input<usize>,
/// }
///
/// #[async_trait]
/// impl Sink for GenericSink {
///     async fn new(
///         _context: Context,
///         _configuration: Option<Configuration>,
///         mut inputs: Inputs,
///     ) -> Result<Self> {
///         let input = inputs
///             .take("in")
///             .expect("No input called 'in' found")
///             .typed(|bytes| todo!("Provide your deserializer here"));
///
///         Ok(GenericSink { input })
///     }
/// }
///
/// #[async_trait]
/// impl Node for GenericSink {
///     async fn iteration(&self) -> Result<()> {
///         let (message, _timestamp) = self.input.recv().await?;
///         match message {
///             Message::Data(t) => println!("{}", *t),
///             Message::Watermark => println!("Watermark"),
///         }
///
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Sink: Node + Send + Sync {
    /// For a `Context`, a `Configuration` and a set of `Inputs`, produce a new **Sink**.
    ///
    /// Sinks only possess `Inputs`, their objective is to send the result of the computations to the
    /// external world.
    ///
    /// Sinks are **started first** when initiating a data flow. As they are at the end of the chain of
    /// computations, by starting them first we ensure that no data is lost.
    async fn new(
        context: Context,
        configuration: Option<Configuration>,
        inputs: Inputs,
    ) -> Result<Self>
    where
        Self: Sized;
}
