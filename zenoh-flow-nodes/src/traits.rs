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

use crate::context::Context;
use crate::io::{Inputs, Outputs};
use async_trait::async_trait;
use std::any::Any;
use zenoh_flow_commons::{Configuration, Result};

/// The `SendSyncAny` trait allows Zenoh-Flow to send data between nodes running in the same process without
/// serialising.
///
/// This trait is implemented for any type that has the `static` lifetime and implements [Send] and [Sync]. These
/// constraints are the same than for the typed [Input](crate::prelude::Input) and typed
/// [Output](crate::prelude::Output) which means that there should be no need to manually implement it.
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

/// The `Node` trait, shared among all node types, dictates how a node *runs*.
///
/// # `Iteration`
///
/// The central method, for which there is no default implementation, is [iteration](Node::iteration()). This method
/// is called, in a loop, by the Zenoh-Flow runtime managing the node.
///
/// Method in this trait takes an immutable reference to `self` so as to not impact performance. To keep a state and to
/// mutate it, the [interior mutability](https://doc.rust-lang.org/reference/interior-mutability.html) pattern is
/// necessary.
///
/// For usage examples see the [Operator](crate::prelude::Operator), [Source](crate::prelude::Source) or
/// [Sink](crate::prelude::Sink) traits.
///
/// # Additional hooks: `on_resume`, `on_abort`
///
/// It is possible to define specific code that the Zenoh-Flow runtime should run *before* the node is aborted and
/// *before* it is resumed.
///
/// Note that the `on_resume` hook is only run once the node has been aborted. It is not run when it is created.
///
/// A default blank implementation is provided.
#[async_trait]
pub trait Node: Send + Sync {
    /// The code a Zenoh-Flow runtime will execute in a loop.
    ///
    /// A typical workflow would be to wait for all or a subset of the [Input(s)](crate::prelude::Input) to be ready,
    /// perform some computation and finally forward the result(s) downstream on the
    /// [Output(s)](crate::prelude::Output).
    async fn iteration(&self) -> Result<()>;

    /// Custom code that Zenoh-Flow will run *before* re-starting a node that was previously aborted.
    ///
    /// The code to correctly manage the state of a node should go there. This hook is for instance leveraged within
    /// the implementation of the Zenoh Source built-in node.
    ///
    /// The blanket implementation defaults to returning `Ok(())`.
    ///
    /// # Performance
    ///
    /// This method is only called when the node is restarted. Hence, its impact is limited and does not affect the
    /// normal execution of a node.
    async fn on_resume(&self) -> Result<()> {
        Ok(())
    }

    async fn on_abort(&self) {}
}

/// A `Source` feeds data into a data flow.
///
/// A `Source` only possesses `Output` (either [typed](crate::prelude::Output) or [raw](crate::prelude::OutputRaw)) as
/// it does not receive any data from upstream nodes but from "outside" the data flow.
///
/// A structure implementing the `Source` trait typically needs to keep a reference to the `Output`.
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
///         _configuration: Configuration,
///         mut outputs: Outputs,
///     ) -> Result<Self> {
///         let output = outputs
///             .take("out")
///             .expect("No output called 'out' found")
///             .typed(|buffer, data| todo!("Provide your serialiser here"));
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
///         // The state is a way for the Source to read information from the external world, i.e., interacting with
///         // I/O devices.
///
///         self.output.send(10usize, None).await
///     }
/// }
/// ```
#[async_trait]
pub trait Source: Node + Send + Sync {
    /// For a [Context], a [Configuration] and a set of [Outputs], produce a new [Source].
    ///
    /// Sources only possess `Outputs` as their purpose is to fetch data from the external world.
    async fn new(context: Context, configuration: Configuration, outputs: Outputs) -> Result<Self>
    where
        Self: Sized;
}

/// An `Operator` is a node performing transformation over the data it receives, outputting the end result to downstream
/// node(s).
///
/// An `Operator` possesses both `Input` (either [typed](crate::prelude::Input) or [raw](crate::prelude::InputRaw)) and
/// `Output` (either [typed](crate::prelude::Output) or [raw](crate::prelude::OutputRaw)).
///
/// A structure implementing the `Operator` trait typically needs to keep a reference to its `Input`(s) and `Output`(s).
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
///         _configuration: Configuration,
///         mut inputs: Inputs,
///         mut outputs: Outputs,
///     ) -> Result<Self> {
///         Ok(NoOp {
///             input: inputs
///                 .take("in")
///                 .expect("No input called 'in' found")
///                 .typed(|bytes| todo!("Provide your deserialiser here")),
///             output: outputs
///                 .take("out")
///                 .expect("No output called 'out' found")
///                 .typed(|buffer, data| todo!("Provide your serialiser here")),
///         })
///     }
/// }
///
/// #[async_trait]
/// impl Node for NoOp {
///     async fn iteration(&self) -> Result<()> {
///         let (message, _timestamp) = self.input.recv().await?;
///         self.output.send(*message, None).await
///     }
/// }
/// ```
#[async_trait]
pub trait Operator: Node + Send + Sync {
    /// For a [Context], a [Configuration], a set of [Inputs] and [Outputs], produce a new [Operator].
    ///
    /// Operators are at the heart of a data flow, they carry out computations on the data they
    /// receive before sending them out to the next downstream node.
    async fn new(
        context: Context,
        configuration: Configuration,
        inputs: Inputs,
        outputs: Outputs,
    ) -> Result<Self>
    where
        Self: Sized;
}

/// A `Sink` exposes the outcome of the data flow processing.
///
/// A `Sink` only possesses `Input` (either [typed](crate::prelude::Input) or [raw](crate::prelude::InputRaw)) as its
/// purpose is to communicate with entities outside of the data flow.
///
/// A structure implementing the `Sink` trait typically needs to keep a reference to its `Input`(s).
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
///         _configuration: Configuration,
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
///         println!("{}", *message);
///
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Sink: Node + Send + Sync {
    /// For a [Context], a [Configuration] and [Inputs], produce a new [Sink].
    ///
    /// Sinks only possess `Inputs`, their objective is to send the result of the computations to the external world.
    async fn new(context: Context, configuration: Configuration, inputs: Inputs) -> Result<Self>
    where
        Self: Sized;
}
