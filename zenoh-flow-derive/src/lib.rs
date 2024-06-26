//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

//! This crate exposes three procedural macros (one for each type of node) to facilitate exposing the symbols required
//! by Zenoh-Flow in order to dynamically load nodes.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Expose the symbols Zenoh-Flow needs to instantiate and start a Source.
///
/// In addition to exposing a specific symbol that will not be mangled by the compiler, this macro records the version
/// of the rust compiler used as well as the version of Zenoh-Flow. These additional information are here to (try) limit
/// possible surprises due to the lack of stable ABI in Rust.
///
/// ## Example
///
/// ```
/// # use async_trait::async_trait;
/// # use zenoh_flow_nodes::prelude::*;
/// #[export_source]
/// pub struct MySource {
///     // Your logic goes here.
/// }
/// # #[async_trait]
/// # impl Source for MySource{
/// #   async fn new(
/// #       context: Context,
/// #       configuration: Configuration,
/// #       outputs: Outputs,
/// #   ) -> Result<Self> {
/// #         todo!()
/// #     }
/// # }
///
/// # #[async_trait]
/// # impl Node for MySource {
/// #     async fn iteration(&self) -> Result<()> {
/// #         todo!()
/// #     }
/// # }
/// ```
#[proc_macro_attribute]
pub fn export_source(_: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let gen = quote! {

        #ast

        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_source: zenoh_flow_nodes::NodeDeclaration<
            zenoh_flow_nodes::SourceFn,
        > = zenoh_flow_nodes::NodeDeclaration::<
            zenoh_flow_nodes::SourceFn,
        > {
            rustc_version: zenoh_flow_nodes::RUSTC_VERSION,
            core_version: zenoh_flow_nodes::CORE_VERSION,
            constructor: |context: zenoh_flow_nodes::prelude::Context,
            configuration: zenoh_flow_nodes::prelude::Configuration,
            outputs: zenoh_flow_nodes::prelude::Outputs| {
                std::boxed::Box::pin(async {
                    let node = <#ident>::new(context, configuration, outputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn zenoh_flow_nodes::prelude::Node>)
                })
            },
        };
    };
    gen.into()
}

/// Expose the symbols Zenoh-Flow needs to instantiate and start a Sink.
///
/// In addition to exposing a specific symbol that will not be mangled by the compiler, this macro records the version
/// of the rust compiler used as well as the version of Zenoh-Flow. These additional information are here to (try) limit
/// possible surprises due to the lack of stable ABI in Rust.
///
/// ## Example
///
/// ```
/// # use async_trait::async_trait;
/// # use zenoh_flow_nodes::prelude::*;
/// #[export_sink]
/// pub struct MySink {
///     // Your logic goes here.
/// }
/// # #[async_trait]
/// # impl Sink for MySink {
/// #   async fn new(
/// #       context: Context,
/// #       configuration: Configuration,
/// #       inputs: Inputs,
/// #   ) -> Result<Self> {
/// #         todo!()
/// #     }
/// # }
///
/// # #[async_trait]
/// # impl Node for MySink {
/// #     async fn iteration(&self) -> Result<()> {
/// #         todo!()
/// #     }
/// # }
/// ```
#[proc_macro_attribute]
pub fn export_sink(_: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let sink = quote! {#ast};

    let constructor = quote! {

        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_sink: zenoh_flow_nodes::NodeDeclaration<
            zenoh_flow_nodes::SinkFn,
        > = zenoh_flow_nodes::NodeDeclaration::<
            zenoh_flow_nodes::SinkFn,
        > {
            rustc_version: zenoh_flow_nodes::RUSTC_VERSION,
            core_version: zenoh_flow_nodes::CORE_VERSION,
            constructor: |context: zenoh_flow_nodes::prelude::Context,
                          configuration: zenoh_flow_nodes::prelude::Configuration,
                          mut inputs: zenoh_flow_nodes::prelude::Inputs| {
                std::boxed::Box::pin(async {
                    let node = <#ident>::new(context, configuration, inputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn zenoh_flow_nodes::prelude::Node>)
                })
            },
        };
    };

    let gen = quote! {
        #sink
        #constructor

    };
    gen.into()
}

/// Expose the symbols Zenoh-Flow needs to instantiate and start a Operator.
///
/// In addition to exposing a specific symbol that will not be mangled by the compiler, this macro records the version
/// of the rust compiler used as well as the version of Zenoh-Flow. These additional information are here to (try) limit
/// possible surprises due to the lack of stable ABI in Rust.
///
/// ## Example
///
/// ```
/// # use async_trait::async_trait;
/// # use zenoh_flow_nodes::prelude::*;
/// #[export_operator]
/// pub struct MyOperator {
///     // Your logic code goes here.
/// }
/// # #[async_trait]
/// # impl Operator for MyOperator {
/// #     async fn new(
/// #         context: Context,
/// #         configuration: Configuration,
/// #         inputs: Inputs,
/// #         outputs: Outputs,
/// #     ) -> Result<Self>
/// #     where
/// #     Self: Sized {
/// #         todo!()
/// #     }
/// # }
///
/// # #[async_trait]
/// # impl Node for MyOperator {
/// #     async fn iteration(&self) -> Result<()> {
/// #         todo!()
/// #     }
/// # }
/// ```
#[proc_macro_attribute]
pub fn export_operator(_: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let gen = quote! {

        #ast

        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_operator: zenoh_flow_nodes::NodeDeclaration<
        zenoh_flow_nodes::OperatorFn,
        > = zenoh_flow_nodes::NodeDeclaration::<
        zenoh_flow_nodes::OperatorFn,
        > {
            rustc_version: zenoh_flow_nodes::RUSTC_VERSION,
            core_version: zenoh_flow_nodes::CORE_VERSION,
            constructor: |context: zenoh_flow_nodes::prelude::Context,
                          configuration: zenoh_flow_nodes::prelude::Configuration,
                          mut inputs: zenoh_flow_nodes::prelude::Inputs,
                          mut outputs: zenoh_flow_nodes::prelude::Outputs| {
                std::boxed::Box::pin(async {
                    let node = <#ident>::new(context, configuration, inputs, outputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn zenoh_flow_nodes::prelude::Node>)
                })
            },
        };
    };
    gen.into()
}
