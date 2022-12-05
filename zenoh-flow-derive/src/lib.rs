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

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// The `ZFData` derive macro is provided to help the users
/// in implementing the `DowncastAny` trait.
///
/// ## Example
///
/// ```no_compile
/// use zenoh_flow_derive::ZFData;
///
/// #[derive(Debug, Clone, ZFData)]
/// pub struct ZFString(pub String);
/// ```
#[proc_macro_derive(ZFData)]
pub fn zf_data_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;
    let gen = quote! {

        impl zenoh_flow::prelude::DowncastAny for #ident {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
    gen.into()
}

/// The `export_source` attribute macro is provided to allow the users
/// in exporting their source.
///
/// ## Example
///
/// ```no_compile
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// #[export_source]
/// pub struct MySource;
///
/// #[async_trait]
/// impl Source for MySource{
///   async fn new(
///       context: Context,
///       configuration: Option<Configuration>,
///       outputs: Outputs,
///   ) -> Result<Self> {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Node for MySource {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// ```
#[proc_macro_attribute]
pub fn export_source(_: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let gen = quote! {

        #ast

        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_source: zenoh_flow::runtime::dataflow::loader::NodeDeclaration<
        zenoh_flow::runtime::dataflow::node::SourceFn,
        > = zenoh_flow::runtime::dataflow::loader::NodeDeclaration::<
        zenoh_flow::runtime::dataflow::node::SourceFn,
        > {
            rustc_version: zenoh_flow::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: zenoh_flow::runtime::dataflow::loader::CORE_VERSION,
            constructor: |context: zenoh_flow::types::Context,
                          configuration: Option<zenoh_flow::types::Configuration>,
                          outputs: zenoh_flow::types::Outputs| {
                std::boxed::Box::pin(async {
                    let node = <#ident>::new(context, configuration, outputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn zenoh_flow::traits::Node>)
                })
            },
        };
    };
    gen.into()
}

/// The `export_sink` attribute macro is provided to allow the users
/// in exporting their sink.
///
/// ## Example
///
/// ```no_compile
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// #[export_sink]
/// pub struct MySink;
///
/// #[async_trait]
/// impl Sink for MySink {
///   async fn new(
///       context: Context,
///       configuration: Option<Configuration>,
///       inputs: Inputs,
///   ) -> Result<Self> {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Node for MySink {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// ```
#[proc_macro_attribute]
pub fn export_sink(_: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let sink = quote! {#ast};

    let constructor = quote! {

        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_sink: zenoh_flow::runtime::dataflow::loader::NodeDeclaration<
        zenoh_flow::runtime::dataflow::node::SinkFn,
        > = zenoh_flow::runtime::dataflow::loader::NodeDeclaration::<
        zenoh_flow::runtime::dataflow::node::SinkFn,
        > {
            rustc_version: zenoh_flow::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: zenoh_flow::runtime::dataflow::loader::CORE_VERSION,
            constructor: |context: zenoh_flow::types::Context,
                          configuration: Option<zenoh_flow::types::Configuration>,
                          mut inputs: zenoh_flow::types::Inputs| {
                std::boxed::Box::pin(async {
                    let node = <#ident>::new(context, configuration, inputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn zenoh_flow::traits::Node>)
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

/// The `export_operator` attribute macro is provided to allow the users
/// in exporting their operator.
///
/// ## Example
///
/// ```no_compile
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use zenoh_flow::prelude::*;
///
/// #[export_operator]
/// struct MyOperator;
///
/// #[async_trait]
/// impl Operator for MyOperator {
///     fn new(
///         context: Context,
///         configuration: Option<Configuration>,
///         inputs: Inputs,
///         outputs: Outputs,
/// ) -> Result<Self>
///    where
///    Self: Sized {
///         todo!()
///     }
/// }
///
/// #[async_trait]
/// impl Node for MyOperator {
///     async fn iteration(&self) -> Result<()> {
///         todo!()
///     }
/// }
///
/// ```
#[proc_macro_attribute]
pub fn export_operator(_: TokenStream, input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let ident = &ast.ident;

    let gen = quote! {

        #ast

        #[doc(hidden)]
        #[no_mangle]
        pub static _zf_export_operator: zenoh_flow::runtime::dataflow::loader::NodeDeclaration<
        zenoh_flow::runtime::dataflow::node::OperatorFn,
        > = zenoh_flow::runtime::dataflow::loader::NodeDeclaration::<
        zenoh_flow::runtime::dataflow::node::OperatorFn,
        > {
            rustc_version: zenoh_flow::runtime::dataflow::loader::RUSTC_VERSION,
            core_version: zenoh_flow::runtime::dataflow::loader::CORE_VERSION,
            constructor: |context: zenoh_flow::types::Context,
                          configuration: Option<zenoh_flow::types::Configuration>,
                          mut inputs: zenoh_flow::types::Inputs,
                          mut outputs: zenoh_flow::types::Outputs| {
                std::boxed::Box::pin(async {
                    let node = <#ident>::new(context, configuration, inputs, outputs).await?;
                    Ok(std::sync::Arc::new(node) as std::sync::Arc<dyn zenoh_flow::traits::Node>)
                })
            },
        };
    };
    gen.into()
}
