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
/// Example::
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

        impl zenoh_flow::DowncastAny for #ident {
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
