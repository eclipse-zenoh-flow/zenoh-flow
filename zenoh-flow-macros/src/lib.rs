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

// #![allow(clippy::upper_case_acronyms)]
// #![recursion_limit = "512"]

// extern crate darling;
// extern crate proc_macro;
// extern crate proc_macro2;
// extern crate quote;
// extern crate serde;
// extern crate syn;

// use darling::FromDeriveInput;
// use inflector::cases::snakecase::to_snake_case;
// use proc_macro::TokenStream;
// use proc_macro2::{Span, TokenStream as TokenStream2};
// use quote::{format_ident, quote, quote_spanned, ToTokens};
// use std::collections::HashMap;
// use std::str::FromStr;
// use syn::visit_mut::VisitMut;
// use syn::{
//     braced,
//     ext::IdentExt,
//     parenthesized,
//     parse::{Parse, ParseStream},
//     parse_macro_input, parse_quote,
//     spanned::Spanned,
//     token::Comma,
//     Attribute, AttributeArgs, Block, DeriveInput, FnArg, Ident, ImplItem, ImplItemMethod,
//     ImplItemType, ItemImpl, Pat, PatIdent, PatType, Receiver, ReturnType, Signature, Token, Type,
//     Visibility,
// };
// use syn_serde::json;

// use proc_macro_error::proc_macro_error;

// #[derive(Debug, FromDeriveInput)]
// struct ZFOperatorMacroArgs {
//     modes: HashMap<String, HashMap<String, Ident>>,
//     inputs: Vec<Type>,
//     outputs: Vec<Type>,
// }

// // struct ZFOpMacro {
// //     // attrs: Vec<Attribute>,
// //     ident: Ident,
// // }

// // impl Parse for ZFOpMacro {
// //     fn parse(input: ParseStream) -> syn::Result<Self> {
// //         // let attrs = input.call(Attribute::parse_outer)?;

// //         let ident: Ident = input.parse()?;
// //         Ok(Self {
// //             // attrs,
// //             ident,
// //         })
// //     }
// // }

// // #[proc_macro_attribute]
// // pub fn zfoperator(attr: TokenStream, input: TokenStream) -> TokenStream {
// //     let i = input.clone();
// //     //parsing attributes and ident
// //     // let ZFOpMacro {
// //     //     // ref attrs,
// //     //     ref ident,
// //     // } = parse_macro_input!(input as ZFOpMacro);

// //     let attr_args = parse_macro_input!(attr as DeriveInput);
// //     let macro_args = match ZFOperatorMacroArgs::from_derive_input(&attr_args) {
// //         Ok(v) => v,
// //         Err(e) => {
// //             return TokenStream::from(e.write_errors());
// //         }
// //     };

// //     println!("Attributes: {:?}", macro_args);

// //     i
// // }

// #[proc_macro_derive(ZFOperator, attributes(input))]
// #[proc_macro_error]
// pub fn derive_zfoperator(input: TokenStream) -> TokenStream {
//     let DeriveInput { ident, attrs, .. } = parse_macro_input!(input);

//     // let inputs : Vec<Ident> = attrs
//     let x: Vec<()> = attrs
//         .iter()
//         .filter(|attr| attr.path.is_ident("input"))
//         .map(|attr| parse_input_attribute(&attr))
//         .collect();

//     println!("x {:?}", x);
//     //println!("Inputs {:?}", inputs);

//     TokenStream::from(quote! {
//         impl #ident {
//             pub fn hello() -> String {
//                 "Hello".to_string()
//             }
//         }
//     })
// }

// //TODO remove unwrap
// fn parse_input_attribute(attr: &Attribute) {
//     println!("### Attr: {:?}", attr);

//     let x = &attr.tokens;
//     println!("### Inner: {:?}", x);
//     let list: syn::Meta = attr.parse_meta().unwrap();
//     println!("### List: {:?}", list);
//     let ident = list.path().get_ident().unwrap();
//     println!("### Ident: {:?}", ident);
//     //(*ident).clone()
//     let ts = quote!(#ident);
//     println!("### ts: {:?}", ts);
//     // let t : Type = parse_macro_input!(ts as Type);
//     // t
// }
