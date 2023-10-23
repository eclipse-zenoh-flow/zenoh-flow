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

use std::collections::HashMap;

use crate::vars::Vars;
use crate::{
    uri::try_load_descriptor, DataFlowDescriptor, FlattenedOperatorDescriptor,
    FlattenedSinkDescriptor, FlattenedSourceDescriptor, InputDescriptor, LinkDescriptor,
    OutputDescriptor,
};
use serde_json::json;

const BASE_DIR: &str = "./tests/descriptors";
const SCHEME: &str = "file://";

// This test is trying to be thorough when it comes to a data flow descriptor. In particular, we
// want to make sure of the following:
//
// - the fact that the configuration is propagated --- correctly,
// - the naming of the composite operators,
// - the naming of the ports,
// - the links.
//
// See the comments around the "expected" structures for more information.
#[test]
fn test_flatten_descriptor() {
    // env variable CARGO_MANIFEST_DIR puts us in zenoh-flow/zenoh-flow-descriptors
    let uri = format!("file://{}/data-flow.yml", BASE_DIR);

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(&uri, Vars::default())
        .expect("Failed to load DataFlowDescriptor");

    let flatten = descriptor
        .flatten(vars)
        .expect("Failed to flatten descriptor");

    let expected_sources = vec![
        FlattenedSourceDescriptor {
            id: "source-1".into(),
            name: "source".into(),
            outputs: vec!["source-out".into()],
            uri: Some("file://source.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSourceDescriptor {
            id: "source-2".into(),
            name: "source".into(),
            outputs: vec!["source-out".into()],
            uri: Some("file://source.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSourceDescriptor {
            id: "source-composite".into(),
            name: "composite-source".into(),
            outputs: vec![
                "source-composite-out-1".into(),
                "source-composite-out-2".into(),
            ],
            uri: Some("file://source-composite.so".into()),
            configuration: json!({ "foo": "global-outer", "bar": "re-reverse" }).into(),
        },
    ];

    expected_sources.iter().for_each(|expected_source| {
        assert!(
            flatten.sources.contains(expected_source),
            "Source missing or incorrect: \n\n (expected) {:?} \n\n {:?}",
            expected_source,
            flatten
                .sources
                .iter()
                .find(|source| source.id == expected_source.id)
        )
    });
    assert_eq!(expected_sources.len(), flatten.sources.len());

    let expected_operators = vec![
        FlattenedOperatorDescriptor {
            id: "operator-1".into(),
            name: "operator".into(),
            inputs: vec!["operator-in".into()],
            outputs: vec!["operator-out".into()],
            uri: Some("file://operator.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedOperatorDescriptor {
            id: "operator-2".into(),
            name: "operator".into(),
            inputs: vec!["operator-in".into()],
            outputs: vec!["operator-out".into()],
            uri: Some("file://operator.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        /*
         * `sub-operator-1` is declared in the file "operator-composite.yml".
         *
         * Here we are checking that its name, after flattening, is the concatenation of the id of
         * the (composite) operator in "data-flow.yml" and the id of the actual operator in
         * "operator-composite.yml": operator-composite/sub-operator-1
         *
         * The names of the ports are left intact.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite/sub-operator-1".into(),
            name: "leaf-operator-1".into(),
            inputs: vec!["sub-operator-1-in-1".into(), "sub-operator-1-in-2".into()],
            outputs: vec!["sub-operator-1-out".into()],
            uri: Some("file://sub-operator-1.so".into()),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer" })
                    .into(),
        },
        /*
         * Same spirit but this time it’s a composite operator within a composite operator. The
         * resulting name should be the concatenation of both:
         *
         * operator-composite/sub-operator-composite/sub-sub-operator-1
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite/sub-operator-composite/sub-sub-operator-1".into(),
            name: "sub-leaf-operator-1".into(),
            inputs: vec!["sub-sub-operator-1-in".into()],
            outputs: vec!["sub-sub-operator-1-out".into()],
            uri: Some("file://sub-sub-operator-1.so".into()),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer", "buzz": "composite-inner", "baz": "leaf" }).into(),
        },
        /*
         * Idem as above: operator-composite/sub-operator-composite/sub-sub-operator-2.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite/sub-operator-composite/sub-sub-operator-2".into(),
            name: "sub-leaf-operator-2".into(),
            inputs: vec!["sub-sub-operator-2-in".into()],
            outputs: vec!["sub-sub-operator-2-out".into()],
            uri: Some("file://sub-sub-operator-2.so".into()),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer", "buzz": "composite-inner" }).into(),
        },
        /*
         * Similarly, we check that the name is the composition: operator-composite/sub-operator-2.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite/sub-operator-2".into(),
            name: "leaf-operator-2".into(),
            inputs: vec!["sub-operator-2-in".into()],
            outputs: vec!["sub-operator-2-out-1".into(), "sub-operator-2-out-2".into()],
            uri: Some("file://sub-operator-2.so".into()),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer" }).into(),
        },
    ];

    expected_operators.iter().for_each(|expected_operator| {
        assert!(
            flatten.operators.contains(expected_operator),
            "Operator missing or incorrect: \n\n (expected) {:?} \n\n (found) {:?} \n\n(operators): {:?}\n\n",
            expected_operator,
            flatten
                .operators
                .iter()
                .find(|operator| operator.id == expected_operator.id),
            flatten.operators
        )
    });
    assert_eq!(expected_operators.len(), flatten.operators.len());

    let expected_sinks = vec![
        FlattenedSinkDescriptor {
            id: "sink-1".into(),
            name: "sink".into(),
            inputs: vec!["sink-in".into()],
            uri: Some("file://sink.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSinkDescriptor {
            id: "sink-2".into(),
            name: "sink".into(),
            inputs: vec!["sink-in".into()],
            uri: Some("file://sink.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSinkDescriptor {
            id: "sink-composite".into(),
            name: "composite-sink".into(),
            inputs: vec!["sink-composite-in-1".into(), "sink-composite-in-2".into()],
            uri: Some("file://sink-composite.so".into()),
            configuration: json!({ "foo": "global-outer", "bar": "reverse" }).into(),
        },
    ];

    expected_sinks.iter().for_each(|expected_sink| {
        assert!(
            flatten.sinks.contains(expected_sink),
            "Sink missing or incorrect: \n\n (expected) {:?} \n\n {:?}",
            expected_sink,
            flatten.sinks
        )
    });
    assert_eq!(expected_sinks.len(), flatten.sinks.len());

    let expected_links = vec![
        // source 1 -> operator 1 -> sink 1
        LinkDescriptor::new(
            OutputDescriptor::new("source-1", "source-out"),
            InputDescriptor::new("operator-1", "operator-in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("operator-1", "operator-out"),
            InputDescriptor::new("sink-1", "sink-in"),
        ),
        // source 2 -> operator 2 -> sink 2
        LinkDescriptor::new(
            OutputDescriptor::new("source-2", "source-out"),
            InputDescriptor::new("operator-2", "operator-in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("operator-2", "operator-out"),
            InputDescriptor::new("sink-2", "sink-in"),
        ),
        // source-composite -> operator-composite-sub-1
        /*
         * The name of the port at the "junction" between the container & the composite is the one
         * declared in the **container**.
         *
         * Hence, the name of the input ports of "sub-operator-1" (operator-composite.yml) are
         * replaced by what is declared in "data-flow.yml".
         */
        LinkDescriptor::new(
            OutputDescriptor::new("source-composite", "source-composite-out-1"),
            InputDescriptor::new("operator-composite/sub-operator-1", "sub-operator-1-in-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("source-composite", "source-composite-out-2"),
            InputDescriptor::new("operator-composite/sub-operator-1", "sub-operator-1-in-2"),
        ),
        // operator-composite-sub-2 -> sink-composite
        LinkDescriptor::new(
            OutputDescriptor::new("operator-composite/sub-operator-2", "sub-operator-2-out-1"),
            InputDescriptor::new("sink-composite", "sink-composite-in-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("operator-composite/sub-operator-2", "sub-operator-2-out-2"),
            InputDescriptor::new("sink-composite", "sink-composite-in-2"),
        ),
        // operator-composite-sub-operator-1 ->
        // operator-composite-sub-operator-composite-sub-sub-operator-1
        LinkDescriptor::new(
            OutputDescriptor::new("operator-composite/sub-operator-1", "sub-operator-1-out"),
            InputDescriptor::new(
                "operator-composite/sub-operator-composite/sub-sub-operator-1",
                "sub-sub-operator-1-in",
            ),
        ),
        // operator-composite-sub-operator-composite-sub-sub-operator-2 ->
        // operator-composite-sub-operator-2
        LinkDescriptor::new(
            OutputDescriptor::new(
                "operator-composite/sub-operator-composite/sub-sub-operator-2",
                "sub-sub-operator-2-out",
            ),
            InputDescriptor::new("operator-composite/sub-operator-2", "sub-operator-2-in"),
        ),
        // operator-composite-sub-operator-composite-sub-sub-operator-1 ->
        // operator-composite-sub-operator-composite-sub-sub-operator-2
        LinkDescriptor::new(
            OutputDescriptor::new(
                "operator-composite/sub-operator-composite/sub-sub-operator-1",
                "sub-sub-operator-1-out",
            ),
            InputDescriptor::new(
                "operator-composite/sub-operator-composite/sub-sub-operator-2",
                "sub-sub-operator-2-in",
            ),
        ),
    ];

    expected_links.iter().for_each(|expected_link| {
        assert!(
            flatten.links.contains(expected_link),
            "Link missing or incorrect: \n\n (expected) {:?} \n\n {:?}",
            expected_link,
            flatten.links
        )
    });
    assert_eq!(expected_links.len(), flatten.links.len());

    let expected_mapping = HashMap::from([
        ("source-1".into(), "runtime-1".into()),
        ("sink-2".into(), "runtime-2".into()),
        ("source-composite".into(), "runtime-source".into()),
        (
            "operator-composite/sub-operator-1".into(),
            "runtime-composite".into(),
        ),
        (
            "operator-composite/sub-operator-composite/sub-sub-operator-1".into(),
            "runtime-composite".into(),
        ),
        (
            "operator-composite/sub-operator-composite/sub-sub-operator-2".into(),
            "runtime-composite".into(),
        ),
        (
            "operator-composite/sub-operator-2".into(),
            "runtime-composite".into(),
        ),
        ("sink-composite".into(), "runtime-sink".into()),
    ]);

    assert_eq!(expected_mapping, flatten.mapping);
}

#[test]
fn test_detect_recursion() {
    let path = format!("{}{}/data-flow-recursion.yml", SCHEME, BASE_DIR,);

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(
        &path,
        Vars::from([("BASE_DIR", BASE_DIR), ("SCHEME", SCHEME)]),
    )
    .expect("Failed to parse descriptor");
    let res_flatten = descriptor.flatten(vars);
    assert!(res_flatten.is_err());
}

#[test]
fn test_duplicate_composite_at_same_level_not_detected_as_recursion() {
    let path = format!(
        "{}{}/data-flow-recursion-duplicate-composite.yml",
        SCHEME, BASE_DIR,
    );

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(
        &path,
        Vars::from([("BASE_DIR", BASE_DIR), ("SCHEME", SCHEME)]),
    )
    .expect("Failed to parse descriptor");
    assert!(descriptor.flatten(vars).is_ok());
}
