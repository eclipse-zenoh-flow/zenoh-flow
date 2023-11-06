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

use crate::{
    flattened::{
        nodes::{sink::SinkLibrary, source::SourceLibrary},
        uri::try_load_descriptor,
    },
    DataFlowDescriptor, FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor,
    FlattenedSinkDescriptor, FlattenedSourceDescriptor, InputDescriptor, LinkDescriptor,
    OutputDescriptor,
};
use serde_json::json;
use std::str::FromStr;
use uuid::Uuid;
use zenoh_flow_commons::{RuntimeId, Vars};

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
    let runtime_1: RuntimeId = Uuid::from_str("10628aa2-66ca-4fda-8d5c-d7de63764bcc")
        .unwrap()
        .into();
    let runtime_2: RuntimeId = Uuid::from_str("5f7a170d-cfaf-4f7a-971e-6c3e63c50e1e")
        .unwrap()
        .into();
    let runtime_source_composite: RuntimeId =
        Uuid::from_str("e051658a-0cd6-4cef-8b08-0d0f17d3cc5d")
            .unwrap()
            .into();
    let runtime_operator_composite: RuntimeId =
        Uuid::from_str("6a16a7ba-ec2d-4bb8-b303-7683e5477900")
            .unwrap()
            .into();
    let runtime_sink_composite: RuntimeId = Uuid::from_str("25270228-9cb1-4e6e-988c-00769622359f")
        .unwrap()
        .into();

    // env variable CARGO_MANIFEST_DIR puts us in zenoh-flow/zenoh-flow-descriptors
    let uri = format!("file://{}/data-flow.yml", BASE_DIR);

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(&uri, Vars::default())
        .expect("Failed to load DataFlowDescriptor");

    let flatten = FlattenedDataFlowDescriptor::try_flatten(descriptor, vars).unwrap();

    let expected_sources = vec![
        FlattenedSourceDescriptor {
            id: "source-1".into(),
            description: "source".into(),
            outputs: vec!["source-out".into()],
            library: SourceLibrary::Uri("file://source.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
            runtime: Some(runtime_1),
        },
        FlattenedSourceDescriptor {
            id: "source-2".into(),
            description: "source".into(),
            outputs: vec!["source-out".into()],
            library: SourceLibrary::Uri("file://source.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
            runtime: None,
        },
        FlattenedSourceDescriptor {
            id: "source-composite".into(),
            description: "composite-source".into(),
            outputs: vec![
                "source-composite-out-1".into(),
                "source-composite-out-2".into(),
            ],
            library: SourceLibrary::Uri("file://source-composite.so".into()),
            configuration: json!({ "foo": "global-outer", "bar": "re-reverse" }).into(),
            runtime: Some(runtime_source_composite),
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
            description: "operator".into(),
            inputs: vec!["operator-in".into()],
            outputs: vec!["operator-out".into()],
            library: "file://operator.so".into(),
            configuration: json!({ "foo": "global-outer" }).into(),
            runtime: None,
        },
        FlattenedOperatorDescriptor {
            id: "operator-2".into(),
            description: "operator".into(),
            inputs: vec!["operator-in".into()],
            outputs: vec!["operator-out".into()],
            library: "file://operator.so".into(),
            configuration: json!({ "foo": "global-outer" }).into(),
            runtime: None,
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
            id: "operator-composite>sub-operator-1".into(),
            description: "leaf-operator-1".into(),
            inputs: vec!["sub-operator-1-in-1".into(), "sub-operator-1-in-2".into()],
            outputs: vec!["sub-operator-1-out".into()],
            library: "file://sub-operator-1.so".into(),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer" })
                    .into(),
            runtime: Some(runtime_operator_composite.clone()),
        },
        /*
         * Same spirit but this time it’s a composite operator within a composite operator. The
         * resulting name should be the concatenation of both:
         *
         * operator-composite/sub-operator-composite/sub-sub-operator-1
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite>sub-operator-composite>sub-sub-operator-1".into(),
            description: "sub-leaf-operator-1".into(),
            inputs: vec!["sub-sub-operator-1-in".into()],
            outputs: vec!["sub-sub-operator-1-out".into()],
            library: "file://sub-sub-operator-1.so".into(),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer", "buzz": "composite-inner", "baz": "leaf" }).into(),
            runtime: Some(runtime_operator_composite.clone()),
        },
        /*
         * Idem as above: operator-composite/sub-operator-composite/sub-sub-operator-2.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite>sub-operator-composite>sub-sub-operator-2".into(),
            description: "sub-leaf-operator-2".into(),
            inputs: vec!["sub-sub-operator-2-in".into()],
            outputs: vec!["sub-sub-operator-2-out".into()],
            library: "file://sub-sub-operator-2.so".into(),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer", "buzz": "composite-inner" }).into(),
            runtime: Some(runtime_operator_composite.clone()),
        },
        /*
         * Similarly, we check that the name is the composition: operator-composite/sub-operator-2.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite>sub-operator-2".into(),
            description: "leaf-operator-2".into(),
            inputs: vec!["sub-operator-2-in".into()],
            outputs: vec!["sub-operator-2-out-1".into(), "sub-operator-2-out-2".into()],
            library: "file://sub-operator-2.so".into(),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer" }).into(),
            runtime: Some(runtime_operator_composite),
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
            description: "sink".into(),
            inputs: vec!["sink-in".into()],
            library: SinkLibrary::Uri("file://sink.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
            runtime: None,
        },
        FlattenedSinkDescriptor {
            id: "sink-2".into(),
            description: "sink".into(),
            inputs: vec!["sink-in".into()],
            library: SinkLibrary::Uri("file://sink.so".into()),
            configuration: json!({ "foo": "global-outer" }).into(),
            runtime: Some(runtime_2),
        },
        FlattenedSinkDescriptor {
            id: "sink-composite".into(),
            description: "composite-sink".into(),
            inputs: vec!["sink-composite-in-1".into(), "sink-composite-in-2".into()],
            library: SinkLibrary::Uri("file://sink-composite.so".into()),
            configuration: json!({ "foo": "global-outer", "bar": "reverse" }).into(),
            runtime: Some(runtime_sink_composite),
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
            InputDescriptor::new("operator-composite>sub-operator-1", "sub-operator-1-in-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("source-composite", "source-composite-out-2"),
            InputDescriptor::new("operator-composite>sub-operator-1", "sub-operator-1-in-2"),
        ),
        // operator-composite-sub-2 -> sink-composite
        LinkDescriptor::new(
            OutputDescriptor::new("operator-composite>sub-operator-2", "sub-operator-2-out-1"),
            InputDescriptor::new("sink-composite", "sink-composite-in-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("operator-composite>sub-operator-2", "sub-operator-2-out-2"),
            InputDescriptor::new("sink-composite", "sink-composite-in-2"),
        ),
        // operator-composite-sub-operator-1 ->
        // operator-composite-sub-operator-composite-sub-sub-operator-1
        LinkDescriptor::new(
            OutputDescriptor::new("operator-composite>sub-operator-1", "sub-operator-1-out"),
            InputDescriptor::new(
                "operator-composite>sub-operator-composite>sub-sub-operator-1",
                "sub-sub-operator-1-in",
            ),
        ),
        // operator-composite-sub-operator-composite-sub-sub-operator-2 ->
        // operator-composite-sub-operator-2
        LinkDescriptor::new(
            OutputDescriptor::new(
                "operator-composite>sub-operator-composite>sub-sub-operator-2",
                "sub-sub-operator-2-out",
            ),
            InputDescriptor::new("operator-composite>sub-operator-2", "sub-operator-2-in"),
        ),
        // operator-composite-sub-operator-composite-sub-sub-operator-1 ->
        // operator-composite-sub-operator-composite-sub-sub-operator-2
        LinkDescriptor::new(
            OutputDescriptor::new(
                "operator-composite>sub-operator-composite>sub-sub-operator-1",
                "sub-sub-operator-1-out",
            ),
            InputDescriptor::new(
                "operator-composite>sub-operator-composite>sub-sub-operator-2",
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
}

#[test]
fn test_detect_recursion() {
    let path = format!("{}{}/data-flow-recursion.yml", SCHEME, BASE_DIR,);

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(
        &path,
        Vars::from([("BASE_DIR", BASE_DIR), ("SCHEME", SCHEME)]),
    )
    .expect("Failed to parse descriptor");
    assert!(FlattenedDataFlowDescriptor::try_flatten(descriptor, vars).is_err());
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
    assert!(FlattenedDataFlowDescriptor::try_flatten(descriptor, vars).is_ok());
}
