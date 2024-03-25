//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::{
    flattened::nodes::{sink::SinkVariant, source::SourceVariant},
    uri::try_load_descriptor,
    DataFlowDescriptor, FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor,
    FlattenedSinkDescriptor, FlattenedSourceDescriptor, InputDescriptor, LinkDescriptor,
    OutputDescriptor,
};
use serde_json::json;
use url::Url;
use uuid::Uuid;
use zenoh_flow_commons::{NodeId, RuntimeId, Vars};

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
    let base_dir = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), BASE_DIR);
    let runtime_1 = RuntimeId::rand();
    let runtime_2 = RuntimeId::rand();
    let runtime_composite = RuntimeId::rand();

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(
        &Url::parse(&format!("file://{}/data-flow.yml", base_dir)).unwrap(),
        Vars::from([
            ("BASE_DIR", base_dir.as_str()),
            ("RUNTIME_1", format!("{}", runtime_1).as_str()),
            ("RUNTIME_2", format!("{}", runtime_2).as_str()),
            (
                "RUNTIME_COMPOSITE",
                format!("{}", runtime_composite).as_str(),
            ),
        ]),
    )
    .expect("Failed to load DataFlowDescriptor");

    let flatten = FlattenedDataFlowDescriptor::try_flatten(descriptor, vars).unwrap();

    let expected_sources = vec![
        FlattenedSourceDescriptor {
            id: "source-1".into(),
            description: Some("source".into()),
            outputs: vec!["source-out".into()],
            source: SourceVariant::Library(Url::parse("file://source.so").unwrap()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSourceDescriptor {
            id: "source-2".into(),
            description: Some("source".into()),
            outputs: vec!["source-out".into()],
            source: SourceVariant::Library(Url::parse("file://source.so").unwrap()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSourceDescriptor {
            id: "source-composite".into(),
            description: Some("composite-source".into()),
            outputs: vec![
                "source-composite-out-1".into(),
                "source-composite-out-2".into(),
            ],
            source: SourceVariant::Library(Url::parse("file://source-composite.so").unwrap()),
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
            description: Some("Outer description".into()),
            inputs: vec!["operator-in".into()],
            outputs: vec!["operator-out".into()],
            library: Url::parse("file://operator.so").unwrap(),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedOperatorDescriptor {
            id: "operator-2".into(),
            description: Some("operator".into()),
            inputs: vec!["operator-in".into()],
            outputs: vec!["operator-out".into()],
            library: Url::parse("file://operator.so").unwrap(),
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
            id: "operator-composite>sub-operator-1".into(),
            description: Some("leaf-operator-1".into()),
            inputs: vec!["sub-operator-1-in-1".into(), "sub-operator-1-in-2".into()],
            outputs: vec!["sub-operator-1-out".into()],
            library: Url::parse("file://sub-operator-1.so").unwrap(),
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
            id: "operator-composite>sub-operator-composite>sub-sub-operator-1".into(),
            description: Some("sub-leaf-operator-1".into()),
            inputs: vec!["sub-sub-operator-1-in".into()],
            outputs: vec!["sub-sub-operator-1-out".into()],
            library: Url::parse("file://sub-sub-operator-1.so").unwrap(),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer", "buzz": "composite-inner", "baz": "leaf" }).into(),
        },
        /*
         * Idem as above: operator-composite/sub-operator-composite/sub-sub-operator-2.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite>sub-operator-composite>sub-sub-operator-2".into(),
            description: Some("sub-leaf-operator-2".into()),
            inputs: vec!["sub-sub-operator-2-in".into()],
            outputs: vec!["sub-sub-operator-2-out".into()],
            library: Url::parse("file://sub-sub-operator-2.so").unwrap(),
            configuration:
                json!({ "foo": "global-outer", "quux": "global-inner", "bar": "composite-outer", "buzz": "composite-inner" }).into(),
        },
        /*
         * Similarly, we check that the name is the composition: operator-composite/sub-operator-2.
         */
        FlattenedOperatorDescriptor {
            id: "operator-composite>sub-operator-2".into(),
            description: Some("leaf-operator-2".into()),
            inputs: vec!["sub-operator-2-in".into()],
            outputs: vec!["sub-operator-2-out-1".into(), "sub-operator-2-out-2".into()],
            library: Url::parse("file://sub-operator-2.so").unwrap(),
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
            description: Some("sink".into()),
            inputs: vec!["sink-in".into()],
            sink: SinkVariant::Library(Url::parse("file://sink.so").unwrap()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSinkDescriptor {
            id: "sink-2".into(),
            description: Some("sink".into()),
            inputs: vec!["sink-in".into()],
            sink: SinkVariant::Library(Url::parse("file://sink.so").unwrap()),
            configuration: json!({ "foo": "global-outer" }).into(),
        },
        FlattenedSinkDescriptor {
            id: "sink-composite".into(),
            description: Some("composite-sink".into()),
            inputs: vec!["sink-composite-in-1".into(), "sink-composite-in-2".into()],
            sink: SinkVariant::Library(Url::parse("file://sink-composite.so").unwrap()),
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

    let expected_mapping: HashMap<RuntimeId, HashSet<NodeId>> = HashMap::from([
        (runtime_1, HashSet::from(["source-1".into()])),
        (runtime_2, HashSet::from(["sink-2".into()])),
        (
            runtime_composite,
            HashSet::from([
                "source-composite".into(),
                "sink-composite".into(),
                "operator-composite>sub-operator-1".into(),
                "operator-composite>sub-operator-composite>sub-sub-operator-1".into(),
                "operator-composite>sub-operator-composite>sub-sub-operator-2".into(),
                "operator-composite>sub-operator-2".into(),
            ]),
        ),
    ]);

    assert_eq!(expected_mapping, flatten.mapping);
}

#[test]
fn test_detect_recursion() {
    let base_dir = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), BASE_DIR);
    let url = Url::parse(&format!("{}{}/data-flow-recursion.yml", SCHEME, base_dir)).unwrap();

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(
        &url,
        Vars::from([("BASE_DIR", base_dir.as_str()), ("SCHEME", SCHEME)]),
    )
    .expect("Failed to parse descriptor");
    assert!(FlattenedDataFlowDescriptor::try_flatten(descriptor, vars).is_err());
}

#[test]
fn test_duplicate_composite_at_same_level_not_detected_as_recursion() {
    let base_dir = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), BASE_DIR);
    let path = Url::parse(&format!(
        "{}/{}/data-flow-recursion-duplicate-composite.yml",
        SCHEME, base_dir,
    ))
    .unwrap();

    let (descriptor, vars) = try_load_descriptor::<DataFlowDescriptor>(
        &path,
        Vars::from([("BASE_DIR", base_dir.as_str()), ("SCHEME", SCHEME)]),
    )
    .expect("Failed to parse descriptor");

    assert!(FlattenedDataFlowDescriptor::try_flatten(descriptor, vars).is_ok());
}

#[test]
fn test_serialize_deserialize() {
    let flow_yaml = r#"
name: test-flow

sources:
  - id: source-0
    description: source-0
    library: "file:///home/zenoh-flow/libsource.so"
    outputs:
      - out-1
    configuration:
      id: source-0

operators:
  - id: operator-1
    description: operator-1
    library: "file:///home/zenoh-flow/liboperator.so"
    inputs:
      - in-0
    outputs:
      - out-2
    configuration:
      id: operator-1

sinks:
  - id: sink-2
    description: sink-2
    library: "file:///home/zenoh-flow/libsink.so"
    inputs:
      - in-1
    configuration:
      id: sink-2

links:
  - from:
      node: source-0
      output: out-1
    to:
      node: operator-1
      input: in-0

  - from:
      node: operator-1
      output: out-2
    to:
      node: sink-2
      input: in-1
"#;

    let flat_flow_yaml = serde_yaml::from_str::<FlattenedDataFlowDescriptor>(flow_yaml)
        .expect("Failed to deserialize flow from YAML");

    let json_string_flow =
        serde_json::to_string(&flat_flow_yaml).expect("Failed to serialize flow as JSON");

    let flat_flow_json =
        serde_json::from_str::<FlattenedDataFlowDescriptor>(json_string_flow.as_str())
            .expect("Failed to deserialize flow from JSON");

    assert_eq!(flat_flow_yaml, flat_flow_json);
    assert!(flat_flow_json.id.is_none());
    assert!(flat_flow_yaml.mapping.is_empty());
}

#[test]
fn test_serialize_deserialize_no_operators() {
    let flow_yaml = r#"
id: e8f1f252-e984-4d0f-92f6-0cd97b5cfc1e

name: test-flow

sources:
  - id: source-0
    description: source-0
    library: "file:///home/zenoh-flow/libsource.so"
    outputs:
      - out-1
    configuration:
      id: source-0

sinks:
  - id: sink-2
    description: sink-2
    library: "file:///home/zenoh-flow/libsink.so"
    inputs:
      - in-1
    configuration:
      id: sink-2

links:
  - from:
      node: source-0
      output: out-1
    to:
      node: sink-2
      input: in-1
"#;

    let flat_flow_yaml = serde_yaml::from_str::<FlattenedDataFlowDescriptor>(flow_yaml)
        .expect("Failed to deserialize flow from YAML");

    let json_string_flow =
        serde_json::to_string(&flat_flow_yaml).expect("Failed to serialize flow as JSON");

    let flat_flow_json =
        serde_json::from_str::<FlattenedDataFlowDescriptor>(json_string_flow.as_str())
            .expect("Failed to deserialize flow from JSON");

    assert_eq!(flat_flow_yaml, flat_flow_json);
    assert_eq!(
        flat_flow_json.id,
        Some(
            Uuid::from_str("e8f1f252-e984-4d0f-92f6-0cd97b5cfc1e")
                .unwrap()
                .into()
        )
    );
    assert!(flat_flow_yaml.mapping.is_empty());
}
