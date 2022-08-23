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

use super::DataFlowDescriptor;
use crate::model::{
    link::{LinkDescriptor, PortDescriptor},
    node::{SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor},
    InputDescriptor, OutputDescriptor,
};
use std::{
    fs::File,
    io::{BufReader, Read},
};

const BASE_PATH: &str = "src/model/dataflow/tests/";

// We are testing quite a few things within this function:
//
// - TODO the fact that flags are propagated,
// - TODO the fact that the configuration is propagated --- correctly,
// - the naming of the composite operators,
// - the naming of the ports,
// - the links.
//
// Look at the comments around the "expected" structures for more information.
#[test]
fn test_flatten_descriptor() {
    // FIXME Use Path to join both?
    // env variable CARGO_MANIFEST_DIR puts us in zenoh-flow/zenoh-flow
    let path = format!(
        "{}/{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        BASE_PATH,
        "data-flow.yml"
    );

    let file = File::open(path).expect("Could not open file");
    let mut buf_reader = BufReader::new(file);
    let mut yaml = String::new();
    buf_reader
        .read_to_string(&mut yaml)
        .expect("Could not read file contents");

    let descriptor = DataFlowDescriptor::from_yaml(&yaml).expect("Unexpected error");

    let flatten = async_std::task::block_on(async { descriptor.flatten().await })
        .expect("Unexpected error while calling `flatten`");

    let expected_sources = vec![
        SourceDescriptor {
            id: "source-1".into(),
            outputs: vec![PortDescriptor::new("source-1-out", "_any_")],
            uri: Some("file://source-1.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
        /*
        "source-2" is kept even though its associated flag is set to `false`.
        Disabled nodes are removed when the FlattenDescriptor is turned into a Record.
         */
        SourceDescriptor {
            id: "source-2".into(),
            outputs: vec![PortDescriptor::new("source-2-out", "_any_")],
            uri: Some("file://source-2.so".into()),
            configuration: None,
            flags: Some(vec!["flag-2".into()]),
            tags: vec![],
        },
        SourceDescriptor {
            id: "source-composite".into(),
            outputs: vec![
                PortDescriptor::new("source-composite-out-1", "_any_"),
                PortDescriptor::new("source-composite-out-2", "_any_"),
            ],
            uri: Some("file://source-composite.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
    ];

    // NOTE: The order matters!
    assert_eq!(expected_sources, flatten.sources);

    let expected_operators = vec![
        SimpleOperatorDescriptor {
            id: "operator-1".into(),
            inputs: vec![PortDescriptor::new("operator-1-in", "_any_")],
            outputs: vec![PortDescriptor::new("operator-1-out", "_any_")],
            uri: Some("file://operator-1.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
        SimpleOperatorDescriptor {
            id: "operator-2".into(),
            inputs: vec![PortDescriptor::new("operator-2-in", "_any_")],
            outputs: vec![PortDescriptor::new("operator-2-out", "_any_")],
            uri: Some("file://operator-2.so".into()),
            configuration: None,
            // FIXME Update once the flags are properly handled.
            // flags: Some(vec!["flag-2".into()]),
            flags: None,
            tags: vec![],
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
        SimpleOperatorDescriptor {
            id: "operator-composite/sub-operator-1".into(),
            inputs: vec![
                PortDescriptor::new("sub-operator-1-in-1", "_any_"),
                PortDescriptor::new("sub-operator-1-in-2", "_any_"),
            ],
            outputs: vec![PortDescriptor::new("sub-operator-1-out", "_any_")],
            uri: Some("file://sub-operator-1.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
        /*
         * Same spirit but this time it’s a composite operator within a composite operator. The
         * resulting name should be the concatenation of both:
         *
         * operator-composite/sub-operator-composite/sub-sub-operator-1
         */
        SimpleOperatorDescriptor {
            id: "operator-composite/sub-operator-composite/sub-sub-operator-1".into(),
            inputs: vec![PortDescriptor::new("sub-sub-operator-1-in", "_any_")],
            outputs: vec![PortDescriptor::new("sub-sub-operator-1-out", "_any_")],
            uri: Some("file://sub-sub-operator-1.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
        /*
         * Idem as above: operator-composite/sub-operator-composite/sub-sub-operator-2.
         */
        SimpleOperatorDescriptor {
            id: "operator-composite/sub-operator-composite/sub-sub-operator-2".into(),
            inputs: vec![PortDescriptor::new("sub-sub-operator-2-in", "_any_")],
            outputs: vec![PortDescriptor::new("sub-sub-operator-2-out", "_any_")],
            uri: Some("file://sub-sub-operator-2.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
        /*
         * Similarly, we check that the name is the composition: operator-composite/sub-operator-2.
         */
        SimpleOperatorDescriptor {
            id: "operator-composite/sub-operator-2".into(),
            inputs: vec![PortDescriptor::new("sub-operator-2-in", "_any_")],
            outputs: vec![
                PortDescriptor::new("sub-operator-2-out-1", "_any_"),
                PortDescriptor::new("sub-operator-2-out-2", "_any_"),
            ],
            uri: Some("file://sub-operator-2.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
    ];

    // NOTE: The order matters!
    assert_eq!(expected_operators, flatten.operators);

    let expected_sinks = vec![
        SinkDescriptor {
            id: "sink-1".into(),
            inputs: vec![PortDescriptor::new("sink-1-in", "_any_")],
            uri: Some("file://sink-1.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
        /*
         * "sink-2" is kept even though its associated flag is set to `false`.
         * Disabled nodes are removed when the FlattenDescriptor is turned into a Record.
         */
        SinkDescriptor {
            id: "sink-2".into(),
            inputs: vec![PortDescriptor::new("sink-2-in", "_any_")],
            uri: Some("file://sink-2.so".into()),
            configuration: None,
            flags: Some(vec!["flag-2".into()]),
            tags: vec![],
        },
        SinkDescriptor {
            id: "sink-composite".into(),
            inputs: vec![
                PortDescriptor::new("sink-composite-in-1", "_any_"),
                PortDescriptor::new("sink-composite-in-2", "_any_"),
            ],
            uri: Some("file://sink-composite.so".into()),
            configuration: None,
            flags: None,
            tags: vec![],
        },
    ];

    // NOTE: The order matters!
    assert_eq!(expected_sinks, flatten.sinks);

    let expected_links = vec![
        // source 1 -> operator 1 -> sink 1
        LinkDescriptor::new(
            OutputDescriptor::new("source-1", "source-1-out"),
            InputDescriptor::new("operator-1", "operator-1-in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("operator-1", "operator-1-out"),
            InputDescriptor::new("sink-1", "sink-1-in"),
        ),
        // source 2 -> operator 2 -> sink 2
        LinkDescriptor::new(
            OutputDescriptor::new("source-2", "source-2-out"),
            InputDescriptor::new("operator-2", "operator-2-in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("operator-2", "operator-2-out"),
            InputDescriptor::new("sink-2", "sink-2-in"),
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

    expected_links.iter().for_each(|link| {
        println!("{:?}", link);
        assert!(flatten.links.contains(link))
    });

    assert_eq!(expected_links, flatten.links);
}

#[test]
fn test_detect_recursion() {
    // FIXME Use Path to join them?
    let path = format!(
        "{}/{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        BASE_PATH,
        "data-flow-recursion.yml"
    );

    let file = File::open(path).expect("Could not open file");
    let mut buf_reader = BufReader::new(file);
    let mut yaml = String::new();
    buf_reader
        .read_to_string(&mut yaml)
        .expect("Could not read file contents");

    let descriptor = DataFlowDescriptor::from_yaml(&yaml).expect("Unexpected error");
    let res_flatten = async_std::task::block_on(async { descriptor.flatten().await });
    assert!(res_flatten.is_err());
}

#[test]
fn test_duplicate_composite_at_same_level_not_detected_as_recursion() {
    // FIXME Use Path to join them?
    let path = format!(
        "{}/{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        BASE_PATH,
        "data-flow-recursion-duplicate-composite.yml"
    );

    let file = File::open(path).expect("Could not open file");
    let mut buf_reader = BufReader::new(file);
    let mut yaml = String::new();
    buf_reader
        .read_to_string(&mut yaml)
        .expect("Could not read file contents");

    let descriptor = DataFlowDescriptor::from_yaml(&yaml).expect("Unexpected error");
    assert!(async_std::task::block_on(async { descriptor.flatten().await }).is_ok());
}
