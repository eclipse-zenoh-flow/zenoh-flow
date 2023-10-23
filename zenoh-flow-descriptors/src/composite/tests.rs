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

use zenoh_flow_commons::Configuration;

use crate::flattened::IFlattenableComposite;
use std::collections::HashSet;

use crate::vars::Vars;
use crate::{
    composite::Substitutions, flattened::Patch, CompositeInputDescriptor,
    CompositeOperatorDescriptor, CompositeOutputDescriptor, FlattenedOperatorDescriptor,
    InputDescriptor, LinkDescriptor, NodeDescriptor, OutputDescriptor,
};

const BASE_DIR: &str = "./tests/descriptors";

#[test]
fn test_flatten_composite_descriptor_non_nested() {
    let composite_descriptor = CompositeOperatorDescriptor {
        description: "composite-test".into(),
        inputs: vec![
            CompositeInputDescriptor::new("input-1", "my-operator-1", "operator-1-in-1"),
            CompositeInputDescriptor::new("input-2", "my-operator-1", "operator-1-in-2"),
        ],
        outputs: vec![CompositeOutputDescriptor::new(
            "output-1",
            "my-operator-2",
            "operator-2-out",
        )],
        operators: vec![
            NodeDescriptor {
                id: "my-operator-1".into(),
                descriptor: format!("file://./{}/operator-1.yml", BASE_DIR).into(),
                configuration: Configuration::default(),
            },
            NodeDescriptor {
                id: "my-operator-2".into(),
                descriptor: format!("file://./{}/operator-2.yml", BASE_DIR).into(),
                configuration: Configuration::default(),
            },
        ],
        links: vec![LinkDescriptor::new(
            OutputDescriptor::new("my-operator-1", "operator-1-out"),
            InputDescriptor::new("my-operator-2", "operator-2-in"),
        )],
        configuration: Configuration::default(),
    };

    let (flattened_operators, flattened_links, patch) = composite_descriptor
        .flatten_composite(
            "composite".into(),
            Configuration::default(),
            Vars::default(),
            &mut HashSet::default(),
        )
        .expect("Unexpected error while flattening composite operator");

    // Here we check that the id of the operator has been correctly updated:
    // - it should be prefixed with the name of the composite operator,
    // - a "/" should follow,
    // - the name of the operator as per how it’s written in the composite should be last.
    //
    // We should see "composite/my-operator-1" & "composite/my-operator-2".
    let expected_operators = vec![
        FlattenedOperatorDescriptor {
            id: "composite/my-operator-1".into(),
            description: "operator-1".into(),
            inputs: vec!["operator-1-in-1".into(), "operator-1-in-2".into()],
            outputs: vec!["operator-1-out".into()],
            uri: Some("file://operator-1.so".into()),
            configuration: Configuration::default(),
        },
        FlattenedOperatorDescriptor {
            id: "composite/my-operator-2".into(),
            description: "operator-2".into(),
            inputs: vec!["operator-2-in".into()],
            outputs: vec!["operator-2-out".into()],
            uri: Some("file://operator-2.so".into()),
            configuration: Configuration::default(),
        },
    ];

    // NOTE: This `assert_eq` also checks the order of the elements!
    assert_eq!(flattened_operators, expected_operators);

    // Here we check that the links have been correctly updated:
    // - the id of the composite operator has been replaced with the "new_id" (composite/operator),
    // - the inputs & outputs have been replaced with what was declared in the yaml file of the
    //   actual operator,
    // - the links inside the composite operator have been added,
    // - only the links of the concerned composite operator were updated.
    let expected_links = vec![LinkDescriptor::new(
        OutputDescriptor::new("composite/my-operator-1", "operator-1-out"),
        InputDescriptor::new("composite/my-operator-2", "operator-2-in"),
    )];

    // NOTE: This `assert_eq` also checks the order of the elements!
    assert_eq!(flattened_links, expected_links);

    let expected_patch = Patch {
        subs_inputs: Substitutions::<InputDescriptor>::from([
            (
                InputDescriptor::new("composite", "input-1"),
                InputDescriptor::new("composite/my-operator-1", "operator-1-in-1"),
            ),
            (
                InputDescriptor::new("composite", "input-2"),
                InputDescriptor::new("composite/my-operator-1", "operator-1-in-2"),
            ),
        ]),
        subs_outputs: Substitutions::<OutputDescriptor>::from([(
            OutputDescriptor::new("composite", "output-1"),
            OutputDescriptor::new("composite/my-operator-2", "operator-2-out"),
        )]),
    };

    assert_eq!(expected_patch, patch);
}

#[test]
fn test_flatten_composite_descriptor_nested() {
    let nested_composite_descriptor = CompositeOperatorDescriptor {
        description: "nested-composite-test".into(),
        inputs: vec![CompositeInputDescriptor::new(
            "composite-input",
            "composite-outer-i",
            "composite-outer-in",
        )],
        outputs: vec![CompositeOutputDescriptor::new(
            "composite-output",
            "composite-outer-o",
            "composite-outer-out",
        )],
        operators: vec![
            NodeDescriptor {
                id: "composite-outer-o".into(),
                descriptor: format!("file://./{}/composite-outer.yml", BASE_DIR).into(),
                configuration: Configuration::default(),
            },
            NodeDescriptor {
                id: "composite-nested".into(),
                descriptor: format!("file://./{}/composite-nested.yml", BASE_DIR).into(),
                configuration: Configuration::default(),
            },
            NodeDescriptor {
                id: "composite-outer-i".into(),
                descriptor: format!("file://./{}/composite-outer.yml", BASE_DIR).into(),
                configuration: Configuration::default(),
            },
        ],
        links: vec![
            LinkDescriptor::new(
                OutputDescriptor::new("composite-outer-i", "composite-outer-out"),
                InputDescriptor::new("composite-nested", "composite-nested-in"),
            ),
            LinkDescriptor::new(
                OutputDescriptor::new("composite-nested", "composite-nested-out"),
                InputDescriptor::new("composite-outer-o", "composite-outer-in"),
            ),
        ],
        configuration: Configuration::default(),
    };

    let (flattened_operators, flattened_links, patch) = nested_composite_descriptor
        .flatten_composite(
            "composite".into(),
            Configuration::default(),
            Vars::from([("SCHEME", "file://"), ("BASE_DIR", BASE_DIR)]),
            &mut HashSet::default(),
        )
        .expect("Unexpected error while calling `flatten`");

    // Important checks:
    // - operators in composite are prefixed with "composite/",
    // - operators in composite and composite-nested are prefixed with "composite/composite-nested".
    let expected_operators = vec![
        FlattenedOperatorDescriptor {
            id: "composite/composite-outer-o".into(),
            description: "composite-outer".into(),
            inputs: vec!["composite-outer-in".into()],
            outputs: vec!["composite-outer-out".into()],
            uri: Some("file://composite-outer.so".into()),
            configuration: Configuration::default(),
        },
        FlattenedOperatorDescriptor {
            id: "composite/composite-nested/operator-1".into(),
            description: "operator-1".into(),
            inputs: vec!["operator-1-in-1".into(), "operator-1-in-2".into()],
            outputs: vec!["operator-1-out".into()],
            uri: Some("file://operator-1.so".into()),
            configuration: Configuration::default(),
        },
        FlattenedOperatorDescriptor {
            id: "composite/composite-nested/operator-2".into(),
            description: "operator-2".into(),
            inputs: vec!["operator-2-in".into()],
            outputs: vec!["operator-2-out".into()],
            uri: Some("file://operator-2.so".into()),
            configuration: Configuration::default(),
        },
        FlattenedOperatorDescriptor {
            id: "composite/composite-outer-i".into(),
            description: "composite-outer".into(),
            inputs: vec!["composite-outer-in".into()],
            outputs: vec!["composite-outer-out".into()],
            uri: Some("file://composite-outer.so".into()),
            configuration: Configuration::default(),
        },
    ];

    assert_eq!(expected_operators, flattened_operators);

    // Important checks:
    // - operators in composite are prefixed with "composite/",
    // - operators in composite and composite-nested are prefixed with "composite/composite-nested".
    let expected_links = vec![
        LinkDescriptor::new(
            OutputDescriptor::new("composite/composite-outer-i", "composite-outer-out"),
            InputDescriptor::new("composite/composite-nested/operator-1", "operator-1-in-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite/composite-nested/operator-2", "operator-2-out"),
            InputDescriptor::new("composite/composite-outer-o", "composite-outer-in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite/composite-nested/operator-1", "operator-1-out"),
            InputDescriptor::new("composite/composite-nested/operator-2", "operator-2-in"),
        ),
    ];

    assert_eq!(expected_links, flattened_links);

    let expected_patch = Patch {
        subs_inputs: Substitutions::<InputDescriptor>::from([(
            InputDescriptor::new("composite", "composite-input"),
            InputDescriptor::new("composite/composite-outer-i", "composite-outer-in"),
        )]),
        subs_outputs: Substitutions::<OutputDescriptor>::from([(
            OutputDescriptor::new("composite", "composite-output"),
            OutputDescriptor::new("composite/composite-outer-o", "composite-outer-out"),
        )]),
    };

    assert_eq!(expected_patch, patch);
}
