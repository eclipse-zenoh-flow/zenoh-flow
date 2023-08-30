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

use crate::model::descriptor::{
    CompositeInputDescriptor, CompositeOperatorDescriptor, CompositeOutputDescriptor,
    InputDescriptor, LinkDescriptor, NodeDescriptor, OperatorDescriptor, OutputDescriptor,
};

#[test]
fn test_flatten_composite_descriptor_non_nested() {
    let _ = env_logger::try_init();

    let composite_descriptor = CompositeOperatorDescriptor {
        id: "composite-test".into(),
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
                descriptor: "file://./src/model/descriptor/tests/operator-1.yml".into(),
                configuration: None,
            },
            NodeDescriptor {
                id: "my-operator-2".into(),
                descriptor: "file://./src/model/descriptor/tests/operator-2.yml".into(),
                configuration: None,
            },
        ],
        links: vec![LinkDescriptor::new(
            OutputDescriptor::new("my-operator-1", "operator-1-out"),
            InputDescriptor::new("my-operator-2", "operator-2-in"),
        )],
        configuration: None,
    };

    let mut flow_links = vec![
        LinkDescriptor::new(
            OutputDescriptor::new("outer", "out-1"),
            InputDescriptor::new("composite", "input-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("outer", "out-2"),
            InputDescriptor::new("composite", "input-2"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite", "output-1"),
            InputDescriptor::new("outer", "in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("other-composite", "output-1"),
            InputDescriptor::new("other-other-composite", "input-1"),
        ),
    ];

    let operators = async_std::task::block_on(async {
        composite_descriptor
            .flatten("composite".into(), &mut flow_links, None, &mut Vec::new())
            .await
    })
    .expect("Unexpected error while calling `flatten`");

    // Here we check that the id of the operator has been correctly updated:
    // - it should be prefixed with the name of the composite operator,
    // - a "/" should follow,
    // - the name of the operator as per how it’s written in the composite should be last.
    //
    // In this specific case, the names of the operators are:
    // - in their yaml descriptors: operator-1, operator-2
    // - in the composite descriptor: my-operator-1, my-operator-2
    //
    // So we should see "composite/my-operator-1" & "composite/my-operator-2".
    let expected_operators = vec![
        OperatorDescriptor {
            id: "composite/my-operator-1".into(),
            inputs: vec!["operator-1-in-1".into(), "operator-1-in-2".into()],
            outputs: vec!["operator-1-out".into()],
            uri: Some("file://operator-1.so".into()),
            configuration: None,
        },
        OperatorDescriptor {
            id: "composite/my-operator-2".into(),
            inputs: vec!["operator-2-in".into()],
            outputs: vec!["operator-2-out".into()],
            uri: Some("file://operator-2.so".into()),
            configuration: None,
        },
    ];

    // NOTE: This `assert_eq` also checks the order of the elements!
    assert_eq!(operators, expected_operators);

    // Here we check that the links have been correctly updated:
    // - the id of the composite operator has been replaced with the "new_id" (composite/operator),
    // - the inputs & outputs have been replaced with what was declared in the yaml file of the
    //   actual operator,
    // - the links inside the composite operator have been added,
    // - only the links of the concerned composite operator were updated.
    let expected_links = vec![
        LinkDescriptor::new(
            OutputDescriptor::new("outer", "out-1"),
            InputDescriptor::new("composite/my-operator-1", "operator-1-in-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("outer", "out-2"),
            InputDescriptor::new("composite/my-operator-1", "operator-1-in-2"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite/my-operator-2", "operator-2-out"),
            InputDescriptor::new("outer", "in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("other-composite", "output-1"),
            InputDescriptor::new("other-other-composite", "input-1"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite/my-operator-1", "operator-1-out"),
            InputDescriptor::new("composite/my-operator-2", "operator-2-in"),
        ),
    ];

    // NOTE: This `assert_eq` also checks the order of the elements!
    assert_eq!(expected_links, flow_links);
}

#[test]
fn test_flatten_composite_descriptor_nested() {
    let _ = env_logger::try_init();

    let nested_composite_descriptor = CompositeOperatorDescriptor {
        id: "nested-composite-test".into(),
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
                descriptor: "file://./src/model/descriptor/tests/composite-outer.yml".into(),
                configuration: None,
            },
            NodeDescriptor {
                id: "composite-nested".into(),
                descriptor: "file://./src/model/descriptor/tests/composite-nested.yml".into(),
                configuration: None,
            },
            NodeDescriptor {
                id: "composite-outer-i".into(),
                descriptor: "file://./src/model/descriptor/tests/composite-outer.yml".into(),
                configuration: None,
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
        configuration: None,
    };

    let mut flow_links = vec![
        LinkDescriptor::new(
            OutputDescriptor::new("outer", "out"),
            InputDescriptor::new("composite", "composite-input"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite", "composite-output"),
            InputDescriptor::new("outer", "in"),
        ),
    ];

    let operators = async_std::task::block_on(async {
        nested_composite_descriptor
            .flatten("composite".into(), &mut flow_links, None, &mut Vec::new())
            .await
    })
    .expect("Unexpected error while calling `flatten`");

    // Important checks:
    // - operators in composite are prefixed with "composite/",
    // - operators in composite and composite-nested are prefixed with "composite/composite-nested".
    let expected_operators = vec![
        OperatorDescriptor {
            id: "composite/composite-outer-o".into(),
            inputs: vec!["composite-outer-in".into()],
            outputs: vec!["composite-outer-out".into()],
            uri: Some("file://composite-outer.so".into()),
            configuration: None,
        },
        OperatorDescriptor {
            id: "composite/composite-nested/operator-1".into(),
            inputs: vec!["operator-1-in-1".into(), "operator-1-in-2".into()],
            outputs: vec!["operator-1-out".into()],
            uri: Some("file://operator-1.so".into()),
            configuration: None,
        },
        OperatorDescriptor {
            id: "composite/composite-nested/operator-2".into(),
            inputs: vec!["operator-2-in".into()],
            outputs: vec!["operator-2-out".into()],
            uri: Some("file://operator-2.so".into()),
            configuration: None,
        },
        OperatorDescriptor {
            id: "composite/composite-outer-i".into(),
            inputs: vec!["composite-outer-in".into()],
            outputs: vec!["composite-outer-out".into()],
            uri: Some("file://composite-outer.so".into()),
            configuration: None,
        },
    ];

    assert_eq!(expected_operators, operators);

    // Important checks:
    // - operators in composite are prefixed with "composite/",
    // - operators in composite and composite-nested are prefixed with "composite/composite-nested".
    let expected_links = vec![
        LinkDescriptor::new(
            OutputDescriptor::new("outer", "out"),
            InputDescriptor::new("composite/composite-outer-i", "composite-outer-in"),
        ),
        LinkDescriptor::new(
            OutputDescriptor::new("composite/composite-outer-o", "composite-outer-out"),
            InputDescriptor::new("outer", "in"),
        ),
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

    assert_eq!(expected_links, flow_links);
}
