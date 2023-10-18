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
    FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor, InputDescriptor, LinkDescriptor, OutputDescriptor,
};
use std::collections::HashMap;
use zenoh_flow_commons::{
    Configuration, NodeId, PortId, RuntimeContext, RuntimeId, SharedMemoryConfiguration,
    SharedMemoryParameters,
};
use zenoh_flow_records::{InputRecord, LinkRecord, OutputRecord};

const SOURCE_ID: &str = "source-0";
const SOURCE_OUTPUT: &str = "out-operator-1";

const OPERATOR_ID: &str = "operator-1";
const OPERATOR_INPUT: &str = "in-source-0";
const OPERATOR_OUTPUT: &str = "out-sink-2";

const SINK_ID: &str = "sink-2";
const SINK_INPUT: &str = "in-operator-1";

const RUNTIME_ID: &str = "default-runtime";

fn generate_default_runtime() -> RuntimeContext {
    RuntimeContext {
        id: RUNTIME_ID.into(),
        shared_memory: SharedMemoryParameters {
            number_elements: 10,
            element_size: 10_000,
            backoff: 500,
        },
    }
}

fn generate_flattened_data_flow_descriptor() -> FlattenedDataFlowDescriptor {
    let source = FlattenedSourceDescriptor {
        id: SOURCE_ID.into(),
        name: "Source".into(),
        uri: Some("file:///home/zenoh-flow/source.so".into()),
        outputs: vec![SOURCE_OUTPUT.into()],
        configuration: Configuration::default(),
    };

    let operator = FlattenedOperatorDescriptor {
        id: OPERATOR_ID.into(),
        name: "Operator".into(),
        inputs: vec![OPERATOR_INPUT.into()],
        outputs: vec![OPERATOR_OUTPUT.into()],
        uri: Some("file:///home/zenoh-flow/operator.so".into()),
        configuration: Configuration::default(),
    };

    let sink = FlattenedSinkDescriptor {
        id: SINK_ID.into(),
        name: "Sink".into(),
        inputs: vec![SINK_INPUT.into()],
        uri: Some("file:///home/zenoh-flow/sink.so".into()),
        configuration: Configuration::default(),
    };

    let links = vec![
        LinkDescriptor {
            from: OutputDescriptor {
                node: SOURCE_ID.into(),
                output: SOURCE_OUTPUT.into(),
            },
            to: InputDescriptor {
                node: OPERATOR_ID.into(),
                input: OPERATOR_INPUT.into(),
            },
            shared_memory: SharedMemoryConfiguration::default(),
        },
        LinkDescriptor {
            from: OutputDescriptor {
                node: OPERATOR_ID.into(),
                output: OPERATOR_OUTPUT.into(),
            },
            to: InputDescriptor {
                node: SINK_ID.into(),
                input: SINK_INPUT.into(),
            },
            shared_memory: SharedMemoryConfiguration::default(),
        },
    ];

    FlattenedDataFlowDescriptor {
        flow: "test-flow".into(),
        sources: vec![source],
        operators: vec![operator],
        sinks: vec![sink],
        links,
        mapping: HashMap::default(),
    }
}

#[test]
fn test_complete_mapping_and_connect_no_runtime() {
    let default_runtime = generate_default_runtime();
    let flattened_descriptor = generate_flattened_data_flow_descriptor();

    let data_flow_record = flattened_descriptor.complete_mapping_and_connect(default_runtime);
    assert!(data_flow_record.receivers.is_empty());
    assert!(data_flow_record.senders.is_empty());

    let expected_mapping = HashMap::from([
        (SOURCE_ID.into(), RUNTIME_ID.into()),
        (OPERATOR_ID.into(), RUNTIME_ID.into()),
        (SINK_ID.into(), RUNTIME_ID.into()),
    ]);
    assert_eq!(expected_mapping, data_flow_record.mapping);
}

#[test]
fn test_complete_mapping_and_connect_all_different_runtime() {
    let default_runtime = generate_default_runtime();
    let mut flattened_descriptor = generate_flattened_data_flow_descriptor();

    let source_runtime: RuntimeId = "source-runtime".into();
    let sink_runtime: RuntimeId = "sink-runtime".into();

    // We only indicate a runtime for the Source and the Sink. The Operator will be on the "default-runtime".
    flattened_descriptor.mapping = HashMap::from([
        (SOURCE_ID.into(), source_runtime.clone()),
        (SINK_ID.into(), sink_runtime.clone()),
    ]);

    let data_flow_record =
        flattened_descriptor.complete_mapping_and_connect(default_runtime.clone());
    // 2 = sender(source-runtime) + sender(default-runtime)
    assert_eq!(data_flow_record.senders.len(), 2);
    // 2 = receiver(default-runtime) + receiver(sink-runtime)
    assert_eq!(data_flow_record.receivers.len(), 2);

    // 4 = source(source-runtime) -> sender(source-runtime)
    //     + receiver(default-runtime) -> operator(default-runtime)
    //     + operator(default-runtime) -> sender(default-runtime)
    //     + receiver(sink-runtime) -> sink(sink-runtime)
    let source_operator_sender_id: NodeId =
        format!("z-sender/{}/{}", SOURCE_ID, SOURCE_OUTPUT).into();
    let source_operator_sender_input: PortId =
        format!("z-sender/{}/{}-input", SOURCE_ID, SOURCE_OUTPUT).into();
    let source_operator_receiver_id: NodeId =
        format!("z-receiver/{}/{}", OPERATOR_ID, OPERATOR_INPUT).into();
    let source_operator_receiver_output: PortId =
        format!("z-receiver/{}/{}-output", OPERATOR_ID, OPERATOR_INPUT).into();

    let operator_sink_sender_id: NodeId =
        format!("z-sender/{}/{}", OPERATOR_ID, OPERATOR_OUTPUT).into();
    let operator_sink_sender_input: PortId =
        format!("z-sender/{}/{}-input", OPERATOR_ID, OPERATOR_OUTPUT).into();
    let operator_sink_receiver_id: NodeId = format!("z-receiver/{}/{}", SINK_ID, SINK_INPUT).into();
    let operator_sink_receiver_output: PortId =
        format!("z-receiver/{}/{}-output", SINK_ID, SINK_INPUT).into();

    let expected_links = vec![
        LinkRecord {
            from: OutputRecord {
                node: SOURCE_ID.into(),
                output: SOURCE_OUTPUT.into(),
            },
            to: InputRecord {
                node: source_operator_sender_id.clone(),
                input: source_operator_sender_input,
            },
            shared_memory: default_runtime.shared_memory.clone(),
        },
        LinkRecord {
            from: OutputRecord {
                node: source_operator_receiver_id.clone(),
                output: source_operator_receiver_output,
            },
            to: InputRecord {
                node: OPERATOR_ID.into(),
                input: OPERATOR_INPUT.into(),
            },
            shared_memory: default_runtime.shared_memory.clone(),
        },
        LinkRecord {
            from: OutputRecord {
                node: OPERATOR_ID.into(),
                output: OPERATOR_OUTPUT.into(),
            },
            to: InputRecord {
                node: operator_sink_sender_id,
                input: operator_sink_sender_input,
            },
            shared_memory: default_runtime.shared_memory.clone(),
        },
        LinkRecord {
            from: OutputRecord {
                node: operator_sink_receiver_id,
                output: operator_sink_receiver_output,
            },
            to: InputRecord {
                node: SINK_ID.into(),
                input: SINK_INPUT.into(),
            },
            shared_memory: default_runtime.shared_memory,
        },
    ];
    assert_eq!(expected_links.len(), data_flow_record.links.len());
    expected_links
        .iter()
        .for_each(|expected_link| assert!(data_flow_record.links.contains(expected_link)));

    let expected_mapping = HashMap::from([
        (SOURCE_ID.into(), source_runtime.clone()),
        (source_operator_sender_id, source_runtime),
        (source_operator_receiver_id, default_runtime.id.clone()),
        (OPERATOR_ID.into(), default_runtime.id.clone()),
        (
            format!("z-sender/{}/{}", OPERATOR_ID, OPERATOR_OUTPUT).into(),
            default_runtime.id,
        ),
        (
            format!("z-receiver/{}/{}", SINK_ID, SINK_INPUT).into(),
            sink_runtime.clone(),
        ),
        (SINK_ID.into(), sink_runtime),
    ]);
    assert_eq!(expected_mapping, data_flow_record.mapping);
}
