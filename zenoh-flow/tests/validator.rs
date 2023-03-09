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

use zenoh_flow::model::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::prelude::ErrorKind;

static DESCRIPTOR_OK: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]

    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink
    input : Data
"#;

#[test]
fn validate_ok() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_OK);
    assert!(r.is_ok());
}

static DESCRIPTOR_KO_INVALID_YAML: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]

    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
"#;

#[test]
fn validate_ko_invalid_yaml() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INVALID_YAML);
    assert!(matches!(r.err().unwrap().into(), ErrorKind::ParsingError))
}

static DESCRIPTOR_KO_INVALID_JSON: &str = r#"
{"flow": "SimplePipeline",
"operators":[{
  "id" : "SumOperator",
    "uri": "file://./target/release/libsum_and_send.dylib",
    "inputs": ["Number"],
    "outputs":["Sum"]
    }],
    "sources": [{"id" : "Counter",
    "uri": "file://./target/release/libcounter_source.dylib",
    "outputs":["Counter"]
    }],
    "sinks":[{"id" : "PrintSink","uri": file://./target/release/libgeneric_sink.dylib"}}}]
"#;

#[test]
fn validate_ko_invalid_json() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INVALID_JSON);
    assert!(matches!(r.err().unwrap().into(), ErrorKind::ParsingError))
}

static DESCRIPTOR_KO_UNCONNECTED: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]

    outputs:
      - Sum
      - Sub

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink
    input : Data
"#;

#[test]
fn validate_ko_unconnected() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_UNCONNECTED);
    let error = ErrorKind::PortNotConnected(("SumOperator".into(), "Sub".into()));
    assert_eq!(ErrorKind::from(r.err().unwrap()), error)
}

static DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]

    outputs:
      - Sum
      - Sum

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]

  - id : PrintSink2
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink
    input : Data
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink2
    input : Data
"#;

#[test]
fn validate_ko_duplicated_output() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT);
    let error = ErrorKind::DuplicatedPort(("SumOperator".into(), "Sum".into()));
    assert_eq!(ErrorKind::from(r.err().unwrap()), error)
}

static DESCRIPTOR_KO_DUPLICATED_INPUT_PORT: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - Number
      - Number
    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink
    input : Data
"#;

#[test]
fn validate_ko_duplicated_input() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_INPUT_PORT);
    let error = ErrorKind::DuplicatedPort(("SumOperator".into(), "Number".into()));
    assert_eq!(ErrorKind::from(r.err().unwrap()), error)
}

static DESCRIPTOR_KO_DUPLICATED_NODE: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]

    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink
    input : Data
"#;

#[test]
fn validate_ko_duplicated_node() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_NODE);
    let error = ErrorKind::DuplicatedNodeId("Counter".into());
    assert_eq!(ErrorKind::from(r.err().unwrap()), error)
}

static DESCRIPTOR_OK_DUPLICATED_CONNECTION: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]
    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]

  - id : PrintSink2
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink
    input : Data
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink2
    input : Data
"#;

#[test]
fn validate_ok_duplicated_connection() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_OK_DUPLICATED_CONNECTION);
    assert!(r.is_ok())
}

static DESCRIPTOR_KO_PORT_NOT_FOUND: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]
    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator
    output : Sum_typo
  to:
    node : PrintSink
    input : Data
"#;

#[test]
fn validate_ko_port_not_found() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_PORT_NOT_FOUND);
    let error = ErrorKind::PortNotFound(("SumOperator".into(), "Sum_typo".into()));
    assert_eq!(ErrorKind::from(r.err().unwrap()), error)
}

static DESCRIPTOR_KO_NODE_NOT_FOUND: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs: [Number]

    outputs: [Sum]

sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    outputs: [Counter]

sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    inputs: [Data]


links:
- from:
    node : Counter
    output : Counter
  to:
    node : SumOperator
    input : Number
- from:
    node : SumOperator_typo
    output : Sum
  to:
    node : PrintSink
    input : Data
"#;

#[test]
fn validate_ko_node_not_found() {
    let _ = env_logger::try_init();
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_NODE_NOT_FOUND);
    let error = ErrorKind::NodeNotFound("SumOperator_typo".into());
    assert_eq!(ErrorKind::from(r.err().unwrap()), error)
}
