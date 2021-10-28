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

use zenoh_flow::{model::dataflow::DataFlowDescriptor, ZFError};

static DESCRIPTOR_OK: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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

static DESCRIPTOR_KO_INVALID_YAML: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

links:
- from:
"#;

static DESCRIPTOR_KO_INVALID_JSON: &str = r#"
{"flow": "SimplePipeline",
"operators":[{
  "id" : "SumOperator",
    "uri": "file://./target/release/libsum_and_send.dylib",
    "inputs": [{"id": "Number","type": "usize"}],
    "outputs":[{"id": "Sum","type": "usize"}]
    }],
    "sources": [{"id" : "Counter",
    "uri": "file://./target/release/libcounter_source.dylib",
    "output":{"id": "Counter","type": "usize"}
    }],
    "sinks":[{"id" : "PrintSink","uri": file://./target/release/libgeneric_sink.dylib"}}}]
"#;

static DESCRIPTOR_KO_DIFFERENT_TYPES: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: isize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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

static DESCRIPTOR_KO_UNCONNECTED: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
      - id: Sub
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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

// static DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT: &str = r#"
// flow: SimplePipeline
// operators:
//   - id : SumOperator
//     uri: file://./target/release/libsum_and_send.dylib
//     inputs:
//       - id: Number
//         type: usize
//     outputs:
//       - id: Sum
//         type: usize
//       - id: Sum
//         type: usize
// sources:
//   - id : Counter
//     uri: file://./target/release/libcounter_source.dylib
//     output:
//       id: Counter
//       type: usize
// sinks:
//   - id : PrintSink
//     uri: file://./target/release/libgeneric_sink.dylib
//     input:
//       id: Data
//       type: usize
//   - id : PrintSink2
//     uri: file://./target/release/libgeneric_sink.dylib
//     input:
//       id: Data
//       type: usize

// links:
// - from:
//     node : Counter
//     output : Counter
//   to:
//     node : SumOperator
//     input : Number
// - from:
//     node : SumOperator
//     output : Sum
//   to:
//     node : PrintSink
//     input : Data
// - from:
//     node : SumOperator
//     output : Sum
//   to:
//     node : PrintSink2
//     input : Data
// "#;

static DESCRIPTOR_KO_DUPLICATED_INPUT_PORT: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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

static DESCRIPTOR_KO_DUPLICATED_NODE: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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

// static DESCRIPTOR_KO_DUPLICATED_CONNECTION: &str = r#"
// flow: SimplePipeline
// operators:
//   - id : SumOperator
//     uri: file://./target/release/libsum_and_send.dylib
//     inputs:
//       - id: Number
//         type: usize
//     outputs:
//       - id: Sum
//         type: usize
// sources:
//   - id : Counter
//     uri: file://./target/release/libcounter_source.dylib
//     output:
//       id: Counter
//       type: usize
// sinks:
//   - id : PrintSink
//     uri: file://./target/release/libgeneric_sink.dylib
//     input:
//       id: Data
//       type: usize
//   - id : PrintSink2
//     uri: file://./target/release/libgeneric_sink.dylib
//     input:
//       id: Data
//       type: usize

// links:
// - from:
//     node : Counter
//     output : Counter
//   to:
//     node : SumOperator
//     input : Number
// - from:
//     node : SumOperator
//     output : Sum
//   to:
//     node : PrintSink
//     input : Data
// - from:
//     node : SumOperator
//     output : Sum
//   to:
//     node : PrintSink2
//     input : Data
// "#;

static DESCRIPTOR_KO_PORT_NOT_FOUND: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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

static DESCRIPTOR_KO_NODE_NOT_FOUND: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    input:
      id: Data
      type: usize

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
fn validate_ok() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_OK);
    assert!(r.is_ok());
}

#[test]
fn validate_ko_unconnected() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_UNCONNECTED);
    let error = Err(ZFError::PortNotConnected((
        "SumOperator".into(),
        "Sub".into(),
    )));
    assert_eq!(r, error)
}

// #[test]
// fn validate_ko_duplicated_output() {
//     let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT);
//     let error = Err(ZFError::DuplicatedOutputPort((
//         "SumOperator".into(),
//         "Sum".into(),
//     )));
//     assert_eq!(r, error)
// }

#[test]
fn validate_ko_duplicated_input() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_INPUT_PORT);
    let error = Err(ZFError::DuplicatedInputPort((
        "SumOperator".into(),
        "Number".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_duplicated_node() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_NODE);
    let error = Err(ZFError::DuplicatedNodeId("Counter".into()));
    assert_eq!(r, error)
}

// #[test]
// fn validate_ko_duplicated_connection() {
//     let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_CONNECTION);
//     let error = Err(ZFError::DuplicatedConnection((
//         "SumOperator.Sum".into(),
//         "PrintSink.Data".into(),
//     )));
//     assert_eq!(r, error)
// }

#[test]
fn validate_ko_different_port_types() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DIFFERENT_TYPES);
    let error = Err(ZFError::PortTypeNotMatching((
        "isize".into(),
        "usize".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_port_not_found() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_PORT_NOT_FOUND);
    let error = Err(ZFError::PortNotFound((
        "SumOperator".into(),
        "Sum_typo".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_node_not_found() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_NODE_NOT_FOUND);
    let error = Err(ZFError::OperatorNotFound("SumOperator_typo".into()));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_invalid_yaml() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INVALID_YAML);
    assert!(matches!(r, Err(ZFError::ParsingError(_))))
}

#[test]
fn validate_ko_invalid_json() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INVALID_JSON);
    assert!(matches!(r, Err(ZFError::ParsingError(_))))
}
