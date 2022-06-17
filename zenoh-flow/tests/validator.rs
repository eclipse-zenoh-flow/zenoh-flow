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

use zenoh_flow::{model::dataflow::descriptor::FlattenDataFlowDescriptor, ZFError};

static DESCRIPTOR_OK: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
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
    tags: []
    uri: file://./target/release/libsum_and_send.dylib
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: isize
sources:
  - id : Counter
    tags: []
    uri: file://./target/release/libcounter_source.dylib
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    tags: []
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
    tags: []
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
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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

static DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
    input:
      id: Data
      type: usize
  - id : PrintSink2
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink2
    input : Data
"#;

static DESCRIPTOR_KO_DUPLICATED_INPUT_PORT: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    tags: []
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
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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

static DESCRIPTOR_OK_DUPLICATED_CONNECTION: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
    input:
      id: Data
      type: usize
  - id : PrintSink2
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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
- from:
    node : SumOperator
    output : Sum
  to:
    node : PrintSink2
    input : Data
"#;

static DESCRIPTOR_KO_PORT_NOT_FOUND: &str = r#"
flow: SimplePipeline
operators:
  - id : SumOperator
    uri: file://./target/release/libsum_and_send.dylib
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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
    tags: []
    inputs:
      - id: Number
        type: usize
    outputs:
      - id: Sum
        type: usize
sources:
  - id : Counter
    uri: file://./target/release/libcounter_source.dylib
    tags: []
    output:
      id: Counter
      type: usize
sinks:
  - id : PrintSink
    uri: file://./target/release/libgeneric_sink.dylib
    tags: []
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
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_OK);
    assert!(r.is_ok());
}

#[test]
fn validate_ko_unconnected() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_UNCONNECTED);
    let error = Err(ZFError::PortNotConnected((
        "SumOperator".into(),
        "Sub".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_duplicated_output() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT);
    let error = Err(ZFError::DuplicatedPort((
        "SumOperator".into(),
        "Sum".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_duplicated_input() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_INPUT_PORT);
    let error = Err(ZFError::DuplicatedPort((
        "SumOperator".into(),
        "Number".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_duplicated_node() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_NODE);
    let error = Err(ZFError::DuplicatedNodeId("Counter".into()));
    assert_eq!(r, error)
}

#[test]
fn validate_ok_duplicated_connection() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_OK_DUPLICATED_CONNECTION);
    assert!(r.is_ok())
}

#[test]
fn validate_ko_different_port_types() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DIFFERENT_TYPES);
    let error = Err(ZFError::PortTypeNotMatching((
        "isize".into(),
        "usize".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_port_not_found() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_PORT_NOT_FOUND);
    let error = Err(ZFError::PortNotFound((
        "SumOperator".into(),
        "Sum_typo".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_node_not_found() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_NODE_NOT_FOUND);
    let error = Err(ZFError::NodeNotFound("SumOperator_typo".into()));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_invalid_yaml() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INVALID_YAML);
    assert!(matches!(r, Err(ZFError::ParsingError(_))))
}

#[test]
fn validate_ko_invalid_json() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INVALID_JSON);
    assert!(matches!(r, Err(ZFError::ParsingError(_))))
}

/*
 *
 * Flags
 *
 */

static DESCRIPTOR_OK_FLAGS: &str = r#"
flow: DESCRIPTOR_OK_FLAGS

flags:
- id: flag-A
  toggle: true
  nodes:
    - A
- id: flag-B-C
  toggle: false
  nodes:
    - B
    - C

sources:
- id : Source
  uri: file://./source.dylib
  tags: []
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  tags: []
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  tags: []
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id : C
  uri: file://./C.dylib
  tags: []
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
    uri: file://./sink.dylib
    tags: []
    input:
      id: in-Sink
      type: any

links:
# Flag: A
- from:
    node : Source
    output : out-Source
  to:
    node : A
    input : in-A
- from:
    node : A
    output : out-A
  to:
    node : Sink
    input : in-Sink
# Flag: B-C
- from:
    node : Source
    output : out-Source
  to:
    node : B
    input : in-B
- from:
    node : B
    output : out-B
  to:
    node : C
    input : in-C
- from:
    node : C
    output : out-C
  to:
    node : Sink
    input : in-Sink
"#;

#[test]
fn validate_ok_flags() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_OK_FLAGS);
    assert!(r.is_ok(), "Expecting no error, have: {:?}", r)
}

static DESCRIPTOR_KO_FLAGS_NODE_NOT_FOUND: &str = r#"
flow: DESCRIPTOR_KO_FLAGS_NODE_NOT_FOUND

flags:
- id: flag-A
  toggle: true
  nodes:
    - A
    - D

sources:
- id : Source
  uri: file://./source.dylib
  tags: []
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  tags: []
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any

sinks:
  - id : Sink
    tags: []
    uri: file://./sink.dylib
    input:
      id: in-Sink
      type: any

links:
- from:
    node : Source
    output : out-Source
  to:
    node : A
    input : in-A
- from:
    node : A
    output : out-A
  to:
    node : Sink
    input : in-Sink
"#;

#[test]
fn validate_ko_flags_node_not_found() {
    let r = FlattenDataFlowDescriptor::from_yaml(DESCRIPTOR_KO_FLAGS_NODE_NOT_FOUND);
    assert!(
        r.is_err(),
        "Expecting error 'Err(NodeNotFound(D))', have: {:?}",
        r
    )
}
