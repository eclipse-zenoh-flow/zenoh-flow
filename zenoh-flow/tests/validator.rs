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

use zenoh_flow::{model::dataflow::descriptor::DataFlowDescriptor, ZFError};

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

static DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT: &str = r#"
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
  - id : PrintSink2
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

static DESCRIPTOR_OK_DUPLICATED_CONNECTION: &str = r#"
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
  - id : PrintSink2
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

#[test]
fn validate_ko_duplicated_output() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_OUTPUT_PORT);
    let error = Err(ZFError::DuplicatedPort((
        "SumOperator".into(),
        "Sum".into(),
    )));
    assert_eq!(r, error)
}

#[test]
fn validate_ko_duplicated_input() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DUPLICATED_INPUT_PORT);
    let error = Err(ZFError::DuplicatedPort((
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

#[test]
fn validate_ok_duplicated_connection() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_OK_DUPLICATED_CONNECTION);
    assert!(r.is_ok())
}

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
    let error = Err(ZFError::NodeNotFound("SumOperator_typo".into()));
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

// Visual representation of the dataflow:
//
//          out   in           out1     in
// ┌────────┐      ┌───────────┐        ┌───────┐
// │ Source ├─────►│ Operator1 ├───────►│ Sink1 │
// └────────┘      └───────┬───┘        └───────┘
//                     out2│
//                         │            ┌───────────┐          ┌───────┐
//                         └───────────►│ Operator2 ├─────────►│ Sink2 │
//                                      └───────────┘          └───────┘
//                                    in            out        in
//
// (drawing made with: https://asciiflow.com/)
static DESCRIPTOR_KO_DEADLINE_NO_PATH: &str = r#"
flow: DeadlineNoPath
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out
    type: usize

operators:
- id : Operator1
  uri: file://./operator1.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out1
      type: usize
    - id: out2
      type: usize
- id : Operator2
  uri: file://./operator1.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out
      type: usize
sinks:
  - id : Sink1
    uri: file://./sink1.dylib
    input:
      id: in
      type: usize
  - id : Sink2
    uri: file://./sink2.dylib
    input:
      id: in
      type: usize

links:
- from:
    node : Source
    output : out
  to:
    node : Operator1
    input : in
- from:
    node : Operator1
    output : out1
  to:
    node : Sink1
    input : in
- from:
    node : Operator1
    output : out2
  to:
    node : Operator2
    input : in
- from:
    node : Operator2
    output : out
  to:
    node : Sink2
    input : in

deadlines:
- from:
    node: Operator1
    output: out1
  to:
    node: Sink2
    input: in
  duration:
    length: 500
    unit: ms
"#;

#[test]
fn validate_ko_deadline_no_path() {
    // There is no path between Operator1.out1 and Sink2.in.
    //
    // There is, however, a path between Operator1.out2 and Sink2.in.
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_DEADLINE_NO_PATH);
    assert_eq!(
        r,
        Err(ZFError::NoPathBetweenNodes((
            ("Operator1".into(), "out1".into()),
            ("Sink2".into(), "in".into())
        )))
    )
}

// Visual representation of the dataflow:
//
//          out   in           out1     in
// ┌────────┐      ┌───────────┐        ┌───────┐
// │ Source ├─────►│ Operator1 ├───────►│ Sink1 │
// └────────┘      └───────┬───┘        └───────┘
//                     out2│
//                         │            ┌───────────┐          ┌───────┐
//                         └───────────►│ Operator2 ├─────────►│ Sink2 │
//                                      └───────────┘          └───────┘
//                                    in            out        in
//
// (drawing made with: https://asciiflow.com/)
static DESCRIPTOR_OK_DEADLINE: &str = r#"
flow: DeadlineNoPath
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out
    type: usize

operators:
- id : Operator1
  uri: file://./operator1.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out1
      type: usize
    - id: out2
      type: usize
- id : Operator2
  uri: file://./operator1.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out
      type: usize
sinks:
  - id : Sink1
    uri: file://./sink1.dylib
    input:
      id: in
      type: usize
  - id : Sink2
    uri: file://./sink2.dylib
    input:
      id: in
      type: usize

links:
- from:
    node : Source
    output : out
  to:
    node : Operator1
    input : in
- from:
    node : Operator1
    output : out1
  to:
    node : Sink1
    input : in
- from:
    node : Operator1
    output : out2
  to:
    node : Operator2
    input : in
- from:
    node : Operator2
    output : out
  to:
    node : Sink2
    input : in

deadlines:
- from:
    node: Operator1
    output: out2
  to:
    node: Sink2
    input: in
  duration:
    length: 500
    unit: ms
- from:
    node: Source
    output: out
  to:
    node: Sink2
    input: in
  duration:
    length: 1000
    unit: ms
"#;

#[test]
fn validate_ok_deadline() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_OK_DEADLINE);
    assert!(r.is_ok())
}

// Visual representation of the dataflow:
//
//          out   in         out      in
// ┌────────┐      ┌──────────┐        ┌──────┐
// │ Source ├─────►│ Operator ├───────►│ Sink │
// └────────┘      └──────────┘        └──────┘
//
// (drawing made with: https://asciiflow.com/)
static DESCRIPTOR_KO_SELF_DEADLINE: &str = r#"
flow: DeadlineNoPath
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out
    type: usize

operators:
- id : Operator
  uri: file://./operator.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out
      type: usize

sinks:
  - id : Sink
    uri: file://./sink.dylib
    input:
      id: in
      type: usize

links:
- from:
    node : Source
    output : out
  to:
    node : Operator
    input : in
- from:
    node : Operator
    output : out
  to:
    node : Sink
    input : in

deadlines:
- from:
    node: Operator
    output: out
  to:
    node: Operator
    input: in
  duration:
    length: 500
    unit: ms
"#;

#[test]
fn validate_ko_self_deadline() {
    // There is no path between Operator1.out1 and Sink2.in.
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_SELF_DEADLINE);
    assert_eq!(
        r,
        Err(ZFError::NoPathBetweenNodes((
            ("Operator".into(), "out".into()),
            ("Operator".into(), "in".into())
        )))
    )
}

// Visual representation of the dataflow:
//
//          out   in         out      in           out      in
// ┌────────┐      ┌───────────┐        ┌───────────┐        ┌──────┐
// │ Source ├─────►│ Operator1 ├───────►│ Operator2 ├───────►│ Sink │
// └────────┘      └───────────┘        └───────────┘        └──────┘
//
// (drawing made with: https://asciiflow.com/)
static DESCRIPTOR_KO_REVERSE_DEADLINE: &str = r#"
flow: DeadlineNoPath
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out
    type: usize

operators:
- id : Operator1
  uri: file://./operator1.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out
      type: usize
- id : Operator2
  uri: file://./operator2.dylib
  inputs:
    - id: in
      type: usize
  outputs:
    - id: out
      type: usize

sinks:
  - id : Sink
    uri: file://./sink.dylib
    input:
      id: in
      type: usize

links:
- from:
    node : Source
    output : out
  to:
    node : Operator1
    input : in
- from:
    node : Operator1
    output : out
  to:
    node : Operator2
    input : in
- from:
    node : Operator2
    output : out
  to:
    node : Sink
    input : in

deadlines:
- from:
    node: Operator2
    output: out
  to:
    node: Operator1
    input: in
  duration:
    length: 500
    unit: ms
"#;

#[test]
fn validate_ko_reverse_deadline() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_REVERSE_DEADLINE);
    assert_eq!(
        r,
        Err(ZFError::NoPathBetweenNodes((
            ("Operator2".into(), "out".into()),
            ("Operator1".into(), "in".into())
        )))
    )
}
