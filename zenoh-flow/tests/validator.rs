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

//            ┌───┐    ┌───┐    ┌───┐    ┌───┐
//  Source ──►│ A ├───►│ B ├───►│ C ├───►│ D ├──► Sink
//            └───┘    └───┘    └─┬─┘    └─┬─┘
//             ▲         ▲ finite │        │
//             │         └────────┘        │
//             │                           │
//             │      infinite             │
//             └───────────────────────────┘
static DESCRIPTOR_OK_LOOP: &str = r#"
flow: LoopOK
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id: C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any
- id: D
  uri: file://./D.dylib
  inputs:
    - id: in-D
      type: any
  outputs:
    - id: out-D
      type: any

sinks:
  - id : Sink
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
    node : D
    input : in-D
- from:
    node : D
    output : out-D
  to:
    node : Sink
    input : in-Sink

loops:
- ingress: A
  egress: D
  feedback_port: feedback-AD
  is_infinite: false
  port_type: any
- ingress: B
  egress: C
  feedback_port: feedback-BC
  is_infinite: true
  port_type: any
"#;

#[test]
fn validate_ok_loop() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_OK_LOOP);
    assert!(r.is_ok(), "Expecting ok, have err: {:?}", r)
}

// Operator A has two outputs, it cannot be an Ingress
static DESCRIPTOR_KO_LOOP_INGRESS_MULTIPLE_OUTPUTS: &str = r#"
flow: Loop-KO-Ingress-multiple outputs
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
    - id: out-A-error
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
    - id: in-B-error
      type: any
  outputs:
    - id: out-B
      type: any
- id: C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
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
    node : B
    input : in-B
- from:
    node : A
    output : out-A-error
  to:
    node : B
    input : in-B-error
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

loops:
- ingress: A
  egress: C
  feedback_port: feedback-AC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_loop_ingress_multiple_outputs() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_LOOP_INGRESS_MULTIPLE_OUTPUTS);
    assert!(
        r.is_err(),
        "Expecting err 'Ingress, multiple outputs', have: {:?}",
        r
    )
}

// Operator C has two outputs, it cannot be an Egress
static DESCRIPTOR_KO_LOOP_EGRESS_MULTIPLE_INPUTS: &str = r#"
flow: Loop-KO-Egress-multiple-inputs
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any
- id : Source-error
  uri: file://./source-error.dylib
  output:
    id: out-Source-error
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id: C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
    - id: in-C-error
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
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
- from:
    node : Source-error
    output : out-Source-error
  to:
    node : C
    input : in-C-error

loops:
- ingress: A
  egress: C
  feedback_port: feedback-AC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_loop_egress_multiple_inputs() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_LOOP_EGRESS_MULTIPLE_INPUTS);
    assert!(
        r.is_err(),
        "Expecting err 'Egress, multiple inputs', have: {:?}",
        r
    )
}

// Operator A already has an input port called "feedback-AC".
static DESCRIPTOR_KO_LOOP_INGRESS_PORT_ALREADY_EXISTS: &str = r#"
flow: Loop-KO-Ingress-port-already-exists
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
    - id: feedback-AC
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id: C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
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
    node : Source
    output : out-Source
  to:
    node : A
    input : feedback-AC
- from:
    node : A
    output : out-A
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

loops:
- ingress: A
  egress: C
  feedback_port: feedback-AC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_loop_ingress_port_already_taken() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_LOOP_INGRESS_PORT_ALREADY_EXISTS);
    assert!(
        r.is_err(),
        "Expecting err 'Ingress, port already exists', have: {:?}",
        r
    )
}

// Operator C already has an output port called "feedback-AC".
static DESCRIPTOR_KO_LOOP_EGRESS_PORT_ALREADY_EXISTS: &str = r#"
flow: Loop-KO-Egress-port-already-exists
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id: C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any
    - id: feedback-AC
      type: any

sinks:
  - id : Sink
    uri: file://./sink.dylib
    input:
      id: in-Sink
      type: any
  - id : Sink-2
    uri: file://./sink.dylib
    input:
      id: in-Sink-2
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
- from:
    node : C
    output : feedback-AC
  to:
    node : Sink-2
    input : in-Sink-2

loops:
- ingress: A
  egress: C
  feedback_port: feedback-AC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_loop_egress_port_already_exists() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_LOOP_EGRESS_PORT_ALREADY_EXISTS);
    assert!(
        r.is_err(),
        "Expecting err 'Egress, port already exists', have: {:?}",
        r
    )
}

static DESCRIPTOR_KO_LOOP_INGRESS_MUST_BE_OPERATOR: &str = r#"
flow: Loop-KO-Ingress-not-operator
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any

sinks:
  - id : Sink
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

loops:
- ingress: Source
  egress: A
  feedback_port: feedback-Source-A
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_loop_ingress_must_be_operator() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_LOOP_INGRESS_MUST_BE_OPERATOR);
    assert!(
        r.is_err(),
        "Expecting err 'Ingress must be an Operator', have: {:?}",
        r
    )
}

static DESCRIPTOR_KO_LOOP_EGRESS_MUST_BE_OPERATOR: &str = r#"
flow: Loop-KO-Egress-not-operator
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any

sinks:
  - id : Sink
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

loops:
- ingress: A
  egress: Sink
  feedback_port: feedback-A-Sink
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_loop_egress_must_be_operator() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_LOOP_EGRESS_MUST_BE_OPERATOR);
    assert!(
        r.is_err(),
        "Expecting err 'Egress must be an Operator', have: {:?}",
        r
    )
}

static DESCRIPTOR_KO_CYCLE_OUTSIDE_LOOPS_SECTION: &str = r#"
flow: Loop-KO-Egress-not-operator
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
    - id: feedback-AB
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
    - id: feedback-AB
      type: any

sinks:
  - id : Sink
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
    node : B
    input : in-B
- from:
    node : B
    output : feedback-AB
  to:
    node : A
    input : feedback-AB
- from:
    node : B
    output : out-B
  to:
    node : Sink
    input : in-Sink
"#;

#[test]
fn validate_ko_cycle_outside_loops_section() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_CYCLE_OUTSIDE_LOOPS_SECTION);
    assert!(
        r.is_err(),
        "Expecting err 'The dataflow contains a cycle, please use the `Loops` section to express this behavior.', have: {:?}",
        r
    )
}

static DESCRIPTOR_KO_INGRESS_REUSED: &str = r#"
flow: Loop-KO-Ingress-reused
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id : C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
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

loops:
- ingress: A
  egress: B
  feedback_port: feedback-AB
  is_infinite: false
  port_type: any
- ingress: A
  egress: C
  feedback_port: feedback-AC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_ingress_reused() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_INGRESS_REUSED);
    assert!(
        r.is_err(),
        "Expecting err 'Ingress < A > is already used in another loop', have: {:?}",
        r
    )
}

static DESCRIPTOR_KO_EGRESS_REUSED: &str = r#"
flow: Loop-KO-Egress-reused
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id : C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
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

loops:
- ingress: A
  egress: C
  feedback_port: feedback-AC
  is_infinite: false
  port_type: any
- ingress: B
  egress: C
  feedback_port: feedback-BC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_egress_reused() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_EGRESS_REUSED);
    assert!(
        r.is_err(),
        "Expecting err 'Egress < C > is already used in another loop', have: {:?}",
        r
    )
}

static DESCRIPTOR_KO_EGRESS_REUSED_AS_INGRESS: &str = r#"
flow: Loop-KO-Egress-reused-as-ingress
sources:
- id : Source
  uri: file://./source.dylib
  output:
    id: out-Source
    type: any

operators:
- id : A
  uri: file://./A.dylib
  inputs:
    - id: in-A
      type: any
  outputs:
    - id: out-A
      type: any
- id : B
  uri: file://./B.dylib
  inputs:
    - id: in-B
      type: any
  outputs:
    - id: out-B
      type: any
- id : C
  uri: file://./C.dylib
  inputs:
    - id: in-C
      type: any
  outputs:
    - id: out-C
      type: any

sinks:
  - id : Sink
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

loops:
- ingress: A
  egress: B
  feedback_port: feedback-AB
  is_infinite: false
  port_type: any
- ingress: B
  egress: C
  feedback_port: feedback-BC
  is_infinite: false
  port_type: any
"#;

#[test]
fn validate_ko_egress_reused_as_ingress() {
    let r = DataFlowDescriptor::from_yaml(DESCRIPTOR_KO_EGRESS_REUSED_AS_INGRESS);
    assert!(
        r.is_err(),
        "Expecting err 'Ingress < B > is already used in another loop', have: {:?}",
        r
    )
}
