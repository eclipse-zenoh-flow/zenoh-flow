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

use zenoh_flow_commons::Vars;

use crate::FlattenedDataFlowDescriptor;

#[test]
fn test_valid_data_flow() {
    let yaml_ok = r#"
name: data flow

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    assert!(FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_ok).unwrap(),
        Vars::default(),
    )
    .is_ok());
}

#[test]
fn test_no_source() {
    let yaml_no_source = r#"
name: invalid data flow no source

sources:

operators:
  - id: operator
    library: file:///home/zenoh-flow/liboperator.so
    inputs:
      - in-op
    outputs:
      - out-op

sinks:
  - id: sink
    library: file:///home/zenoh-flow/libsink.so
    inputs:
      - in-si

links:
  - from:
      node: operator
      output: out-op
    to:
      node: operator
      input: in-op

  - from:
      node: operator
      output: out-op
    to:
      node: sink-1
      input: sink-in
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_no_source).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("A data flow must specify at least ONE Source."));
}

#[test]
fn test_no_sink() {
    let yaml_no_sink = r#"
name: invalid data flow no sink

sources:
  - id: source
    library: file:///home/zenoh-flow/libsource.so
    outputs:
      - out-so

operators:
  - id: operator
    library: file:///home/zenoh-flow/liboperator.so
    inputs:
      - in-op
    outputs:
      - out-op

sinks:

links:
  - from:
      node: source
      output: out-so
    to:
      node: operator
      input: in-op

  - from:
      node: operator
      output: out-op
    to:
      node: operator
      input: in-op
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_no_sink).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("A data flow must specify at least ONE Sink."));
}

#[test]
fn test_duplicate_ids() {
    let yaml_duplicate_same_section = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source-1.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_same_section).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains(
        "Two nodes share the same identifier: < source-0 >. The identifiers must be unique."
    ));

    let yaml_duplicate_different_sections_1 = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

  - id: source-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_different_sections_1).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains(
        "Two nodes share the same identifier: < source-0 >. The identifiers must be unique."
    ));

    let yaml_duplicate_different_sections_2 = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: operator-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_different_sections_2).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains(
        "Two nodes share the same identifier: < operator-0 >. The identifiers must be unique."
    ));
}

#[test]
fn test_duplicate_ports() {
    let yaml_duplicate_inputs_operator = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_inputs_operator).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res)
        .contains("Node < operator-0 > declares the following input (at least) twice: < in-0 >"));

    let yaml_duplicate_inputs_sink = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_inputs_sink).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res)
        .contains("Node < sink-0 > declares the following input (at least) twice: < in-0 >"));

    let yaml_duplicate_outputs_operator = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_outputs_operator).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res)
        .contains("Node < operator-0 > declares the following output (at least) twice: < out-0 >"));

    let yaml_duplicate_outputs_source = r#"
name: duplicate

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_duplicate_outputs_source).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res)
        .contains("Node < source-0 > declares the following output (at least) twice: < out-0 >"));
}

#[test]
fn test_unknown_link() {
    let yaml_unknown_link_from_output = r#"
name: unknown link

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: ouuuuuut-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_unknown_link_from_output).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("Does the node < source-0 > exist?"));
    assert!(format!("{:?}", res).contains("Does it declare an output named < ouuuuuut-0 >?"));

    let yaml_unknown_link_from_node = r#"
name: unknown link

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: sooooooource-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_unknown_link_from_node).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("Does the node < sooooooource-0 > exist?"));
    assert!(format!("{:?}", res).contains("Does it declare an output named < out-0 >?"));

    let yaml_unknown_link_to_node = r#"
name: unknown link

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: siiiiiiiiiiiink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_unknown_link_to_node).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("Does the node < siiiiiiiiiiiink-0 > exist?"));
    assert!(format!("{:?}", res).contains("Does it declare an input named < in-0 >?"));

    let yaml_unknown_link_to_input = r#"
name: unknown link

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: iiiiiiiiiin-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_unknown_link_to_input).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("Does the node < sink-0 > exist?"));
    assert!(format!("{:?}", res).contains("Does it declare an input named < iiiiiiiiiin-0 >?"));
}

#[test]
fn test_port_not_connected() {
    let yaml_port_not_connected_output = r#"
name: port not connected

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0
      - out-1

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
    outputs:
      - out-0
      - out-1

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_port_not_connected_output).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("The following outputs are not connected:"));
    assert!(format!("{:?}", res).contains("- source-0: out-1"));
    assert!(format!("{:?}", res).contains("- operator-0: out-1"));

    let yaml_port_not_connected_input = r#"
name: port not connected

sources:
  - id: source-0
    description: my source
    library: file:///home/zenoh-flow/source.so
    outputs:
      - out-0

operators:
  - id: operator-0
    description: my operator
    library: file:///home/zenoh-flow/operator.so
    inputs:
      - in-0
      - in-1
    outputs:
      - out-0

sinks:
  - id: sink-0
    description: my sink
    library: file:///home/zenoh-flow/sink.so
    inputs:
      - in-0
      - in-1

links:
  - from:
      node: source-0
      output: out-0
    to:
      node: operator-0
      input: in-0
  - from:
      node: operator-0
      output: out-0
    to:
      node: sink-0
      input: in-0
"#;

    let res = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str(yaml_port_not_connected_input).unwrap(),
        Vars::default(),
    );

    assert!(res.is_err());
    assert!(format!("{:?}", res).contains("The following inputs are not connected:"));
    assert!(format!("{:?}", res).contains("- operator-0: in-1"));
    assert!(format!("{:?}", res).contains("- sink-0: in-1"));
}
