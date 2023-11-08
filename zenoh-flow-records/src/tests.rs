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
    dataflow::{RECEIVER_SUFFIX, SENDER_SUFFIX},
    DataFlowRecord, ReceiverRecord, SenderRecord,
};
use uuid::Uuid;
use zenoh_flow_commons::{NodeId, RuntimeId, Vars};
use zenoh_flow_descriptors::{
    DataFlowDescriptor, FlattenedDataFlowDescriptor, InputDescriptor, LinkDescriptor,
    OutputDescriptor,
};
use zenoh_keyexpr::OwnedKeyExpr;

#[test]
fn test_success_no_runtime() {
    let flow: &str = r#"
name: base test flow

sources:
  - id: source-0
    description: test source
    library: file:///home/zenoh-flow/libsource.so
    outputs:
      - out-0

operators:
  - id: operator-1
    description: test operator
    library: file:///home/zenoh-flow/liboperator.so
    inputs:
      - in-1
    outputs:
      - out-1

sinks:
  - id: sink-2
    description: test sink
    library: file:///home/zenoh-flow/libsink.so
    inputs:
      - in-2

links:
  - from:
     node: source-0
     output: out-0
    to:
     node: operator-1
     input: in-1

  - from:
     node: operator-1
     output: out-1
    to:
     node: sink-2
     input: in-2
"#;

    let flat_desc = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str::<DataFlowDescriptor>(flow).unwrap(),
        Vars::default(),
    )
    .unwrap();

    let default_runtime: RuntimeId = Uuid::new_v4().into();
    let record = DataFlowRecord::try_new(flat_desc, &default_runtime).unwrap();

    assert!(record.receivers.is_empty());
    assert!(record.senders.is_empty());
    assert_eq!(2, record.links.len());
}

#[test]
fn test_success_same_runtime() {
    let runtime: RuntimeId = Uuid::new_v4().into();
    let flow = format!(
        r#"
name: base test flow

sources:
  - id: source-0
    description: test source
    library: file:///home/zenoh-flow/libsource.so
    outputs:
      - out-0
    runtime: {0}

operators:
  - id: operator-1
    description: test operator
    library: file:///home/zenoh-flow/liboperator.so
    inputs:
      - in-1
    outputs:
      - out-1
    runtime: {0}

sinks:
  - id: sink-2
    description: test sink
    library: file:///home/zenoh-flow/libsink.so
    inputs:
      - in-2
    runtime: {0}

links:
  - from:
     node: source-0
     output: out-0
    to:
     node: operator-1
     input: in-1

  - from:
     node: operator-1
     output: out-1
    to:
     node: sink-2
     input: in-2
"#,
        runtime
    );

    let flat_desc = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str::<DataFlowDescriptor>(&flow).unwrap(),
        Vars::default(),
    )
    .unwrap();

    let record = DataFlowRecord::try_new(flat_desc, &Uuid::new_v4().into()).unwrap();

    assert!(record.receivers.is_empty());
    assert!(record.senders.is_empty());
    assert_eq!(2, record.links.len());
}

#[test]
fn test_success_different_runtime() {
    let runtime_thing: RuntimeId = Uuid::new_v4().into();
    let runtime_edge: RuntimeId = Uuid::new_v4().into();
    let default_runtime: RuntimeId = Uuid::new_v4().into();

    let desc = format!(
        r#"
name: base test flow

sources:
  - id: source-0
    description: test source
    library: file:///home/zenoh-flow/libsource.so
    outputs:
      - out-0
    runtime: {}

operators:
  - id: operator-1
    description: test operator
    library: file:///home/zenoh-flow/liboperator.so
    inputs:
      - in-1
    outputs:
      - out-1
    runtime: {}

sinks:
  - id: sink-2
    description: test sink
    library: file:///home/zenoh-flow/libsink.so
    inputs:
      - in-2

links:
  - from:
     node: source-0
     output: out-0
    to:
     node: operator-1
     input: in-1

  - from:
     node: operator-1
     output: out-1
    to:
     node: sink-2
     input: in-2
"#,
        runtime_thing, runtime_edge
    );

    let flat_desc = FlattenedDataFlowDescriptor::try_flatten(
        serde_yaml::from_str::<DataFlowDescriptor>(&desc).unwrap(),
        Vars::default(),
    )
    .unwrap();

    let record = DataFlowRecord::try_new(flat_desc, &default_runtime).unwrap();
    assert_eq!(2, record.receivers.len());
    assert_eq!(2, record.senders.len());
    assert_eq!(4, record.links.len());

    // assert the connectors
    let key_expr_thing_edge =
        OwnedKeyExpr::autocanonize(format!("{}/source-0/out-0", record.record_id)).unwrap();
    let sender_thing_edge: NodeId = format!("source-0{}", SENDER_SUFFIX).into();
    let receiver_thing_edge: NodeId = format!("operator-1{}", RECEIVER_SUFFIX).into();
    assert_eq!(
        Some(&SenderRecord {
            id: sender_thing_edge.clone(),
            resource: key_expr_thing_edge.clone(),
            runtime: runtime_thing,
        }),
        record.senders.get(&sender_thing_edge)
    );
    assert_eq!(
        Some(&ReceiverRecord {
            id: receiver_thing_edge.clone(),
            resource: key_expr_thing_edge.clone(),
            runtime: runtime_edge.clone(),
        }),
        record.receivers.get(&receiver_thing_edge)
    );

    let key_expr_edge_default =
        OwnedKeyExpr::autocanonize(format!("{}/operator-1/out-1", record.record_id)).unwrap();
    let sender_edge_default: NodeId = format!("operator-1{}", SENDER_SUFFIX).into();
    let receiver_edge_default: NodeId = format!("sink-2{}", RECEIVER_SUFFIX).into();
    assert_eq!(
        Some(&SenderRecord {
            id: sender_edge_default.clone(),
            resource: key_expr_edge_default.clone(),
            runtime: runtime_edge,
        }),
        record.senders.get(&sender_edge_default)
    );
    assert_eq!(
        Some(&ReceiverRecord {
            id: receiver_edge_default.clone(),
            resource: key_expr_edge_default.clone(),
            runtime: default_runtime,
        }),
        record.receivers.get(&receiver_edge_default)
    );

    // assert the links
    let link_thing = LinkDescriptor {
        from: OutputDescriptor {
            node: "source-0".into(),
            output: "out-0".into(),
        },
        to: InputDescriptor {
            node: sender_thing_edge,
            input: key_expr_thing_edge.to_string().into(),
        },
        shared_memory: None,
    };
    assert!(record.links.contains(&link_thing));

    let link_egde_1 = LinkDescriptor {
        from: OutputDescriptor {
            node: receiver_thing_edge,
            output: key_expr_thing_edge.to_string().into(),
        },
        to: InputDescriptor {
            node: "operator-1".into(),
            input: "in-1".into(),
        },
        shared_memory: None,
    };
    assert!(record.links.contains(&link_egde_1));

    let link_edge_2 = LinkDescriptor {
        from: OutputDescriptor {
            node: "operator-1".into(),
            output: "out-1".into(),
        },
        to: InputDescriptor {
            node: sender_edge_default,
            input: key_expr_edge_default.to_string().into(),
        },
        shared_memory: None,
    };
    assert!(record.links.contains(&link_edge_2));

    let link_default = LinkDescriptor {
        from: OutputDescriptor {
            node: receiver_edge_default,
            output: key_expr_edge_default.to_string().into(),
        },
        to: InputDescriptor {
            node: "sink-2".into(),
            input: "in-2".into(),
        },
        shared_memory: None,
    };
    assert!(record.links.contains(&link_default));
}
