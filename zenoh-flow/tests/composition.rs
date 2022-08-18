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

use std::fs::File;
use std::io::Write;
use std::path::Path;
use tempdir::TempDir;
use zenoh_flow::model::dataflow::descriptor::*;
use zenoh_flow::model::node::{
    CompositeOperatorDescriptor, SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor,
};
use zenoh_flow::prelude::*;

static DESCRIPTOR_FLOW: &str = r#"
flow: test
operators:
 - id: x
   descriptor: ./x.yml
sources:
  - id: y
    descriptor: ./y.yml
sinks:
  - id: z
    descriptor: ./z.yml

links:
 - from:
    node: y
    output: Content
   to:
    node: x
    input: Data
 - from:
    node: x
    output: Result
   to:
     node: z
     input: Line
"#;

static DESCRIPTOR_A: &str = r#"
id: to_lines
uri: file://daeubwu
inputs:
  - id: Data
    type: str
outputs:
  - id: Lines
    type: vec<str>
tags: []
"#;

static DESCRIPTOR_B: &str = r#"
id: to_vec
operators:
  - id: D
    descriptor: ./D.yml
  - id: E
    descriptor: ./E.yml

inputs:
  - node: D
    input: DIN0
outputs:
  - node: E
    output: EOUT0

links:
 - from:
    node: D
    output: DOUT0
   to:
    node: E
    input: EIN0
"#;

static DESCRIPTOR_C: &str = r#"
id: count
uri: file://deiqdeiad
inputs:
  - id: Words
    type: vec<str>
outputs:
  - id: Result
    type: str
tags: []
"#;

static DESCRIPTOR_D: &str = r#"
id: operaDor
uri: file://.edaewdqe
inputs:
  - id: DIN0
    type: vec<str>
outputs:
  - id: DOUT0
    type: t
tags: []
"#;

static DESCRIPTOR_E: &str = r#"
id: whatevver
uri: file://.edaewdqe
inputs:
  - id: EIN0
    type: t
outputs:
  - id: EOUT0
    type: vec<str>
tags: []
"#;

static DESCRIPTOR_X: &str = r#"
id: word_count
operators:
- id: A
  descriptor: ./A.yml
- id: B
  descriptor: ./B.yml
- id: C
  descriptor: ./C.yml

inputs:
  - node: A
    input: Data
outputs:
  - node: C
    output: Result


links:
 - from:
    node: A
    output: Lines
   to:
    node: B
    input: DIN0
 - from:
    node: B
    output: EOUT0
   to:
    node: C
    input: Words

"#;

static DESCRIPTOR_Y: &str = r#"
id: file-src
uri: file://bla/bla
outputs:
  - id: Content
    type: str
tags: []
"#;

static DESCRIPTOR_Z: &str = r#"
id: file-snk
uri: file://vlala
inputs:
  - id: Line
    type: str
tags: []
"#;

//      Composite flow schema
//            ┌────┐   ┌────┐   ┌────┐
//   Source   │ y  ├──►│ x  ├──►│  z │ Sink
//            └────┘   └────┘   └────┘
//                     /     \
//                    /       \
//                   /         \
//             ┌────┐   ┌────┐   ┌────┐
//          ──►│ A  ├──►│  B ├──►│ C  ├──►
//             └────┘   └────┘   └────┘
//                      /    \
//                     /      \
//                    /        \
//                 ┌────┐    ┌────┐
//              ──►│ D  ├───►│  E ├──►
//                 └────┘    └────┘
// Drawing generated with https://asciiflow.com

#[test]
fn composition_ok() {
    // First let's load the string and store them into a temp directory.

    // Loading simple operators first
    let op_e = SimpleOperatorDescriptor::from_yaml(DESCRIPTOR_E);
    let op_d = SimpleOperatorDescriptor::from_yaml(DESCRIPTOR_D);
    let op_c = SimpleOperatorDescriptor::from_yaml(DESCRIPTOR_C);
    let op_a = SimpleOperatorDescriptor::from_yaml(DESCRIPTOR_A);

    // Verify the loading
    assert!(op_e.is_ok());
    assert!(op_d.is_ok());
    assert!(op_c.is_ok());
    assert!(op_a.is_ok());

    // getting the actual descriptor
    let op_e = op_e.unwrap();
    let op_d = op_d.unwrap();
    let op_c = op_c.unwrap();
    let op_a = op_a.unwrap();

    // Loading compositite operators
    let op_b = CompositeOperatorDescriptor::from_yaml(DESCRIPTOR_B);
    let op_x = CompositeOperatorDescriptor::from_yaml(DESCRIPTOR_X);

    // Verify the loading
    assert!(op_b.is_ok());
    assert!(op_x.is_ok());

    // getting the actual descriptor
    let mut op_b = op_b.unwrap();
    let mut op_x = op_x.unwrap();

    //Loading source and sink
    let src = SourceDescriptor::from_yaml(DESCRIPTOR_Y);
    let sink = SinkDescriptor::from_yaml(DESCRIPTOR_Z);

    // Verify the loading
    assert!(src.is_ok());
    assert!(sink.is_ok());

    // getting the actual descriptor
    let src = src.unwrap();
    let sink = sink.unwrap();

    //Loading the flow
    let flow = DataFlowDescriptor::from_yaml(DESCRIPTOR_FLOW);

    // Verify the loading
    assert!(flow.is_ok());

    // getting the actual descriptor
    let mut flow = flow.unwrap();

    // Creating a temp directory.
    let temp_dir = TempDir::new("zf-composition-test");
    assert!(temp_dir.is_ok());
    let temp_dir = temp_dir.unwrap();

    // Creating paths for simple operators
    let e_op_path = temp_dir.path().join("E.yml");
    let d_op_path = temp_dir.path().join("D.yml");
    let c_op_path = temp_dir.path().join("C.yml");
    let a_op_path = temp_dir.path().join("A.yml");

    // Storing content for simple operators
    assert!(write_file(&e_op_path, op_e.to_yaml().unwrap().into_bytes()).is_ok());
    assert!(write_file(&d_op_path, op_d.to_yaml().unwrap().into_bytes()).is_ok());
    assert!(write_file(&c_op_path, op_c.to_yaml().unwrap().into_bytes()).is_ok());
    assert!(write_file(&a_op_path, op_a.to_yaml().unwrap().into_bytes()).is_ok());

    // updating the paths in the B composite operator
    for op in op_b.operators.iter_mut() {
        if op.id == "E".into() {
            op.descriptor = e_op_path.to_string_lossy().into();
        }
        if op.id == "D".into() {
            op.descriptor = d_op_path.to_string_lossy().into();
        }
    }

    //creating the B path and storing it.
    let b_op_path = temp_dir.path().join("B.yml");
    assert!(write_file(&b_op_path, op_b.to_yaml().unwrap().into_bytes()).is_ok());

    // updating the paths in the X composite operator
    for op in op_x.operators.iter_mut() {
        if op.id == "A".into() {
            op.descriptor = a_op_path.to_string_lossy().into();
        }
        if op.id == "B".into() {
            op.descriptor = b_op_path.to_string_lossy().into();
        }

        if op.id == "C".into() {
            op.descriptor = c_op_path.to_string_lossy().into();
        }
    }

    //creating the X path and storing it.
    let x_op_path = temp_dir.path().join("X.yml");
    assert!(write_file(&x_op_path, op_x.to_yaml().unwrap().into_bytes()).is_ok());

    // creating and storing source and sink
    let src_path = temp_dir.path().join("Y.yml");
    let sink_path = temp_dir.path().join("Z.yml");

    assert!(write_file(&src_path, src.to_yaml().unwrap().into_bytes()).is_ok());
    assert!(write_file(&sink_path, sink.to_yaml().unwrap().into_bytes()).is_ok());

    // updating the paths in the flow
    for op in flow.operators.iter_mut() {
        if op.id == "x".into() {
            op.descriptor = x_op_path.to_string_lossy().into();
        }
    }

    for op in flow.sources.iter_mut() {
        if op.id == "y".into() {
            op.descriptor = src_path.to_string_lossy().into();
        }
    }

    for op in flow.sinks.iter_mut() {
        if op.id == "z".into() {
            op.descriptor = sink_path.to_string_lossy().into();
        }
    }

    async_std::task::block_on(async move {
        // Flattening

        let res = flow.flatten().await;
        assert!(res.is_ok());
        let flattened = res.unwrap();

        // validation
        assert!(flattened.validate().is_ok());
    });
}

fn write_file(path: &Path, content: Vec<u8>) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(&content)?;
    Ok(file.sync_all()?)
}
