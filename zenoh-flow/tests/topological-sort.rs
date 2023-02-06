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

use std::collections::HashMap;
use std::convert::TryFrom;
use uuid::Uuid;
use zenoh_flow::model::descriptor::FlattenDataFlowDescriptor;
use zenoh_flow::model::record::DataFlowRecord;
use zenoh_flow::types::NodeId;

static MONTBLANC: &str = r#"
flow: Montblanc

operators:
  - id : Lyon
    uri: file://./liblyon.so
    inputs:
      - id: Amazon
        type: f32
    outputs:
      - id: Tigris
        type: f32

  - id : Hamburg
    uri: file://./libhamburg.so
    inputs:
      - id: Danube
        type: str
      - id: Ganges
        type: i64
      - id: Nile
        type: i32
      - id: Tigris
        type: f32
    outputs:
      - id: Parana
        type: str

  - id : Taipei
    uri: file://./libtaipei.so
    inputs:
      - id: Columbia
        type: img
    outputs:
      - id: Colorado
        type: img

  - id : Osaka
    uri: file://./libosaka.so
    inputs:
      - id: Parana
        type: str
      - id: Columbia
        type: img
      - id: Colorado
        type: img
    outputs:
      - id: Salween
        type: point-cloud2
      - id: Godavari
        type: laser-scan

  - id : Tripoli
    uri: file://./libtripoli.so
    inputs:
      - id: Columbia
        type: img
      - id: Godavari
        type: laser-scan
    outputs:
      - id: Loire
        type: point-cloud2

  - id : Mandalay
    uri: file://./libmandalay.so
    inputs:
      - id: Danube
        type: str
      - id: Chenab
        type: quaternion
      - id: Salween
        type: point-cloud2
      - id: Godavari
        type: laser-scan
      - id: Loire
        type: point-cloud2
      - id: Yamuna
        type: vector3
    outputs:
      - id: Brazos
        type: point-cloud2
      - id: Tagus
        type: pose
      - id: Missouri
        type: img

  - id : Ponce
    uri: file://./libponce.so
    inputs:
      - id: Danube
        type: str
      - id: Brazos
        type: point-cloud2
      - id: Tagus
        type: pose
      - id: Missouri
        type: img
      - id: Loire
        type: point-cloud2
      - id: Yamuna
        type: vector3
      - id: Godavari
        type: laser-scan
    outputs:
      - id: Congo
        type: twist
      - id: Mekong
        type: twist-w-coovariance-ts

  - id : Geneva
    uri: file://./libgeneva.so
    inputs:
      - id: Danube
        type: str
      - id: Parana
        type: str
      - id: Tagus
        type: pose
      - id: Congo
        type: twist
    outputs:
      - id: Arkansas
        type: str

  - id : Rotterdam
    uri: file://./librotterdam.so
    inputs:
      - id: Mekong
        type: twist-w-coovariance-ts
    outputs:
      - id: Murray
        type: vector3-ts

  - id : Barcelona
    uri: file://./libbarcelona.so
    inputs:
      - id: Mekong
        type: twist-w-coovariance-ts
    outputs:
      - id: Lena
        type: wrench-ts




sources:
  - id : Cordoba
    uri: file://./libcordoba.so
    outputs:
      - id: Amazon
        type: f32

  - id : Portsmouth
    uri: file://./libportsmouth.so
    outputs:
      - id: Danube
        type: str

  - id : Freeport
    uri: file://./libfreeport.so
    outputs:
      - id: Ganges
        type: i64

  - id : Madelin
    uri: file://./libmadelin.so
    outputs:
      - id: Nile
        type: i32

  - id : Delhi
    uri: file://./libdelhi.so
    outputs:
      - id: Columbia
        type: img

  - id : Hebron
    uri: file://./libhebron.so
    outputs:
      - id: Chenab
        type: quaternion

  - id : Kingston
    uri: file://./libkingston.so
    outputs:
      - id: Yamuna
        type: vector3

sinks:
  - id : Arequipa
    uri: file://./libarequipa.so
    inputs:
      - id: Arkansas
        type: str

  - id : Monaco
    uri: file://./libmonaco.so
    inputs:
      - id: Congo
        type: twist

  - id : Georgetown
    uri: file://./libgeorgetown.so
    inputs:
      - id: Lena
        type: wrench-ts
      - id: Murray
        type: vector3-ts

links:
- from:
    node : Cordoba
    output : Amazon
  to:
    node : Lyon
    input : Amazon
- from:
    node : Portsmouth
    output : Danube
  to:
    node : Hamburg
    input : Danube
- from:
    node : Freeport
    output : Ganges
  to:
    node : Hamburg
    input : Ganges
- from:
    node : Madelin
    output : Nile
  to:
    node : Hamburg
    input : Nile
- from:
    node : Lyon
    output : Tigris
  to:
    node : Hamburg
    input : Tigris
- from:
    node : Delhi
    output : Columbia
  to:
    node : Taipei
    input : Columbia
- from:
    node : Delhi
    output : Columbia
  to:
    node : Osaka
    input : Columbia
- from:
    node : Taipei
    output : Colorado
  to:
    node : Osaka
    input : Colorado
- from:
    node : Hamburg
    output : Parana
  to:
    node : Osaka
    input : Parana
- from:
    node : Osaka
    output : Godavari
  to:
    node : Tripoli
    input : Godavari
- from:
    node : Delhi
    output : Columbia
  to:
    node : Tripoli
    input : Columbia
- from:
    node : Portsmouth
    output : Danube
  to:
    node : Mandalay
    input : Danube
- from:
    node : Osaka
    output : Salween
  to:
    node : Mandalay
    input : Salween
- from:
    node : Hebron
    output : Chenab
  to:
    node : Mandalay
    input : Chenab
- from:
    node : Osaka
    output : Godavari
  to:
    node : Mandalay
    input : Godavari
- from:
    node : Tripoli
    output : Loire
  to:
    node : Mandalay
    input : Loire
- from:
    node : Kingston
    output : Yamuna
  to:
    node : Mandalay
    input : Yamuna
- from:
    node : Portsmouth
    output : Danube
  to:
    node : Ponce
    input : Danube
- from:
    node : Mandalay
    output : Brazos
  to:
    node : Ponce
    input : Brazos
- from:
    node : Mandalay
    output : Tagus
  to:
    node : Ponce
    input : Tagus
- from:
    node : Mandalay
    output : Missouri
  to:
    node : Ponce
    input : Missouri
- from:
    node : Tripoli
    output : Loire
  to:
    node : Ponce
    input : Loire
- from:
    node : Kingston
    output : Yamuna
  to:
    node : Ponce
    input : Yamuna
- from:
    node : Osaka
    output : Godavari
  to:
    node : Ponce
    input : Godavari
- from:
    node : Ponce
    output : Congo
  to:
    node : Geneva
    input : Congo
- from:
    node : Portsmouth
    output : Danube
  to:
    node : Geneva
    input : Danube
- from:
    node : Hamburg
    output : Parana
  to:
    node : Geneva
    input : Parana
- from:
    node : Mandalay
    output : Tagus
  to:
    node : Geneva
    input : Tagus
- from:
    node : Geneva
    output : Arkansas
  to:
    node : Arequipa
    input : Arkansas
- from:
    node : Ponce
    output : Congo
  to:
    node : Monaco
    input : Congo
- from:
    node : Ponce
    output : Mekong
  to:
    node : Rotterdam
    input : Mekong
- from:
    node : Ponce
    output : Mekong
  to:
    node : Barcelona
    input : Mekong
- from:
    node : Rotterdam
    output : Murray
  to:
    node : Georgetown
    input : Murray
- from:
    node : Barcelona
    output : Lena
  to:
    node : Georgetown
    input : Lena

mapping:
  Cordoba: pc
  Portsmouth: pc
  Freeport: pc
  Madelin: pc
  Lyon: pc
  Delhi: pc
  Hamburg: pc
  Taipei: pc
  Osaka: pc
  Hebron: pc
  Tripoli: pc
  Kingston: pc
  Mandalay: pc
  Ponce: pc
  Geneva: pc
  Monaco: pc
  Rotterdam: pc
  Barcelona: pc
  Georgetown: pc
  Arequipa: pc
"#;

#[allow(clippy::uninlined_format_args)]
#[test]
fn validate_complex() {
    let expected_sort: HashMap<u32, Vec<NodeId>> = HashMap::from([
        (
            0u32,
            vec![
                "Cordoba".into(),
                "Portsmouth".into(),
                "Freeport".into(),
                "Madelin".into(),
                "Delhi".into(),
                "Hebron".into(),
                "Kingston".into(),
            ],
        ),
        (1u32, vec!["Lyon".into(), "Taipei".into()]),
        (2u32, vec!["Hamburg".into()]),
        (3u32, vec!["Osaka".into()]),
        (4u32, vec!["Tripoli".into()]),
        (5u32, vec!["Mandalay".into()]),
        (6u32, vec!["Ponce".into()]),
        (
            7u32,
            vec![
                "Barcelona".into(),
                "Rotterdam".into(),
                "Monaco".into(),
                "Geneva".into(),
            ],
        ),
        (8u32, vec!["Arequipa".into(), "Georgetown".into()]),
    ]);

    // First validate the dataflow.
    let r = FlattenDataFlowDescriptor::from_yaml(MONTBLANC);
    assert!(r.is_ok(), "Unexepected error: {:?}", r);

    // Converts it into a record.
    let record = DataFlowRecord::try_from((r.unwrap(), Uuid::nil()));
    assert!(record.is_ok(), "Unexepected error: {:?}", record);
    let record = record.unwrap();

    // Gets the dependency graph.
    let sorted = record.as_sorted_dependency_graph();
    assert!(sorted.is_ok(), "Unexepected error: {:?}", sorted);
    let sorted = sorted.unwrap();

    // Verify the order is correct.
    assert!(expected_sort
        .keys()
        .all(|x| sorted.keys().collect::<Vec<_>>().contains(&x)));
    for (idx, nodes) in &expected_sort {
        let level = sorted.get(idx);
        assert!(level.is_some(), "Not found level {}", idx);

        let level = level.unwrap();

        assert!(nodes.iter().all(|n| level.contains(n)));
    }
}
