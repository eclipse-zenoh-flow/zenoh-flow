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

use zenoh_flow::types::Configuration;

static EXAMPLE_CONFIG: &str = r#"
map1:
  foo: value
  bar: value
  baz: value
string1: some_value
empty:
list1:
    - this
    - is
    - a
    - list
"#;

#[test]
fn configuration() {
    let deserialized_config: Result<Configuration, serde_yaml::Error> =
        serde_yaml::from_str(EXAMPLE_CONFIG);

    assert!(deserialized_config.is_ok());

    let deserialized_config = deserialized_config.unwrap();

    assert!(deserialized_config["non_existing_key"].is_null());

    assert!(deserialized_config["empty"].is_null());

    assert_eq!(
        "some_value",
        deserialized_config["string1"].as_str().unwrap()
    );

    assert!(deserialized_config["map1"].is_object());

    assert_eq!(
        "value",
        deserialized_config["map1"]["foo"].as_str().unwrap()
    );

    assert!(deserialized_config["list1"].is_array());

    assert_eq!("a", deserialized_config["list1"][2].as_str().unwrap());
}
