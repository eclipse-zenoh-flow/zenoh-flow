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

use crate::model::descriptor::{SinkDescriptor, SourceDescriptor};
use crate::runtime::dataflow::instance::builtin::zenoh::{
    get_zenoh_sink_descriptor, get_zenoh_source_descriptor,
};
use crate::types::Configuration;
use serde_yaml;

static SOURCE_CONFIGURATION_OK: &str = r#"
key-expressions:
  house_data: house_data
"#;

static SOURCE_DESCRIPTOR_GENERATED: &str = r#"
id: zenoh-source
configuration:
  key-expressions:
    house_data: house_data
uri: "builtin://zenoh"
outputs: [house_data]
"#;

#[test]
fn test_builtin_source_ok() {
    let descr = SourceDescriptor::from_yaml(SOURCE_DESCRIPTOR_GENERATED);
    assert!(descr.is_ok());

    let descr = descr.unwrap();

    let configuration = serde_yaml::from_str(SOURCE_CONFIGURATION_OK);
    assert!(configuration.is_ok());
    let configuration: Configuration = configuration.unwrap();

    let generated = get_zenoh_source_descriptor(&configuration);
    assert!(generated.is_ok());

    let generated = generated.unwrap();

    assert_eq!(descr, generated);
}

static SOURCE_CONFIGURATION_KO: &str = r#"
  house_data: house_data
"#;

#[test]
fn test_builtin_source_ko() {
    let configuration = serde_yaml::from_str(SOURCE_CONFIGURATION_KO);
    assert!(configuration.is_ok());
    let configuration: Configuration = configuration.unwrap();

    let generated = get_zenoh_source_descriptor(&configuration);
    assert!(generated.is_err());
}

static SINK_CONFIGURATION_OK: &str = r#"
key-expressions:
  something: debug/something
  something2: debug/something2
"#;

static SINK_DESCRIPTOR_GENERATED: &str = r#"
id: zenoh-sink
configuration:
  key-expressions:
    something: debug/something
    something2: debug/something2
uri: "builtin://zenoh"
inputs:
  - something
  - something2
"#;

#[test]
fn test_builtin_sink_ok() {
    let descr = SinkDescriptor::from_yaml(SINK_DESCRIPTOR_GENERATED);
    assert!(descr.is_ok());

    let descr = descr.unwrap();

    let configuration = serde_yaml::from_str(SINK_CONFIGURATION_OK);
    assert!(configuration.is_ok());
    let configuration: Configuration = configuration.unwrap();

    let generated = get_zenoh_sink_descriptor(&configuration);
    assert!(generated.is_ok());

    let generated = generated.unwrap();

    assert_eq!(descr, generated);
}

static SINK_CONFIGURATION_KO: &str = r#"
  inputid: key/expression
"#;

#[test]
fn test_builtin_sink_ko() {
    let configuration = serde_yaml::from_str(SINK_CONFIGURATION_KO);
    assert!(configuration.is_ok());
    let configuration: Configuration = configuration.unwrap();

    let generated = get_zenoh_sink_descriptor(&configuration);
    assert!(generated.is_err());
}
