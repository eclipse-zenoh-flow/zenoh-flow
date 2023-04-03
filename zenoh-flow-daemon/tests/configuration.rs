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

use zenoh_flow_daemon::daemon::DaemonConfig;

static EXAMPLE_OK_CONFIG: &str = r#"
---
    pid_file : /var/zenoh-flow/runtime.pid
    path : /etc/zenoh-flow
    extensions: /etc/zenoh-flow/extensions.d
    zenoh_config: /etc/zenoh-flow/zenoh-daemon.json
    worker_pool_size: 4
    default_shared_memory_element_size: 10KiB
    default_shared_memory_elements: 10
    default_shared_memory_backoff: 100us
"#;

#[test]
fn daemon_configuration_ok() {
    let _ = env_logger::try_init();

    let deserialized_config: Result<DaemonConfig, serde_yaml::Error> =
        serde_yaml::from_str(EXAMPLE_OK_CONFIG);

    assert!(deserialized_config.is_ok());

    let deserialized_config = deserialized_config.unwrap();

    assert_eq!(
        deserialized_config.pid_file,
        "/var/zenoh-flow/runtime.pid".to_string()
    );
    assert_eq!(deserialized_config.path, "/etc/zenoh-flow".to_string());
    assert_eq!(
        deserialized_config.extensions,
        "/etc/zenoh-flow/extensions.d".to_string()
    );

    assert!(deserialized_config.zenoh_config.is_some());
    assert_eq!(
        deserialized_config.zenoh_config.unwrap(),
        "/etc/zenoh-flow/zenoh-daemon.json".to_string()
    );

    assert_eq!(deserialized_config.worker_pool_size, 4);

    assert!(deserialized_config
        .default_shared_memory_element_size
        .is_some());
    assert_eq!(
        deserialized_config
            .default_shared_memory_element_size
            .unwrap(),
        10240
    );

    assert!(deserialized_config.default_shared_memory_elements.is_some());
    assert_eq!(
        deserialized_config.default_shared_memory_elements.unwrap(),
        10
    );

    assert!(deserialized_config.default_shared_memory_backoff.is_some());
    assert_eq!(
        deserialized_config.default_shared_memory_backoff.unwrap(),
        100_000
    );
}

static EXAMPLE_KO_CONFIG: &str = r#"
---
    pid_file : /var/zenoh-flow/runtime.pid
    uuid: 123,
    path : /etc/zenoh-flow
    extensions: /etc/zenoh-flow/extensions.d
    zenoh_config: /etc/zenoh-flow/zenoh-daemon.json
    worker_pool_size: 4
"#;

#[test]
fn daemon_configuration_ko() {
    let _ = env_logger::try_init();

    let deserialized_config: Result<DaemonConfig, serde_yaml::Error> =
        serde_yaml::from_str(EXAMPLE_KO_CONFIG);

    assert!(deserialized_config.is_err());
}
