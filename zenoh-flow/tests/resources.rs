//
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

use zenoh_flow::runtime::resources::ROOT_STANDALONE;
use zenoh_flow::{
    FLOW_SELECTOR_BY_FLOW, FLOW_SELECTOR_BY_INSTANCE, RT_CONFIGURATION_PATH, RT_FLOW_PATH,
    RT_FLOW_SELECTOR_ALL, RT_FLOW_SELECTOR_BY_FLOW, RT_FLOW_SELECTOR_BY_INSTANCE, RT_INFO_PATH,
    RT_STATUS_PATH,
};

#[test]
fn runtime_macros() {
    let correct_path = String::from("/zenoh-flow/runtimes/1/info");
    let gen_path = RT_INFO_PATH!(ROOT_STANDALONE, "1");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/1/status");
    let gen_path = RT_STATUS_PATH!(ROOT_STANDALONE, "1");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/1/configuration");
    let gen_path = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, "1");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/1/flows/2/3");
    let gen_path = RT_FLOW_PATH!(ROOT_STANDALONE, "1", "2", "3");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/1/flows/*/3");
    let gen_path = RT_FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, "1", "3");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/1/flows/2/*");
    let gen_path = RT_FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, "1", "2");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/1/flows/*/*");
    let gen_path = RT_FLOW_SELECTOR_ALL!(ROOT_STANDALONE, "1");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/*/flows/*/3");
    let gen_path = FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, "3");

    assert_eq!(correct_path, gen_path);

    let correct_path = String::from("/zenoh-flow/runtimes/*/flows/2/*");
    let gen_path = FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, "2");

    assert_eq!(correct_path, gen_path);
}
