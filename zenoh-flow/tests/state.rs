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

// FIXME Whole tests to remove?
// use zenoh_flow::zenoh_flow_derive::ZFState;
// use zenoh_flow::State;

// #[derive(Debug, ZFState, Clone)]
// struct TestState {
//     pub field1: u8,
//     pub field2: String,
//     pub field3: f64,
// }

// #[test]
// fn state_wrapping_unwrapping() {
//     let test_state = TestState {
//         field1: 16u8,
//         field2: String::from("TestString"),
//         field3: 123.456f64,
//     };

//     let mut wrapped_state = State::from(test_state.clone());

//     let unwrapped_state = wrapped_state.try_get::<TestState>().unwrap();

//     assert_eq!(unwrapped_state.field1, test_state.field1);
//     assert_eq!(unwrapped_state.field2, test_state.field2);
//     assert!((unwrapped_state.field3 - test_state.field3).abs() < f64::EPSILON);

//     let boxed_state = Box::new(test_state.clone());

//     let mut wrapped_state = State::from_box(boxed_state);
//     let unwrapped_state = wrapped_state.try_get::<TestState>().unwrap();

//     assert_eq!(unwrapped_state.field1, test_state.field1);
//     assert_eq!(unwrapped_state.field2, test_state.field2);
//     assert!((unwrapped_state.field3 - test_state.field3).abs() < f64::EPSILON);
// }
