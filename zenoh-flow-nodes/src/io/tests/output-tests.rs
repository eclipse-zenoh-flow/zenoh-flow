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

use prost::Message;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use zenoh_flow_commons::PortId;

use super::Outputs;
use crate::messages::{LinkMessage, Payload};

/// Test that the Output behaves as expected for the provided data and serializer:
/// 1. the `serializer` is correctly type-erased yet still produces the correct output,
/// 2. the `expected_data` is not eagerly serialized and can correctly be downcasted.
///
/// ## Scenario tested
///
/// A bogus output is generated — see the call to `outputs.take`. We go through the `Outputs`
/// structure such that the transformation on the serializer is performed (i.e. the type is erased).
///
/// The provided `expected_data` is sent on the output.
///
/// A receiver channel ensures that:
/// 1. it is a `Payload::Typed`,
/// 2. we can still downcast it to `T`,
/// 3. the result of the serialization is correct.
///
/// ## Traits on T
///
/// The bounds on `T` are more restrictive than what they are in the code. In particular, `Clone`
/// and `std::fmt::Debug` are not required. This has no impact on the test and mostly help us debug.
fn test_typed_output<T: Send + Sync + Clone + std::fmt::Debug + PartialEq + 'static>(
    expected_data: T,
    expected_serialized: Vec<u8>,
    serializer: impl for<'b, 'a> Fn(&'b mut Vec<u8>, &'a T) -> anyhow::Result<()>
        + Send
        + Sync
        + 'static,
) {
    let hlc = uhlc::HLC::default();
    let key: PortId = "test".into();

    let (tx, rx) = flume::unbounded::<LinkMessage>();

    let mut outputs = Outputs {
        hmap: HashMap::from([(key.clone(), vec![tx])]),
        hlc: Arc::new(hlc),
    };

    let output = outputs
        .take(key.as_ref())
        .expect("Wrong key provided")
        .typed(serializer);

    output
        .try_send(expected_data.clone(), None)
        .expect("Failed to send the message");

    let message = rx.recv().expect("Received no message");
    match message {
        LinkMessage::Data(data) => match &*data {
            Payload::Bytes(_) => panic!("Unexpected bytes payload"),
            Payload::Typed((dyn_data, serializer)) => {
                let mut dyn_serialized = Vec::new();
                (serializer)(&mut dyn_serialized, dyn_data.clone()).expect("Failed to serialize");
                assert_eq!(expected_serialized, dyn_serialized);

                let data = (**dyn_data)
                    .as_any()
                    .downcast_ref::<T>()
                    .expect("Failed to downcast");
                assert_eq!(expected_data, *data);
            }
        },
        LinkMessage::Watermark(_) => panic!("Unexpected watermark message"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// SERDE JSON

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    pub field1: u8,
    pub field2: String,
    pub field3: f64,
}

#[test]
fn test_serde_json() {
    let expected_data = TestData {
        field1: 1u8,
        field2: "two".into(),
        field3: 0.3f64,
    };

    let expected_serialized =
        serde_json::ser::to_vec(&expected_data).expect("serde_json failed to serialize");

    let serializer = |buffer: &mut Vec<u8>, data: &TestData| {
        serde_json::ser::to_writer(buffer, data).map_err(|e| anyhow::anyhow!(e))
    };

    test_typed_output(expected_data, expected_serialized, serializer)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// PROTOBUF PROST

// This structure was generated using the `prost-build` crate. We copied & pasted it here such that
// we do not have to include `prost-build` as a build dependency to Zenoh-Flow. Our only purpose is
// to ensure that at least one implementation of ProtoBuf works, not to suggest to use Prost.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestProto {
    #[prost(int64, tag = "1")]
    pub field1: i64,
    #[prost(string, tag = "2")]
    pub field2: ::prost::alloc::string::String,
    #[prost(double, tag = "3")]
    pub field3: f64,
}

#[test]
fn test_protobuf_prost() {
    let expected_data = TestProto {
        field1: 1i64,
        field2: "two".into(),
        field3: 0.3f64,
    };

    let expected_serialized = expected_data.encode_to_vec();

    let serializer = |buffer: &mut Vec<u8>, data: &TestProto| {
        data.encode(buffer).map_err(|e| anyhow::anyhow!(e))
    };

    test_typed_output(expected_data, expected_serialized, serializer)
}
