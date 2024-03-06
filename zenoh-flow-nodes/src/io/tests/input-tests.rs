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

use prost::Message as pMessage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{Input, InputRaw};
use crate::{
    messages::{LinkMessage, Payload},
    traits::SendSyncAny,
};

/// Test that the Input behaves as expected for the provided data and deserializer:
/// 1. when a Payload::Bytes is received the deserializer is called and produces the correct output,
/// 2. when a Payload::Typed is received the data can correctly be downcasted.
///
/// ## Scenario tested
///
/// A typed input is created.
///
/// We send on the associated channel:
/// 1. a Payload::Bytes (the `expected_serialized`),
/// 2. a Payload::Typed (the `expected_data` upcasted to `dyn SendSyncAny`).
///
/// ## Traits bound on T
///
/// The bounds on `T` are more restrictive than what they are in the code. In particular, `Clone`
/// and `std::fmt::Debug` are not required. This has no impact on the test and mostly help us debug.
fn test_typed_input<T: Send + Sync + Clone + std::fmt::Debug + PartialEq + 'static>(
    expected_data: T,
    expected_serialized: Vec<u8>,
    deserializer: impl Fn(&[u8]) -> anyhow::Result<T> + Send + Sync + 'static,
) {
    let hlc = uhlc::HLC::default();
    let (tx, rx) = flume::unbounded::<LinkMessage>();

    let input_raw = InputRaw {
        port_id: "test-id".into(),
        receiver: rx,
    };

    let input = Input {
        input_raw,
        deserializer: Arc::new(deserializer),
    };

    let message = LinkMessage::new(
        Payload::Bytes(Arc::new(expected_serialized)),
        hlc.new_timestamp(),
    );
    tx.send(message).expect("Failed to send message");

    let (data, _) = input
        .try_recv()
        .expect("Message (serialized) was not sent")
        .expect("No message was received");

    assert_eq!(expected_data, *data);

    let message = LinkMessage::new(
        Payload::Typed((
            Arc::new(expected_data.clone()) as Arc<dyn SendSyncAny>,
            // The serializer should never be called, hence the panic.
            Arc::new(|_buffer, _data| panic!("Unexpected call to serialize the data")),
        )),
        hlc.new_timestamp(),
    );
    tx.send(message).expect("Failed to send message");

    let (data, _) = input
        .try_recv()
        .expect("Message (dyn SendSyncAny) was not sent")
        .expect("No message was received");
    assert_eq!(expected_data, *data);
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
        field2: "test".to_string(),
        field3: 0.2f64,
    };

    let expected_serialized =
        serde_json::ser::to_vec(&expected_data).expect("serde_json failed to serialize");

    test_typed_input(expected_data, expected_serialized, |bytes| {
        serde_json::de::from_slice::<TestData>(bytes).map_err(|e| anyhow::anyhow!(e))
    })
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
        field2: "test".to_string(),
        field3: 0.2f64,
    };

    // First test, send data serialized.
    let expected_serialized = expected_data.encode_to_vec();

    test_typed_input(expected_data, expected_serialized, |bytes| {
        <TestProto>::decode(bytes).map_err(|e| anyhow::anyhow!(e))
    })
}
