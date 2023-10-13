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
    FlattenedOperatorDescriptor, FlattenedSinkDescriptor, FlattenedSourceDescriptor, LinkDescriptor,
};

use serde::{Deserialize, Serialize};

/// TODO@J-Loudet Documentation?
///
/// # Examples
///
/// ```
/// use zenoh_flow_descriptors::FlattenedDataFlowDescriptor;
///
/// let yaml = "
/// flow: DataFlow
///
/// sources:
///   - id: Source-0
///     name: Source
///     configuration:
///       foo: bar
///       answer: 0
///     uri: file:///home/zenoh-flow/node/libsource.so
///     outputs:
///       - out-operator
///     mapping: zenoh-flow-plugin-0
///     
/// operators:
///   - id: Operator-1
///     name: Operator
///     configuration:
///       foo: bar
///       answer: 1
///     uri: file:///home/zenoh-flow/node/liboperator.so
///     inputs:
///       - in-source
///     outputs:
///       - out-sink
///     
/// sinks:
///   - id: Sink-2
///     name: Sink
///     configuration:
///       foo: bar
///       answer: 2
///     uri: file:///home/zenoh-flow/node/libsink.so
///     inputs:
///       - in-operator
///
/// links:
///   - from:
///       node: Source-0
///       output : out-operator
///     to:
///       node : Operator-1
///       input : in-source
///       
///   - from:
///       node : Operator-1
///       output : out-sink
///     to:
///       node : Sink-2
///       input : in-operator
/// ";
///
/// let data_flow_yaml = serde_yaml::from_str::<FlattenedDataFlowDescriptor>(yaml).unwrap();
///
/// let json = "
/// {
///   \"flow\": \"DataFlow\",
///
///   \"configuration\": {
///     \"foo\": \"bar\"
///   },
///
///   \"sources\": [
///     {
///       \"id\": \"Source-0\",
///       \"name\": \"Source\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 0
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/libsource.so\",
///       \"outputs\": [
///         \"out-operator\"
///       ],
///       \"mapping\": \"zenoh-flow-plugin-0\"
///     }
///   ],
///       
///   \"operators\": [
///     {
///       \"id\": \"Operator-1\",
///       \"name\": \"Operator\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 1
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/liboperator.so\",
///       \"inputs\": [
///         \"in-source\"
///       ],
///       \"outputs\": [
///         \"out-sink\"
///       ]
///     }
///   ],
///   
///   \"sinks\": [
///     {
///       \"id\": \"Sink-2\",
///       \"name\": \"Sink\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 2
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/libsink.so\",
///       \"inputs\": [
///         \"in-operator\"
///       ]
///     }
///   ],
///
///   \"links\": [
///     {
///       \"from\": {
///         \"node\": \"Source-0\",
///         \"output\": \"out-operator\"
///       },
///       \"to\": {
///         \"node\": \"Operator-1\",
///         \"input\": \"in-source\"
///       }
///     },
///     {
///       \"from\": {
///         \"node\": \"Operator-1\",
///         \"output\": \"out-sink\"
///       },
///       \"to\": {
///         \"node\": \"Sink-2\",
///         \"input\": \"in-operator\"
///       }
///     }
///   ]
/// }
/// ";
///
/// let data_flow_json = serde_json::from_str::<FlattenedDataFlowDescriptor>(json).unwrap();
/// assert_eq!(data_flow_yaml, data_flow_json);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedDataFlowDescriptor {
    pub flow: String,
    pub sources: Vec<FlattenedSourceDescriptor>,
    pub operators: Vec<FlattenedOperatorDescriptor>,
    pub sinks: Vec<FlattenedSinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
}
