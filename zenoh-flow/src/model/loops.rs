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

use crate::{NodeId, PortId, PortType};
use serde::{Deserialize, Serialize};

/// A LoopDescriptor is the internal representation of a Loop in Zenoh Flow.
///
/// Zenoh Flow supports two types of loops: infinite loops (such as a control loop in a robotic
/// context) or finite loop. When the Loop is finite, Zenoh Flow keeps track of the number of
/// iterations.
///
/// The Operator at the "beginning" of a Loop is called the `Ingress`. The Operator at the "end" is
/// called the `Egress`.
///
/// The provided `feedback_port` will be created by Zenoh Flow for both operators. An error will be
/// raised if an input (resp. output) port with the same name already exists for the Ingress (resp.
/// Egress).
///
/// The Ingress and Egress must respect the following constraints:
/// - the Ingress only has a single output,
/// - the Egress only has a single input,
/// - they both must be an Operator (not Source nor Sink),
/// - they must be Ingress or Egress for a single Loop,
/// - the Ingress must be "before" the Egress (i.e. without the Loop, there should be no path
///   between both).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopDescriptor {
    pub ingress: NodeId,
    pub egress: NodeId,
    pub feedback_port: PortId,
    pub is_infinite: bool,
    pub port_type: PortType,
}
