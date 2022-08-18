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

use crate::types::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// A Flag is an optional structure to indicate which part(s) of a data flow to activate.
///
/// The YAML definition is as follow:
///
/// ```yaml
/// flags:
/// - id: my-flag
///   toggle: true
///   nodes:
///   - A
///   - B
/// - id: my-second-flag
///   toggle: false
///   nodes:
///   - B
///   - C
/// ```
///
/// This definition tells Zenoh-Flow to activate nodes A and B, and to deactivate node C.
///
/// Node A is activated because it is specified under the flag `my-flag` which is toggled. Although
/// node B is present under the flag `my-second-flag` which is not toggled, as it is present under
/// the flag `my-flag` that is toggled, Zenoh-Flow will activate it.
///
/// Node C is deactivated as it is present under the flag `my-second-flag` which is _not_ toggled.
///
/// **All the nodes that are not present under any flag are activated.**
///
/// Zenoh-Flow will ensure the validity of the toggled flags: if they mention a node that does not
/// exist, an error is returned.
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Flag {
    pub(crate) id: Arc<str>,
    pub(crate) toggle: bool,
    pub(crate) nodes: Vec<NodeId>,
}

/// Given a set of flags, return the list of nodes that should be "removed" (i.e. not activate).
///
/// The logic is:
/// 1. if a flag is toggled, put it aside to be processed later,
/// 2. if a flag is _not_ toggled, add all the nodes it indicates to the list of nodes to remove,
/// 3. processing the toggled flags, remove the nodes that should be activated from the list.
pub(crate) fn get_nodes_to_remove(flags: &[Flag]) -> HashSet<NodeId> {
    let mut nodes = HashSet::new();
    let mut flags_on = Vec::new();

    for flag in flags {
        if flag.toggle {
            flags_on.push(flag);
        } else {
            nodes.extend(flag.nodes.iter().cloned());
        }
    }

    flags_on.iter().for_each(|flag| {
        flag.nodes.iter().for_each(|node| {
            let _ = nodes.remove(node);
        })
    });

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_nodes_to_remove() {
        let flag1 = Flag {
            id: "flag1".into(),
            toggle: true,
            nodes: vec!["A".into()],
        };
        let flag2 = Flag {
            id: "flag2".into(),
            toggle: false,
            nodes: vec!["A".into(), "B".into(), "C".into()],
        };
        let flag3 = Flag {
            id: "flag3".into(),
            toggle: true,
            nodes: vec!["C".into()],
        };
        let flag4 = Flag {
            id: "flag4".into(),
            toggle: false,
            nodes: vec!["A".into(), "C".into()],
        };

        // Flag1 activates A.
        // Flag2 deactivates A, B, C.
        // Flag3 activates C.
        // Flag4 deactivates A, C.
        //
        // The only node that should be deactivated is B.
        assert_eq!(
            get_nodes_to_remove(&[flag1, flag2, flag3, flag4]),
            ["B".into()].into()
        );
    }
}
