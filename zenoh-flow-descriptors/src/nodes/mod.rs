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

pub(crate) mod operator;
pub use operator::OperatorDescriptor;
pub(crate) mod sink;
pub use sink::SinkDescriptor;
pub(crate) mod source;
pub use source::SourceDescriptor;

use crate::{
    flattened::{IFlattenable, IFlattenableComposite, Patch},
    uri, LinkDescriptor,
};
use anyhow::bail;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use zenoh_flow_commons::deserialize_id;
use zenoh_flow_commons::{Configuration, NodeId, Result, Vars};

/// A generic Zenoh-Flow node.
///
/// It could represent a [SourceDescriptor], an [OperatorDescriptor], a [CompositeOperatorDescriptor] or a
/// [SinkDescriptor].
///
/// How Zenoh-Flow will try to parse the associated `descriptor` depends on the section in which this `NodeDescriptor`
/// is declared. For example, Zenoh-Flow will parse the nodes declared in the `sources` section as [SourceDescriptor].
///
/// # `configuration` section caveat
///
/// The `configuration` section of a `NodeDescriptor` supersedes the same section in the Node it references. See the
/// documentation for more details regarding this behavior.
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::NodeDescriptor;
///
/// let yaml = "
/// id: NodeDescriptor
/// descriptor: file:///home/zenoh-flow/nodes/libnode.so
/// ";
///
/// let node_descriptor_yaml = serde_yaml::from_str::<NodeDescriptor>(&yaml).unwrap();
///
/// let json = "
/// {
///   \"id\": \"NodeDescriptor\",
///   \"descriptor\": \"file:///home/zenoh-flow/nodes/libnode.so\"
/// }
/// ";
///
/// let node_descriptor_json = serde_json::from_str::<NodeDescriptor>(&json).unwrap();
/// assert_eq!(node_descriptor_json, node_descriptor_yaml);
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NodeDescriptor {
    #[serde(deserialize_with = "deserialize_id")]
    pub id: NodeId,
    pub descriptor: Arc<str>,
    #[serde(default)]
    pub configuration: Configuration,
}

impl std::fmt::Display for NodeDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl NodeDescriptor {
    /// TODO@J-Loudet Documentation?
    pub(crate) fn flatten<N: IFlattenable>(
        self,
        overwriting_configuration: Configuration,
        vars: Vars,
    ) -> Result<N::Flattened> {
        let (node_desc, _) = uri::try_load_descriptor::<N>(&self.descriptor, vars)?;
        Ok(node_desc.flatten(self.id, overwriting_configuration))
    }

    /// TODO@J-Loudet Documentation?
    pub(crate) fn flatten_maybe_composite<N: IFlattenableComposite>(
        self,
        overwriting_configuration: Configuration,
        vars: Vars,
        ancestors: &mut HashSet<Arc<str>>,
    ) -> Result<(Vec<N::Flattened>, Vec<LinkDescriptor>, Patch)> {
        // 1st attempt: try to flatten as a regular node.
        let res_node = self
            .clone()
            .flatten::<N::Flattenable>(overwriting_configuration.clone(), vars.clone());

        if let Ok(node) = res_node {
            return Ok((vec![node], Vec::default(), Patch::default()));
        }

        // 2nd attempt: try to flatten as a composite node.
        if !ancestors.insert(self.descriptor.clone()) {
            bail!(
                r###"
Possible infinite recursion detected, the following descriptor appears to include itself:
    {}
"###,
                self.descriptor
            )
        }

        let (composite_desc, merged_vars) = uri::try_load_descriptor::<N>(&self.descriptor, vars)?;
        let res_composite = composite_desc.clone().flatten_composite(
            self.id,
            overwriting_configuration,
            merged_vars,
            ancestors,
        );

        if res_composite.is_ok() {
            return res_composite;
        }

        // Deserializations from Operator and CompositeOperator failed: we return both errors.
        bail!(
            r###"
Failed to deserialize descriptor either as non-Composite or as Composite:
----------------------------
Descriptor:
{}

----------------------------
- non-Composite deserialization attempt:
{:?}

- Composite deserialization attempt:
{:?}
"###,
            composite_desc,
            res_node.unwrap_err(),
            res_composite.unwrap_err()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success() {
        let yaml = r#"
id: my-custom-correct-id
descriptor: file:///dev/null
configuration:
  answer: 42
"#;

        let expected_node = NodeDescriptor {
            id: "my-custom-correct-id".into(),
            descriptor: "file:///dev/null".into(),
            configuration: Configuration::from(serde_json::json!({ "answer": 42 })),
        };
        assert_eq!(
            expected_node,
            serde_yaml::from_str::<NodeDescriptor>(yaml).unwrap()
        )
    }

    #[test]
    fn test_name_with_slash_rejected() {
        let yaml = r#"
id: my-custom/wrong-id
descriptor: file:///dev/null
configuration:
  answer: 42
"#;
        assert!(format!(
            "{:?}",
            serde_yaml::from_str::<NodeDescriptor>(yaml).err().unwrap()
        )
        .contains("A NodeId cannot contain any '/'"))
    }
}
