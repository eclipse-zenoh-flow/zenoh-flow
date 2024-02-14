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

/// Types which can be combined with another instance of the same type and for which, in case there are common elements,
/// the elements of `self` would be kept over those held by `other`.
///
/// For instance, a map-like type with keys shared by both would see the associated values in `self` preserved.
///
/// This trait is leveraged in Zenoh-Flow for the [Configuration](crate::Configuration) and the [Vars](crate::Vars).
pub trait IMergeOverwrite {
    fn merge_overwrite(self, other: Self) -> Self;
}
