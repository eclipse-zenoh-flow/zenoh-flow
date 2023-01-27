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

//!
//! This module implements the [`ZFData`](`ZFData`) trait for some basic types: signed and unsigned
//! integers, floats, Strings, booleans.
//!
//! The implementation is (for now) rather naïve and thus not the most efficient.
//!
//! ## Caveat: `Vec<u8>`
//!
//! Given that data can be received serialized (i.e. in a `Vec<u8>` form), the `ZFData` trait should
//! not be implemented for `Vec<u8>` directly. Indeed, if we do, then when trying to downcast after
//! deserializing through `<Typed>::try_deserialize` Rust will assume that the actual type of the
//! data is still `Vec<u8>` and not `<Typed>`.
//!
//! ## Todo
//!
//! - Optimize with additional Reader and Writer traits so as to not require an allocation when
//!   serializing (i.e. we provide a `&mut Writer` that Zenoh-Flow controls).
//! - Investigate supporting ProtoBuf and Cap'n'Proto.
//!

use crate::prelude::{DowncastAny, ErrorKind, ZFData};
use crate::{bail, zferror};
use std::convert::TryInto;

/*
 *
 * Unsigned integers
 *
 */

impl DowncastAny for usize {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for usize {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(usize::from_le_bytes(b))
    }
}

impl DowncastAny for u128 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for u128 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(u128::from_le_bytes(b))
    }
}

impl DowncastAny for u64 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for u64 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(u64::from_le_bytes(b))
    }
}

impl DowncastAny for u32 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for u32 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(u32::from_le_bytes(b))
    }
}

impl DowncastAny for u16 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for u16 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(u16::from_le_bytes(b))
    }
}

impl DowncastAny for u8 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for u8 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(u8::from_le_bytes(b))
    }
}

/*
 *
 * Signed integers
 *
 */

impl DowncastAny for isize {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for isize {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(isize::from_le_bytes(b))
    }
}

impl DowncastAny for i128 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for i128 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(i128::from_le_bytes(b))
    }
}

impl DowncastAny for i64 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for i64 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(i64::from_le_bytes(b))
    }
}

impl DowncastAny for i32 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for i32 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(i32::from_le_bytes(b))
    }
}

impl DowncastAny for i16 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for i16 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(i16::from_le_bytes(b))
    }
}

impl DowncastAny for i8 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for i8 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(i8::from_le_bytes(b))
    }
}

/*
 *
 * Float
 *
 */

impl DowncastAny for f64 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for f64 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(f64::from_le_bytes(b))
    }
}

impl DowncastAny for f32 {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for f32 {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.to_le_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let b = bytes
            .try_into()
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?;

        Ok(f32::from_le_bytes(b))
    }
}

/*
 *
 * Boolean
 *
 */

impl DowncastAny for bool {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for bool {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(match self {
            true => vec![1u8],
            false => vec![0u8],
        })
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        if bytes.len() != 1 {
            bail!(ErrorKind::DeserializationError)
        }

        match bytes[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => bail!(ErrorKind::DeserializationError),
        }
    }
}

/*
 *
 * String
 *
 */

impl DowncastAny for String {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for String {
    fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }

    fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(String::from_utf8(bytes.to_vec())
            .map_err(|e| zferror!(ErrorKind::DeserializationError, e))?)
    }
}

impl<T: prost::Message> DowncastAny for T {
    default fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    default fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl<T: prost::Message> ZFData for T {
    default fn try_serialize(&self) -> crate::Result<Vec<u8>> {
        Ok(self.encode_to_vec())
    }

    default fn try_deserialize(bytes: &[u8]) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(T::decode(bytes).map_err(|e| zferror!(ErrorKind::DeserializationError, e))?)
    }
}
