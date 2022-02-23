//
// Copyright (c) 2017, 2022 ADLINK Technology Inc.
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

use std::fs;
use std::io::Write;
use std::path::Path;
use zenoh_flow::ZFResult;

/// Helper function to read a file into a string.
///
/// # Errors
/// It returns an error variant if unable o read the file.
pub(crate) fn read_file(path: &Path) -> ZFResult<String> {
    Ok(fs::read_to_string(path)?)
}

/// Helper function to write a file.
///
/// # Errors
///
/// It returns an error varian it unable to create or write the file.
pub(crate) fn _write_file(path: &Path, content: Vec<u8>) -> ZFResult<()> {
    // FIXME.
    let mut file = fs::File::create(path)?;
    file.write_all(&content)?;
    Ok(file.sync_all()?)
}
