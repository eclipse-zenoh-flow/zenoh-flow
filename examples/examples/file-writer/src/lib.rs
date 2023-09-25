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

use async_std::{fs::File, io::WriteExt, sync::Mutex};
use prost::Message as pMessage;
use zenoh_flow::{anyhow, prelude::*};

#[export_sink]
pub struct FileWriter {
    input: Input<String>,
    file: Mutex<File>,
}

#[async_trait::async_trait]
impl Node for FileWriter {
    async fn iteration(&self) -> Result<()> {
        let (message, _) = self.input.recv().await?;

        if let Message::Data(greeting) = message {
            let mut file = self.file.lock().await;
            file.write_all(greeting.as_bytes())
                .await
                .map_err(|e| zferror!(ErrorKind::IOError, "{:?}", e))?;
            return file
                .flush()
                .await
                .map_err(|e| zferror!(ErrorKind::IOError, "{:?}", e).into());
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Sink for FileWriter {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Self> {
        Ok(FileWriter {
            file: Mutex::new(
                File::create("/tmp/greetings.txt")
                    .await
                    .expect("Could not create '/tmp/greetings.txt'"),
            ),
            input: inputs
                .take("in")
                .expect("No Input called 'in' found")
                .typed(|bytes| String::decode(bytes).map_err(|e| anyhow!(e))),
        })
    }
}
