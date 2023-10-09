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

use prost::Message as pMessage;
use zenoh_flow::{anyhow, prelude::*};

#[export_operator]
pub struct GreetingsMaker {
    input: Input<String>,
    output: Output<String>,
}

#[async_trait::async_trait]
impl Operator for GreetingsMaker {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        Ok(GreetingsMaker {
            input: inputs
                .take("name")
                .expect("No input 'name' found")
                .typed(|bytes| String::from_utf8(bytes.into()).map_err(|e| anyhow!(e))),
            output: outputs
                .take("greeting")
                .expect("No output 'greeting' found")
                .typed(|buffer, data: &String| data.encode(buffer).map_err(|e| anyhow!(e))),
        })
    }
}

#[async_trait::async_trait]
impl Node for GreetingsMaker {
    async fn iteration(&self) -> Result<()> {
        let (message, _) = self.input.recv().await?;
        if let Message::Data(characters) = message {
            let name = characters.trim_end();

            let greetings = match name {
                "Sofia" | "Leonardo" => format!("Ciao, {}!\n", name),
                "Lucia" | "Martin" => format!("¡Hola, {}!\n", name),
                "Jade" | "Gabriel" => format!("Bonjour, {} !\n", name),
                _ => format!("Hello, {}!\n", name),
            };

            return self.output.send(greetings, None).await;
        }

        Ok(())
    }
}
