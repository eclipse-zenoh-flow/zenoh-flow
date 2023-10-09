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

use async_std::prelude::FutureExt;
use async_std::sync::{Arc, Mutex};
use prost::Message as pMessage;
use std::time::{Duration, Instant};
use zenoh_flow::{anyhow, prelude::*};

#[export_operator]
pub struct PeriodMissDetector {
    input: Input<String>,
    output: Output<String>,
    period_duration: Duration,
    next_period: Arc<Mutex<Instant>>,
}

#[async_trait::async_trait]
impl Operator for PeriodMissDetector {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        let period_duration = Duration::from_secs(5);
        Ok(PeriodMissDetector {
            input: inputs
                .take("in")
                .expect("No input 'in' found")
                .typed(|bytes| String::from_utf8(bytes.into()).map_err(|e| anyhow!(e))),
            output: outputs
                .take("out")
                .expect("No output 'out' found")
                .typed(|buffer, data: &String| data.encode(buffer).map_err(|e| anyhow!(e))),
            // CAVEAT: There can be a delay between the moment the node is created and the moment it
            // is actually run.
            next_period: Arc::new(Mutex::new(
                Instant::now().checked_add(period_duration).unwrap(),
            )),
            period_duration,
        })
    }
}

#[async_trait::async_trait]
impl Node for PeriodMissDetector {
    async fn iteration(&self) -> Result<()> {
        let mut next_period = self.next_period.lock().await;
        let now = Instant::now();

        let sleep_duration = match next_period.checked_duration_since(now) {
            Some(sleep_duration) => sleep_duration,
            None => {
                *next_period = next_period
                    .checked_add(self.period_duration)
                    .expect("Could not add duration");
                self.period_duration
            }
        };

        drop(next_period); // explicitely release the lock

        let default = async {
            async_std::task::sleep(sleep_duration).await;
            self.output
                .send(String::from("(default) 0\n"), None)
                .await
                .expect("output channel disconnected");

            let mut next_period = self.next_period.lock().await;
            *next_period = next_period
                .checked_add(self.period_duration)
                .expect("Could not add duration");
        };

        let run = async {
            let (message, _) = self.input.recv().await.expect("input channel disconnected");
            if let Message::Data(data) = message {
                if let Ok(number) = data.trim_end().parse::<f64>() {
                    self.output
                        .send(format!("Received: {number}\n"), None)
                        .await
                        .expect("output channel disconnected");

                    // We just sent a value, if we are within a period (i.e. `next_period` is less
                    // than `period_duration` away) we can safely increase the value of
                    // `next_period` by a single period.
                    let mut next_period = self.next_period.lock().await;
                    let now = Instant::now();
                    if let Some(interval) = next_period.checked_duration_since(now) {
                        if interval < self.period_duration {
                            *next_period = next_period.checked_add(self.period_duration).unwrap();
                        }
                    } else {
                        // This else clause is an edge case: we sent the value riiiiiight before the
                        // end. So by the time we are reaching this code, `now` is later than
                        // `next_period`. Considering that we are executing this code, we still
                        // received data before reaching the next period so we can also safely
                        // increase by one period.
                        *next_period = next_period.checked_add(self.period_duration).unwrap();
                    }
                }
            }
        };

        run.race(default).await;

        Ok(())
    }
}
