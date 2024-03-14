//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use crate::{loader::Loader, Extensions, Runtime};

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{anyhow, bail};
use async_std::sync::{Mutex, RwLock};
use uhlc::HLC;
use zenoh::prelude::r#async::*;
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;
use zenoh_flow_commons::{Result, RuntimeId};

/// Builder structure to help create a [Runtime].
///
/// Most of the internals of the Runtime can left to their default values. Leveraging a builder pattern thus simplifies
/// the creation of a Runtime.
pub struct RuntimeBuilder {
    name: Arc<str>,
    hlc: Option<HLC>,
    runtime_id: Option<RuntimeId>,
    #[cfg(feature = "zenoh")]
    session: Option<Arc<Session>>,
    #[cfg(feature = "shared-memory")]
    shared_memory: Option<SharedMemoryConfiguration>,
    loader: Loader,
}

impl RuntimeBuilder {
    /// Creates a new `RuntimeBuilder` with default parameters.
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into().into(),
            hlc: None,
            runtime_id: None,
            #[cfg(feature = "zenoh")]
            session: None,
            loader: Loader::default(),
        }
    }

    /// Forces the identifier of the Runtime to be build.
    ///
    /// If no [Session] is provided, this identifier will be forced on the [Session].
    ///
    /// # Errors
    ///
    /// This method will fail if the Zenoh feature is enabled (it is by default) and a [Session] was already
    /// provided. Zenoh-Flow will re-use the unique identifier of the Zenoh session for the Runtime.
    ///
    /// ⚠️ *If the runtime identifier is set first and a Session is provided after, the runtime identifier will be
    /// overwritten by the identifier of the Zenoh session*.
    pub fn runtime_id(mut self, runtime_id: impl Into<RuntimeId>) -> Result<Self> {
        self.runtime_id = Some(runtime_id.into());

        #[cfg(feature = "zenoh")]
        if self.session.is_some() {
            bail!(
                "Cannot set the identifier of this runtime, a Zenoh session was already provided"
            );
        }

        Ok(self)
    }

    #[cfg(feature = "shared-memory")]
    pub fn shared_memory(mut self, shm: SharedMemoryConfiguration) -> Self {
        self.shared_memory = Some(shm);
        self
    }

    /// Forces the [Session] the Runtime should use to communicate over Zenoh.
    ///
    /// # Runtime identifier
    ///
    /// If a [Session] is provided, the Zenoh-Flow runtime will re-use the identifier of the Session as its identifier.
    #[cfg(feature = "zenoh")]
    pub fn session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Forces the hybrid logical clock the Runtime should use.
    pub fn hlc(mut self, hlc: HLC) -> Self {
        self.hlc = Some(hlc);
        self
    }

    /// Attempts to add the provided [Extensions] to the list of extensions supported by this Runtime.
    ///
    /// If previous extensions were already declared for the same file extension, the newly added extensions will
    /// override the previous values.
    ///
    /// # Errors
    ///
    /// This method will fail if any of the extension is not valid. See [here](Extensions::try_add_extension) for a
    /// complete list of error cases.
    ///
    /// Note that the extensions are added *after* checking them all.
    pub fn add_extensions(mut self, extensions: Extensions) -> Result<Self> {
        for extension in extensions.values() {
            extension.libraries.validate()?;
        }

        self.loader.extend::<HashMap<_, _>>(extensions.into());

        Ok(self)
    }

    /// Attempts to add a single [Extension](crate::Extension) to the list of extensions supported by this Runtime.
    ///
    /// If a previous extension was already declared for the same file extension, the newly added extension will
    /// override the previous value.
    ///
    /// # Errors
    ///
    /// This method will fail if the extension is not valid. See [here](Extensions::try_add_extension) for a complete
    /// list of error cases.
    pub fn add_extension(
        mut self,
        file_extension: impl Into<String>,
        source: impl Into<PathBuf>,
        operator: impl Into<PathBuf>,
        sink: impl Into<PathBuf>,
    ) -> Result<Self> {
        self.loader
            .try_add_extension(file_extension, source, operator, sink)?;
        Ok(self)
    }

    /// Attempts to build the [Runtime].
    ///
    /// # Errors
    ///
    /// This method can fail if the `zenoh` feature is enabled (it is by default), no [Session] was provided to the
    /// builder and the creation of a Session failed.
    pub async fn build(self) -> Result<Runtime> {
        #[cfg(feature = "zenoh")]
        let session = match self.session {
            Some(session) => session,
            None => {
                let mut zenoh_config = zenoh::config::peer();
                if let Some(runtime_id) = self.runtime_id {
                    // NOTE: `set_id` will return the previous id in one was set before. We can safely ignore this
                    // result.
                    let _ = zenoh_config.set_id(*runtime_id);
                }

                zenoh::open(zenoh_config)
                    .res_async()
                    .await
                    .map_err(|e| anyhow!("Failed to open a Zenoh session in peer mode:\n{e:?}"))?
                    .into_arc()
            }
        };

        #[cfg(not(feature = "zenoh"))]
        let runtime_id = self.runtime_id.unwrap_or_else(RuntimeId::rand);
        #[cfg(feature = "zenoh")]
        let runtime_id = session.zid().into();

        Ok(Runtime {
            name: self.name,
            runtime_id,
            hlc: self
                .hlc
                .map(Arc::new)
                .unwrap_or_else(|| Arc::new(HLC::default())),
            #[cfg(feature = "zenoh")]
            session,
            loader: Mutex::new(self.loader),
            flows: RwLock::new(HashMap::new()),
        })
    }
}
