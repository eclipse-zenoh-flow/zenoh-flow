//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use async_std::sync::{Mutex, RwLock};
use uhlc::HLC;
use zenoh::Session;
#[cfg(feature = "zenoh")]
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;
use zenoh_flow_commons::{Result, RuntimeId};

use crate::{loader::Loader, Extensions, Runtime};

/// Builder structure to help create a [Runtime].
///
/// Most of the internals of the Runtime can left to their default values. Leveraging a builder pattern thus simplifies
/// the creation of a Runtime.
#[must_use = "The Runtime will not be generated unless you `build()` it"]
pub struct RuntimeBuilder {
    name: Arc<str>,
    hlc: Option<HLC>,
    runtime_id: Option<RuntimeId>,
    #[cfg(feature = "zenoh")]
    session: Option<Session>,
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use zenoh_flow_commons::RuntimeId;
    /// use zenoh_flow_runtime::Runtime;
    ///
    /// let builder = Runtime::builder("demo").runtime_id(RuntimeId::rand());
    /// ```
    pub fn runtime_id(mut self, runtime_id: impl Into<RuntimeId>) -> Result<Self> {
        self.runtime_id = Some(runtime_id.into());

        #[cfg(feature = "zenoh")]
        if self.session.is_some() {
            anyhow::bail!(
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
    ///
    /// # Example
    ///
    /// ```no_run
    /// use zenoh_flow_runtime::{zenoh, Runtime};
    /// # async_std::task::block_on(async {
    ///
    /// let zenoh_session = zenoh::open(zenoh::Config::default())
    ///     .await
    ///     .expect("Failed to open Session");
    /// let builder = Runtime::builder("demo").session(zenoh_session);
    /// # });
    /// ```
    #[cfg(feature = "zenoh")]
    pub fn session(mut self, session: Session) -> Self {
        self.session = Some(session);
        self
    }

    /// Forces the hybrid logical clock the Runtime should use.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use uhlc::HLC;
    /// use zenoh_flow_runtime::Runtime;
    ///
    /// let builder = Runtime::builder("demo").hlc(HLC::default());
    /// ```
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
    /// This method will fail if any of the extension is not valid. See [here](RuntimeBuilder::add_extension) for a
    /// complete list of error cases.
    ///
    /// Note that the extensions are added *after* checking them all.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use zenoh_flow_commons::{try_parse_from_file, Vars};
    /// use zenoh_flow_runtime::{Extensions, Runtime};
    ///
    /// let (extensions, _) =
    ///     try_parse_from_file::<Extensions>("/home/zenoh-flow/extensions.ext", Vars::default())
    ///         .expect("Failed to parse Extensions");
    ///
    /// let builder = Runtime::builder("demo")
    ///     .add_extensions(extensions)
    ///     .expect("Failed to add set of extensions");
    /// ```
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
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol (see these macros: [1], [2], [3]),
    /// - was not compiled with the same Rust version,
    /// - was not using the same version of Zenoh-Flow as this [runtime](crate::Runtime).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use zenoh_flow_runtime::Runtime;
    /// let builder = Runtime::builder("demo")
    ///     .add_extension(
    ///         "py",
    ///         "/home/zenoh-flow/libpy_source.so",
    ///         "/home/zenoh-flow/libpy_operator.so",
    ///         "/home/zenoh-flow/libpy_sink.so",
    ///     )
    ///     .expect("Failed to add 'py' extension");
    /// ```
    ///
    /// [1]: zenoh_flow_nodes::prelude::export_source
    /// [2]: zenoh_flow_nodes::prelude::export_operator
    /// [3]: zenoh_flow_nodes::prelude::export_sink
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
    ///
    /// # Example
    ///
    /// ```
    /// # async_std::task::block_on(async {
    /// use zenoh_flow_runtime::Runtime;
    /// let runtime = Runtime::builder("demo")
    ///     .build()
    ///     .await
    ///     .expect("Failed to build Zenoh-Flow runtime");
    /// # });
    /// ```
    pub async fn build(self) -> Result<Runtime> {
        #[cfg(feature = "zenoh")]
        let session = match self.session {
            Some(session) => session,
            None => {
                let mut zenoh_config = zenoh::Config::default();
                if let Some(runtime_id) = self.runtime_id {
                    // NOTE: `set_id` will return the previous id in one was set before. We can safely ignore this
                    // result.
                    let _ = zenoh_config.set_id(*runtime_id);
                }

                zenoh::open(zenoh_config).await.map_err(|e| {
                    anyhow::anyhow!("Failed to open a Zenoh session in peer mode:\n{e:?}")
                })?
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
