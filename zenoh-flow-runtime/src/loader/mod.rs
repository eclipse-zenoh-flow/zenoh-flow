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

mod extensions;
pub use extensions::{Extension, Extensions};

use anyhow::{anyhow, bail, Context};
use libloading::Library;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};
use url::Url;
use zenoh_flow_commons::Result;
use zenoh_flow_nodes::{NodeDeclaration, CORE_VERSION, RUSTC_VERSION};

/// NodeSymbol groups the symbol we must find in the shared library we load.
pub(crate) enum NodeSymbol {
    Source,
    Operator,
    Sink,
}

impl NodeSymbol {
    /// Returns the bytes representation of the symbol.
    ///
    /// They are of the form:
    ///
    /// `b"_zf_export_<node_kind>\0"`
    ///
    /// Where `<node_kind>` is either `operator`, `source`, or `sink`.
    pub(crate) fn to_bytes(&self) -> &[u8] {
        match self {
            NodeSymbol::Source => b"_zf_export_source\0",
            NodeSymbol::Operator => b"_zf_export_operator\0",
            NodeSymbol::Sink => b"_zf_export_sink\0",
        }
    }
}

/// Validates that the library exposes the correct symbols for the provided constructor.
///
/// # Errors
///
/// This function will return an error if:
/// - the provided `node_symbol` is not found in the shared library,
/// - the version of the rust compiler used to compile the node is not the same as the one used to compile the
///   Zenoh-Flow runtime,
/// - the version of Zenoh-Flow used to compile the node is not the same as the one used in the Zenoh-Flow runtime.
pub(crate) fn validate_library<N>(library: &Library, node_symbol: &NodeSymbol) -> Result<()> {
    let decl = unsafe {
        library
            .get::<*mut NodeDeclaration<N>>(node_symbol.to_bytes())?
            .read()
    };

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        if decl.rustc_version != RUSTC_VERSION {
            bail!(
                r#"
It appears that the node was not compiled with the same version of the Rust compiler than Zenoh-Flow:
- (expected, Zenoh-Flow): {}
- (found, Node): {}
"#,
                RUSTC_VERSION,
                decl.rustc_version,
            )
        }

        bail!(
            r#"
It appears that the node was not compiled with the same version of Zenoh-Flow than that of this Zenoh-Flow runtime:
- (expected, this Zenoh-Flow runtime): {}
- (found, Node): {}
"#,
            CORE_VERSION,
            decl.core_version,
        )
    }

    Ok(())
}

/// Tries to get the node constructor from the shared library.
///
/// # Errors
///
/// This function will return an error if the shared library did not pass our validation check, see [validate_library].
pub(crate) fn try_get_constructor<N>(
    library: Arc<Library>,
    node_symbol: &NodeSymbol,
) -> Result<(N, Arc<Library>)> {
    validate_library::<N>(&library, node_symbol)?;

    let decl = unsafe {
        library
            .get::<*mut NodeDeclaration<N>>(node_symbol.to_bytes())?
            .read()
    };

    Ok((decl.constructor, library))
}

/// The dynamic library loader.
///
/// This structure is responsible for:
/// 1. loading the shared libraries containing the implementation of the nodes,
/// 2. keeping track of these libraries to avoid loading several times the same one,
/// 3. leveraging the [Extension]s to load "non-standard" node implementation.
///
/// Note that "non-standard" libraries are libraries that have an extension that is different than
/// [DLL_EXTENSION](std::env::consts::DLL_EXTENSION) --- e.g. different than `.so` on Linux-based systems.
///
/// Before calling the constructor of any node, the loader will perform the following checks:
/// - it will check that the node implementation was compiled with the same version of the Rust compiler than the
///   Zenoh-Flow runtime it belongs to,
/// - it will check that the node implementation was using the same version of the Zenoh-Flow library than the
///   Zenoh-Flow runtime it belongs to.
///
/// To do these checks, the loader is expecting to find specific symbols (different for each type of node). These
/// symbols are automatically exported via the respective procedural macros: [export_source], [export_operator],
/// [export_sink].
///
/// [export_source]: zenoh_flow_nodes::prelude::export_source
/// [export_operator]: zenoh_flow_nodes::prelude::export_operator
/// [export_sink]: zenoh_flow_nodes::prelude::export_sink
#[derive(Default)]
pub(crate) struct Loader {
    pub(crate) extensions: Extensions,
    pub(crate) libraries: HashMap<Url, Arc<Library>>,
}

impl Deref for Loader {
    type Target = HashMap<Arc<str>, Extension>;

    fn deref(&self) -> &Self::Target {
        &self.extensions
    }
}

impl DerefMut for Loader {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.extensions
    }
}

impl Loader {
    /// Attempts to add an extension to this Zenoh-Flow [runtime](crate::Runtime).
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the library:
    /// - does not expose the correct symbol (see these macros: [1], [2], [3]),
    /// - was not compiled with the same Rust version,
    /// - was not using the same Zenoh-Flow version as this Zenoh-Flow [runtime](crate::Runtime).
    ///
    /// [1]: zenoh_flow_nodes::prelude::export_source
    /// [2]: zenoh_flow_nodes::prelude::export_operator
    /// [3]: zenoh_flow_nodes::prelude::export_sink
    pub(crate) fn try_add_extension(
        &mut self,
        file_extension: impl Into<String>,
        source: impl Into<PathBuf>,
        operator: impl Into<PathBuf>,
        sink: impl Into<PathBuf>,
    ) -> Result<Option<Extension>> {
        self.extensions
            .try_add_extension(file_extension, source, operator, sink)
    }

    /// This method will free the shared libraries that are no longer being used.
    ///
    /// Every time a data flow is created, each node will receive an `Arc<Library>` of the shared library it
    /// depends. Once a data flow is deleted, the `strong_count` of all the libraries used is diminished by one and this
    /// method is called after to see if any count has reached 1. If so, that library can be safely dropped.
    pub(crate) fn remove_unused_libraries(&mut self) {
        let number_libraries = self.libraries.len();
        self.libraries
            .retain(|_, library| Arc::strong_count(library) > 1);
        tracing::trace!(
            "Removed {} unused libraries.",
            number_libraries - self.libraries.len()
        );
    }

    /// Given a [Url] and a [NodeSymbol], attempt to load the node constructor.
    ///
    /// This method will first look into its cache of shared libraries and check if it does not already know of a
    /// library with the same Url.
    ///
    /// If it does then it will reuse this library.
    ///
    /// If not, it will attempt to load it and check its compatibility.
    ///
    /// # Errors
    ///
    /// This method can fail for the following reasons:
    /// - the provided Url specifies a scheme that we do not support (for now only "file://" is supported),
    /// - we failed to load the library from the provided Url (e.g. file not found),
    /// - the library does not expose the correct symbol or is not compatible with this Zenoh-Flow runtime.
    pub(crate) fn try_load_constructor<C>(
        &mut self,
        url: &Url,
        node_symbol: &NodeSymbol,
    ) -> Result<(C, Arc<Library>)> {
        if let Some(library) = self.libraries.get(url) {
            return try_get_constructor(library.clone(), node_symbol);
        }

        let library = Arc::new(match url.scheme() {
            "file" => self
                .try_load_library_from_uri(url.path(), node_symbol)
                .context(format!("Failed to load library from file:\n{}", url.path()))?,
            _ => bail!(
                "Unsupported scheme < {} > while trying to load node:\n{}",
                url.scheme(),
                url
            ),
        });

        let (constructor, library) = try_get_constructor::<C>(library, node_symbol)?;
        self.libraries.insert(url.clone(), library.clone());

        Ok((constructor, library))
    }

    /// Given the string representation of a path, attempts to load a library.
    ///
    /// This method will look at the file extension to determine if it should leverage the [Extensions] or not.
    ///
    /// # Errors
    ///
    /// This method can fail if:
    /// - the extension of the path is not supported (i.e. not [DLL_EXTENSION] and not in the [Extensions]),
    /// - there is no file in the provided path,
    /// - the libloading crate failed to create a `Library` using the provided path.
    pub(crate) fn try_load_library_from_uri(
        &self,
        path: &str,
        node_symbol: &NodeSymbol,
    ) -> Result<Library> {
        let path_buf = PathBuf::from_str(path)
            .context(format!("Failed to convert path to a `PathBuf`:\n{}", path))?;

        let library_path = match path_buf.extension().and_then(|ext| ext.to_str()) {
            Some(extension) => {
                if extension == std::env::consts::DLL_EXTENSION {
                    &path_buf
                } else {
                    self.extensions
                        .get_library_path(extension, node_symbol)
                        .ok_or_else(|| {
                            anyhow!(
                                "Cannot load library, no extension found for files of type < {} > :\n{}",
                                extension,
                                path_buf.display()
                            )
                        })?
                }
            }
            None => bail!(
                "Cannot load library, missing file extension:\n{}",
                path_buf.display()
            ),
        };

        let library_path = std::fs::canonicalize(library_path).context(format!(
            "Failed to canonicalize path (did you put an absolute path?):\n{}",
            path_buf.display()
        ))?;

        #[cfg(any(target_family = "unix", target_family = "windows"))]
        Ok(unsafe {
            Library::new(&library_path).context(format!(
                "libloading::Library::new failed:\n{}",
                library_path.display()
            ))?
        })
    }
}
