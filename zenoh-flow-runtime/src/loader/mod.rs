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
pub use extensions::Extensions;

use anyhow::{anyhow, bail, Context};
use libloading::Library;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use url::Url;
use zenoh_flow_commons::{Result, Vars};
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

/// The dynamic library loader.
/// Before loading it verifies if the versions are compatible
/// and if the symbols are presents.
/// It loads the files in different way depending on the operating system.
/// In particular the scope of the symbols is different between Unix and
/// Windows.
/// In Unix system the symbols are loaded with the flags:
///
/// - `RTLD_NOW` load all the symbols when loading the library.
/// - `RTLD_LOCAL` keep all the symbols local.
#[derive(Default)]
pub struct Loader {
    pub(crate) extensions: Extensions,
    pub(crate) libraries: HashMap<Url, Arc<Library>>,
}

impl Loader {
    /// Creates a new `Loader` with the given `Extensions`.
    pub fn new(extensions: Extensions) -> Self {
        Self {
            extensions,
            libraries: HashMap::default(),
        }
    }

    pub fn remove_unused_libraries(&mut self) {
        let number_libraries = self.libraries.len();
        self.libraries
            .retain(|_, library| Arc::strong_count(library) > 1);
        tracing::trace!(
            "Removed {} unused libraries.",
            number_libraries - self.libraries.len()
        );
    }

    /// TODO@J-Loudet
    pub fn try_from_file(extensions_path: impl AsRef<Path>) -> Result<Self> {
        let (extensions, _) =
            zenoh_flow_commons::try_load_from_file::<Extensions>(extensions_path, Vars::default())?;

        Ok(Self {
            extensions,
            libraries: HashMap::default(),
        })
    }

    pub(crate) fn try_load_constructor<C>(
        &mut self,
        url: &Url,
        node_symbol: &NodeSymbol,
    ) -> Result<(C, Arc<Library>)> {
        if let Some(library) = self.libraries.get(url) {
            return self.try_get_constructor(library.clone(), node_symbol);
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

        let (constructor, library) = self.try_get_constructor::<C>(library, node_symbol)?;
        self.libraries.insert(url.clone(), library.clone());

        Ok((constructor, library))
    }

    /// TODO@J-Loudet
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

    /// TODO@J-Loudet
    fn try_get_constructor<N>(
        &self,
        library: Arc<Library>,
        node_symbol: &NodeSymbol,
    ) -> Result<(N, Arc<Library>)> {
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

        Ok((decl.constructor, library))
    }
}
