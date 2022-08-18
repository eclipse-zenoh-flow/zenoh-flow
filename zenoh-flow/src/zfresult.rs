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

use crate::types::{NodeId, PortId, PortType};
use serde::{Deserialize, Serialize};


use anyhow::Error as AnyError;
use std::convert::From;
use std::fmt;
use uhlc::Timestamp;
use uuid::Uuid;

#[macro_export]
macro_rules! zferror {
    ($kind: expr, $source: expr => $($t: tt)*) => {
        $crate::zfresult::ZFError::new($kind, $crate::anyhow!($($t)*), file!(), line!()).set_source($source)
    };
    ($kind: expr, $t: literal) => {
        $crate::zfresult::ZFError::new($kind, $crate::anyhow!($t), file!(), line!())
    };
    ($kind: expr, $t: expr) => {
        $crate::zfresult::ZFError::new($kind, $t, file!(), line!())
    };
    ($kind: expr, $($t: tt)*) => {
        $crate::zfresult::ZFError::new($kind, $crate::anyhow!($($t)*), file!(), line!())
    };
    ($kind: expr) => {
        $crate::zfresult::ZFError::new($kind, $crate::anyhow!("{:?}", $kind), file!(), line!())
    };
}

// @TODO: re-design ZError and macros
// This macro is a shorthand for the creation of a ZError
#[macro_export]
macro_rules! bail{
    ($kind: expr, $($t: tt)*) => {
        return Err($crate::zferror!($kind, $($t)*).into())
    };
}

// Todo for refactoring
// pub trait KindError : std::error::Error {
//     fn get_kind(&self) -> &ErrorKind;
// }


// pub type Error = Box<dyn KindError + Send + Sync + 'static >;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static >;

/// The Zenoh Flow result type.
pub type ZFResult<T> = Result<T, Error>;


pub type DaemonResult<T> = Result<T, ErrorKind>;

/// The Zenoh Flow error
/// It contains mapping to most of the errors that could happen within
/// Zenoh Flow and its dependencies.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ErrorKind {
    GenericError,
    SerializationError,
    DeseralizationError,
    MissingState,
    InvalidState,
    Unimplemented,
    Unsupported,
    Empty,
    NotFound,
    Duplicate,
    MissingConfiguration,
    ConfigurationError(String),
    VersionMismatch,
    Disconnected,
    Uncompleted,
    RecvError,
    SendError,
    MissingInput(String),
    MissingOutput(String),
    InvalidData,
    IOError,
    ZenohError,
    LoadingError,
    ParsingError,
    RunnerStopError,
    RunnerStopSendError,
    InstanceNotFound(Uuid),
    RPCError,
    SourceDoNotHaveInputs,
    ReceiverDoNotHaveInputs,
    SinkDoNotHaveOutputs,
    SenderDoNotHaveOutputs,
    // Validation Error
    DuplicatedNodeId(NodeId),
    DuplicatedPort((NodeId, PortId)),
    DuplicatedLink(((NodeId, PortId), (NodeId, PortId))),
    MultipleOutputsToInput((NodeId, PortId)),
    PortTypeNotMatching((PortType, PortType)),
    NodeNotFound(NodeId),
    PortNotFound((NodeId, PortId)),
    PortNotConnected((NodeId, PortId)),
    NotRecording,
    AlreadyRecording,
    NoPathBetweenNodes(((NodeId, PortId), (NodeId, PortId))),
    BelowWatermarkTimestamp(Timestamp),
}

#[derive(Serialize, Deserialize)]
pub struct ZFError {
    kind: ErrorKind,
    #[serde(skip_serializing, skip_deserializing)]
    error: Option<AnyError>,
    file: &'static str,
    line: u32,
    #[serde(skip_serializing, skip_deserializing)]
    source: Option<Error>,
}

unsafe impl Send for ZFError {}
unsafe impl Sync for ZFError {}

impl ZFError {
    pub fn new<E: Into<AnyError>>(
        kind: ErrorKind,
        error: E,
        file: &'static str,
        line: u32,
    ) -> ZFError {
        ZFError {
            kind,
            error: Some(error.into()),
            file,
            line,
            source: None,
        }
    }
    pub fn set_source<S: Into<Error>>(mut self, source: S) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn get_kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl std::error::Error for ZFError {
    fn source(&self) -> Option<&'_ (dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|r| unsafe { std::mem::transmute(r.as_ref()) })
    }
}
impl fmt::Debug for ZFError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

impl fmt::Display for ZFError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} {:?} at {}:{}.",
            self.kind, self.error, self.file, self.line
        )?;
        if let Some(s) = &self.source {
            write!(f, " - Caused by {}", *s)?;
        }
        Ok(())
    }
}

// impl KindError for ZFError {
//     fn get_kind(&self) -> &ErrorKind {
//         &self.kind
//     }
// }

// impl KindError for dyn std::error::Error {
//     fn get_kind(&self) -> &ErrorKind {
//         &ErrorKind::GenericError
//     }
// }

impl From<zrpc::zrpcresult::ZRPCError> for ZFError {
    fn from(err: zrpc::zrpcresult::ZRPCError) -> Self {
        zferror!(ErrorKind::RPCError, err)
    }
}

impl From<flume::RecvError> for ZFError {
    fn from(err: flume::RecvError) -> Self {
        zferror!(ErrorKind::RecvError, err)
    }
}

impl From<async_std::channel::RecvError> for ZFError {
    fn from(err: async_std::channel::RecvError) -> Self {
        zferror!(ErrorKind::RunnerStopError, err)
    }
}

impl From<async_std::channel::SendError<()>> for ZFError {
    fn from(err: async_std::channel::SendError<()>) -> Self {
        zferror!(ErrorKind::RunnerStopSendError, err)
    }
}

impl From<flume::TryRecvError> for ZFError {
    fn from(err: flume::TryRecvError) -> Self {
        match err {
            flume::TryRecvError::Disconnected => zferror!(ErrorKind::Disconnected, err),
            flume::TryRecvError::Empty => zferror!(ErrorKind::Empty, err),
        }
    }
}

impl<T> From<flume::SendError<T>> for ZFError {
    fn from(err: flume::SendError<T>) -> Self {
        zferror!(ErrorKind::SendError, "{}", err)
    }
}

impl From<std::io::Error> for ZFError {
    fn from(err: std::io::Error) -> Self {
        zferror!(ErrorKind::IOError, err)
    }
}

impl From<zenoh_util::core::Error> for ZFError {
    fn from(err: zenoh_util::core::Error) -> Self {
        zferror!(ErrorKind::ZenohError, "{}", err)
    }
}

impl From<libloading::Error> for ZFError {
    fn from(err: libloading::Error) -> Self {
        zferror!(ErrorKind::LoadingError, err)
    }
}

#[cfg(feature = "data_json")]
impl From<serde_json::Error> for ZFError {
    fn from(err: serde_json::Error) -> Self {
        zferror!(ErrorKind::SerializationError, err)
    }
}

#[cfg(feature = "data_json")]
impl From<std::str::Utf8Error> for ZFError {
    fn from(err: std::str::Utf8Error) -> Self {
        zferror!(ErrorKind::SerializationError, err)
    }
}


impl From<ZFError> for ErrorKind {
    fn from(err: ZFError) -> Self {
        err.get_kind().clone()
    }
}


impl From<Box<dyn std::error::Error + Send + Sync>> for ErrorKind {
    fn from(_err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ErrorKind::GenericError
    }
}

impl From<zrpc::zrpcresult::ZRPCError> for ErrorKind {
    fn from(_err: zrpc::zrpcresult::ZRPCError) -> Self {
        ErrorKind::RPCError
    }
}