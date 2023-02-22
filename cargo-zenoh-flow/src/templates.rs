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

use crate::{CZFError, CZFResult};
use serde::Serialize;
use tinytemplate::TinyTemplate;

static CARGO_OPERATOR_TEMPLATE: &str = r#"
zenoh-flow = \{ version = "=0.4.0-alpha.2"}

[lib]
name = "{name}"
crate-type=["cdylib"]
path="src/lib.rs"

[package.metadata.zenohflow]
id = "{name}"
kind = "operator"
inputs=[ \{id ="INPUT", type="bytes"}]
outputs=[ \{id ="OUTPUT", type="bytes"}]

"#;

static CARGO_SOURCE_TEMPLATE: &str = r#"
zenoh-flow = \{ version = "=0.4.0-alpha.2"}
async-trait = "0.1"

[lib]
name = "{name}"
crate-type=["cdylib"]
path="src/lib.rs"

[package.metadata.zenohflow]
id = "{name}"
kind = "source"
outputs=[ \{id ="Data", type="bytes"}]

"#;

static CARGO_SINK_TEMPLATE: &str = r#"
zenoh-flow = \{ version = "=0.4.0-alpha.2"}
async-trait = "0.1"

[lib]
name = "{name}"
crate-type=["cdylib"]
path="src/lib.rs"

[package.metadata.zenohflow]
id = "{name}"
kind = "sink"
inputs=[ \{id ="Data", type="bytes"}]

"#;

static LIB_OPERATOR_TEMPLATE: &str = r#"
use async_trait::async_trait;
use std::sync::Arc;
use zenoh_flow::prelude::*;

#[export_operator]
struct {name};

#[async_trait]
impl Operator for {name} \{
    fn new(
        context: Context,
        configuration: Option<Configuration>,
        inputs: Inputs,
        outputs: Outputs,
) -> Result<Self>
   where
   Self: Sized \{
        todo!()
    }
}

#[async_trait]
impl Node for {name} \{
    async fn iteration(&self) -> Result<()> \{
        todo!()
    }
}
"#;

static LIB_SOURCE_TEMPLATE: &str = r#"
use async_trait::async_trait;
use std::sync::Arc;
use zenoh_flow::prelude::*;

#[export_source]
pub struct {name};

#[async_trait]
impl Source for {name} \{
   async fn new(
       context: Context,
       configuration: Option<Configuration>,
       outputs: Outputs,
    ) -> Result<Self> \{
        todo!()
    }
}

#[async_trait]
impl Node for {name} \{
    async fn iteration(&self) -> Result<()> \{
        todo!()
    }
}


"#;

static LIB_SINK_TEMPLATE: &str = r#"
use async_trait::async_trait;
use std::sync::Arc;
use zenoh_flow::prelude::*;

#[export_sink]
pub struct {name};

#[async_trait]
impl Sink for {name} \{
  async fn new(
      context: Context,
      configuration: Option<Configuration>,
      inputs: Inputs,
  ) -> Result<Self> \{
        todo!()
    }
}
#[async_trait]
impl Node for {name} \{
    async fn iteration(&self) -> Result<()> \{
        todo!()
    }
"#;

static PY_SINK_TEMPLATE: &str = r#"
from zenoh_flow.interfaces import Sink
from zenoh_flow import Input
from zenoh_flow.types import Context
from typing import Dict, Any
import asyncio


class {name}(Sink):
    def finalize(self):
        raise NotImplementedError("Please implement your own method")

    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
    ):
        raise NotImplementedError("Please implement your own method")

    async def iteration(self) -> None:
        # in order to wait on multiple input streams use:
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.gather
        # or
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.wait

        raise NotImplementedError("Please implement your own method")


def register():
    return {name}

"#;

static PY_SRC_TEMPATE: &str = r#"
from zenoh_flow.interfaces import Source
from zenoh_flow import Output
from zenoh_flow.types import Context
from typing import Any, Dict
import time
import asyncio


class {name}(Source):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        outputs: Dict[str, Output],
    ):
        raise NotImplementedError("Please implement your own method")

    def finalize(self) -> None:
        return None

    def produce_data(self):
        raise NotImplementedError("Please implement your own method")

    async def iteration(self) -> None:
        raise NotImplementedError("Please implement your own method")




def register():
    return {name}

"#;

static PY_OP_TEMPLATE: &str = r#"
from zenoh_flow.interfaces import Operator
from zenoh_flow import Input, Output
from zenoh_flow.types import Context
from typing import Dict, Any
import asyncio


class {name}(Operator):
    def __init__(
        self,
        context: Context,
        configuration: Dict[str, Any],
        inputs: Dict[str, Input],
        outputs: Dict[str, Output],
    ):
        raise NotImplementedError("Please implement your own method")

    def finalize(self) -> None:
        raise NotImplementedError("Please implement your own method")

    async def iteration(self) -> None:
        # in order to wait on multiple input streams use:
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.gather
        # or
        # https://docs.python.org/3/library/asyncio-task.html#asyncio.wait

        raise NotImplementedError("Please implement your own method")

def register():
    return {name}

"#;

#[derive(Serialize)]
struct OperatorContext {
    name: String,
}

fn some_kind_of_uppercase_first_letter(s: &str) -> String {
    let s = str::replace(s, "-", "_");
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

pub fn operator_template_cargo(name: String) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("operator", CARGO_OPERATOR_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(&name),
    };

    tt.render("operator", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn operator_template_lib(name: String) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("operator", LIB_OPERATOR_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(&name),
    };

    tt.render("operator", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn source_template_lib(name: String) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("source", LIB_SOURCE_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(&name),
    };

    tt.render("source", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn source_template_cargo(name: String) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("source", CARGO_SOURCE_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(&name),
    };

    tt.render("source", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn sink_template_lib(name: String) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("sink", LIB_SINK_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(&name),
    };

    tt.render("sink", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn sink_template_cargo(name: String) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("sink", CARGO_SINK_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(&name),
    };

    tt.render("sink", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn source_template_py(name: &str) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("source", PY_SRC_TEMPATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(name),
    };

    tt.render("source", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn operator_template_py(name: &str) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("source", PY_OP_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(name),
    };

    tt.render("source", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}

pub fn sink_template_py(name: &str) -> CZFResult<String> {
    let mut tt = TinyTemplate::new();
    tt.add_template("source", PY_SINK_TEMPLATE)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))?;

    let ctx = OperatorContext {
        name: some_kind_of_uppercase_first_letter(name),
    };

    tt.render("source", &ctx)
        .map_err(|e| CZFError::GenericError(format!("{}", e)))
}
