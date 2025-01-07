use std::path::PathBuf;

use anyhow::Context;
use async_std::io::ReadExt;
use clap::Parser;
use zenoh::Session;
use zenoh_flow_commons::{parse_vars, Result, Vars};
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::{Extensions, Runtime};

#[derive(Parser)]
pub struct RunLocalCommand {
    /// The data flow to execute.
    flow: PathBuf,
    /// The, optional, location of the configuration to load nodes implemented not in Rust.
    #[arg(short, long, value_name = "path")]
    extensions: Option<PathBuf>,
    /// Variables to add / overwrite in the `vars` section of your data
    /// flow, with the form `KEY=VALUE`. Can be repeated multiple times.
    ///
    /// Example:
    ///     --vars HOME_DIR=/home/zenoh-flow --vars BUILD=debug
    #[arg(long, value_parser = parse_vars::<String, String>, verbatim_doc_comment)]
    vars: Option<Vec<(String, String)>>,
}

impl RunLocalCommand {
    pub async fn run(self, session: Session) -> Result<()> {
        let extensions = match self.extensions {
            Some(extensions_path) => {
                let (extensions, _) = zenoh_flow_commons::try_parse_from_file::<Extensions>(
                    extensions_path.as_os_str(),
                    Vars::default(),
                )
                .context(format!(
                    "Failed to load Loader configuration from < {} >",
                    &extensions_path.display()
                ))
                .unwrap();

                extensions
            }
            None => Extensions::default(),
        };

        let vars = match self.vars {
            Some(v) => Vars::from(v),
            None => Vars::default(),
        };

        let (data_flow, vars) = zenoh_flow_commons::try_parse_from_file::<DataFlowDescriptor>(
            self.flow.as_os_str(),
            vars,
        )
        .context(format!(
            "Failed to load data flow descriptor from < {} >",
            &self.flow.display()
        ))
        .unwrap();

        let flattened_flow = FlattenedDataFlowDescriptor::try_flatten(data_flow, vars)
            .context(format!(
                "Failed to flattened data flow extracted from < {} >",
                &self.flow.display()
            ))
            .unwrap();

        let runtime_builder = Runtime::builder("zenoh-flow-standalone-runtime")
            .add_extensions(extensions)
            .expect("Failed to add extensions")
            .session(session);

        let runtime = runtime_builder
            .build()
            .await
            .expect("Failed to build the Zenoh-Flow runtime");

        let record = DataFlowRecord::try_new(&flattened_flow, runtime.id())
            .context("Failed to create a Record from the flattened data flow descriptor")
            .unwrap();

        let instance_id = record.instance_id().clone();
        let record_name = record.name().clone();
        runtime
            .try_load_data_flow(record)
            .await
            .context("Failed to load Record")
            .unwrap();

        runtime
            .try_start_instance(&instance_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to start data flow < {} >: {:?}", &instance_id, e));

        let mut stdin = async_std::io::stdin();
        let mut input = [0_u8];
        println!(
            r#"
                        The flow ({}) < {} > was successfully started.
                        To abort its execution, simply enter 'q'.
                        "#,
            record_name, instance_id
        );

        loop {
            let _ = stdin.read_exact(&mut input).await;
            if input[0] == b'q' {
                break;
            }
        }

        runtime
            .try_delete_instance(&instance_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to delete data flow < {} >: {:?}", &instance_id, e));

        Ok(())
    }
}
