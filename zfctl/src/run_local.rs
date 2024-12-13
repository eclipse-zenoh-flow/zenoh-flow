use std::path::PathBuf;

use anyhow::Context;
use async_std::io::ReadExt;
use zenoh_flow_commons::{Result, Vars};
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::{Extensions, Runtime};

pub async fn run_locally(
    flow: PathBuf,
    zenoh_configuration: Option<PathBuf>,
    extensions: Option<PathBuf>,
    vars: Option<Vec<(String, String)>>,
) -> Result<()> {
    let extensions = match extensions {
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

    let vars = match vars {
        Some(v) => Vars::from(v),
        None => Vars::default(),
    };

    let (data_flow, vars) =
        zenoh_flow_commons::try_parse_from_file::<DataFlowDescriptor>(flow.as_os_str(), vars)
            .context(format!(
                "Failed to load data flow descriptor from < {} >",
                &flow.display()
            ))
            .unwrap();

    let flattened_flow = FlattenedDataFlowDescriptor::try_flatten(data_flow, vars)
        .context(format!(
            "Failed to flattened data flow extracted from < {} >",
            &flow.display()
        ))
        .unwrap();

    let mut runtime_builder = Runtime::builder("zenoh-flow-standalone-runtime")
        .add_extensions(extensions)
        .expect("Failed to add extensions");

    if let Some(path) = zenoh_configuration {
        let zenoh_config = zenoh_flow_runtime::zenoh::Config::from_file(path.clone())
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to parse the Zenoh configuration from < {} >:\n{e:?}",
                    path.display()
                )
            });
        let zenoh_session = zenoh_flow_runtime::zenoh::open(zenoh_config)
            .await
            .unwrap_or_else(|e| panic!("Failed to open a Zenoh session: {e:?}"));

        runtime_builder = runtime_builder.session(zenoh_session);
    }

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
