# Eclipse Zenoh-Flow

[![Join the chat at https://gitter.im/atolab/zenoh-flow](https://badges.gitter.im/atolab/zenoh-flow.svg)](https://gitter.im/atolab/zenoh-flow?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Zenoh-Flow provides a zenoh-based dataflow programming framework for computations that span from the cloud to the device.

:warning: **This software is still in alpha status and should _not_ be used in production. Breaking changes are likely to happen and the API is not stable.**

-----------
## Description

Zenoh-Flow allow users to declare a dataflow graph, via a YAML file, and use tags to express location affinity and requirements for the operators that makeup the graph. When deploying the dataflow graph, Zenoh-Flow automatically deals with distribution by linking remote operators through zenoh.

A dataflow is composed of set of _nodes_: _sources_ — producing data, _operators_ — computing over the data, and _sinks_ — consuming the resulting data. These nodes are _dynamically_ loaded at runtime.

Remote source, operators, and sinks leverage zenoh to communicate in a transparent manner. In other terms, the dataflow the dafalow graph retails location transparency and could be deployed in different ways depending on specific needs.

Zenoh-Flow provides several working examples that illustrate how to define operators, sources and sinks as well as how to declaratively define they dataflow graph by means of a YAML file.

-----------
## How to build it

Install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). Zenoh Flow can be successfully compiled with Rust stable (>= 1.5.1), so no special configuration is required — except for certain examples.

To build Zenoh-Flow, just type the following command after having followed the previous instructions:

```bash
$ cargo build --release
```

-----------
## How to run

Assuming that the previous steps completed successfully, you'll find the Zenoh-Flow runtime under `target/release/runtime`. This executable expects the following arguments:

- the path of the dataflow graph to execute: `--graph-file zenoh-flow-examples/graphs/fizz_buzz_pipeline.yaml`,
- a name for the runtime: `--runtime foo`.

The graph describes the different nodes composing the dataflow. Although mandatory, the name of the runtime is used to "deploy" the graph on different "runtime instances" (see the related examples).


-----------
## Creating your nodes

Assuming that the build steps completed successfully, you'll be able to use the `cargo zenoh-flow` subcommand to create a boilerplate for your nodes.
First let's ensure to have the `cargo-zenoh-flow` binary in the Cargo path.

```bash
$ ln -s $(pwd)/target/release/cargo-zenoh-flow ~/.cargo/bin/
```

Then you can create your own node with:

```bash
$ cd ~
$ cargo zenoh-flow new myoperator
```

By default `cargo zenoh-flow` generates the template for an operator. In order to create a source or a sink you need to add either `--kind source` or `--kind sink`.\
The `Cargo.toml` will contain metadata information (eg. the inputs/outputs) used during the build process to generate the descriptor.


More information about the `cargo zenoh-flow` can be obtained using `cargo zenoh-flow --help`.\
You can now modify the `src/lib.rs` file with your business logic and update the `Cargo.toml` according to the `inputs/outputs` that you need.

Once you are done you can build it:

```bash
$ cargo zenoh-flow build
```

It will provide you the path of the descriptor for the new node, that can be used inside a flow descriptor.

-----------
## Examples

### FizzBuzz

First, compile the relevant examples:

```bash
cargo build --example manual-source --example example-fizz --example example-buzz --example generic-sink
```

This will create, depending on your OS, the libraries that the pipeline will fetch.

#### Single runtime

To run all nodes on the same Zenoh Flow runtime:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/fizz_buzz_pipeline.yaml --runtime foo
```

_Note: in that particular case the `--runtime foo` is discarded._

:warning: _Note: the zenoh flow description  `fizz_buzz_pipeline.yaml`  is currently prepared to link libraries in MacOS environment (`.dylib`). Change the libraries extension accordingly if you run on a different OS, e.g. `.dylib`->`.so`  under Unix._

#### Multiple runtimes

In a first machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/fizz-buzz-multiple-runtimes.yaml --runtime foo
```

In a second machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/fizz-buzz-multiple-runtimes.yaml --runtime bar
```

:warning: If you change the name of the runtime in the yaml file, the name(s) passed as argument of the previous commands must be changed accordingly.

:warning: Without configuration, the different machines need to be on the _same local network_ for this example to work. See how to add a [Zenoh router](https://zenoh.io/docs/getting-started/key-concepts/#zenoh-router) if you want to connect them through the internet.

---

### OpenCV FaceDetection - Haarcascades

:warning: This example works only on Linux and it require OpenCV to be installed, please follow the instruction on the [OpenCV documentation](https://docs.opencv.org/4.5.2/d7/d9f/tutorial_linux_install.html) to install it.

:warning: You need a machine equipped of a webcam in order to run this example.

First, compile the relevant examples:

```bash
cargo build --example camera-source --example face-detection --example video-sink
```

This will create, depending on your OS, the libraries that the pipeline will fetch.

#### Single runtime

To run all nodes on the same Zenoh Flow runtime:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/face_detection.yaml --runtime foo
```

_Note: in that particular case the `--runtime foo` is discarded._

#### Multiple runtimes

In a first machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/face-detection-multi-runtime.yaml --runtime gigot
```

In a second machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/face-detection-multi-runtime.yaml --runtime nuc
```

In a third machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/face-detection-multi-runtime.yaml --runtime leia
```

:warning: If you change the name of the runtime in the yaml file, the name(s) passed as argument of the previous commands must be changed accordingly.

:warning: Without configuration, the different machines need to be on the _same local network_ for this example to work. See how to add a [Zenoh router](https://zenoh.io/docs/getting-started/key-concepts/#zenoh-router) if you want to connect them through the internet.

---

### OpenCV Object Detection - Deep Neural Network - CUDA powered

:warning: This example works only on Linux and it require OpenCV with CUDA enabled to be installed, please follow the instruction on [this gits](https://gist.github.com/raulqf/f42c718a658cddc16f9df07ecc627be7) to install it.

:warning: This example works only on Linux and it require a **CUDA** capable **NVIDIA GPU**, as well as NVIDIA CUDA and CuDNN to be installed, please follow [CUDA instructions](https://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html) and [CuDNN instructions](https://docs.nvidia.com/deeplearning/cudnn/install-guide/index.html).

:warning: You need a machine equipped of a webcam in order to run this example.

:warning: You need to download a YOLOv3 configuration, weights and classes, you can use the ones from [this GitHub repository](https://github.com/sthanhng/yoloface).

First, compile the relevant examples:

```bash
cargo build --example camera-source --example object-detection-dnn --example video-sink
```

This will create, depending on your OS, the libraries that the pipeline will fetch.

Then please update the files `zenoh-flow-examples/graphs/dnn-object-detection.yaml` and `zenoh-flow-examples/graphs/dnn-object-detection-multi-runtime.yaml` by changing the `neural-network`, `network-weights`, and `network-classes` to match the absolute path of your *Neural Network* configuration

#### Single runtime

To run all nodes on the same Zenoh Flow runtime:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/dnn-object-detection.yaml --runtime foo
```

_Note: in that particular case the `--runtime foo` is discarded._

#### Multiple runtimes

In a first machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/dnn-object-detection-multi-runtime.yaml --runtime foo
```

In a second machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/dnn-object-detection-multi-runtime.yaml --runtime cuda
```

In a third machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/dnn-object-detection-multi-runtime.yaml --runtime bar
```

:warning: If you change the name of the runtime in the yaml file, the name(s) passed as argument of the previous commands must be changed accordingly.

:warning: Without configuration, the different machines need to be on the _same local network_ for this example to work. See how to add a [Zenoh router](https://zenoh.io/docs/getting-started/key-concepts/#zenoh-router) if you want to connect them through the internet.


### OpenCV Car Vision - Deep Neural Network - CUDA powered

![Car vision dataflow](./car-pipeline.png)

:warning: This example works only on Linux and it require OpenCV with CUDA enabled to be installed, please follow the instruction on [this gits](https://gist.github.com/raulqf/f42c718a658cddc16f9df07ecc627be7) to install it.

:warning: This example works only on Linux and it require a **CUDA** capable **NVIDIA GPU**, as well as NVIDIA CUDA and CuDNN to be installed, please follow [CUDA instructions](https://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html) and [CuDNN instructions](https://docs.nvidia.com/deeplearning/cudnn/install-guide/index.html).

:warning: You need a machine equipped of a webcam in order to run this example.

:warning: You need to download a YOLOv3 configuration, weights and classes, you can use the ones from [this GitHub repository](https://github.com/tooth2/YOLOv3-Object-Detection/tree/main/dat/yolo).

:warning: You need to download a camera car video,  you can use the ones from [this data set](https://www.mrpt.org/Karlsruhe_Dataset_Rawlog_Format).
This dataset contains the frames, in oder to merge them in video you need `ffmpeg` and run the following command: `ffmpeg -framerate 15 -pattern_type glob -i 'I1*.png' -c:v libx264 I1.mp4`.

First, compile the relevant examples:

```bash
cargo build --example video-file-source --example object-detection-dnn --example video-sink
```

This will create, depending on your OS, the libraries that the pipeline will fetch.

Then please edit the file `zenoh-flow-examples/graphs/car-pipeline-multi-runtime.yaml` by changing the `neural-network`, `network-weights`, and `network-classes` to match the absolute path of your *Neural Network* configuration.

You also need to edit the `file` in `zenoh-flow-examples/graphs/car-pipeline-multi-runtime.yaml` to match the absolute path of your video file.

#### Multiple runtimes

In a first machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/car-pipeline-multi-runtime.yaml --runtime gigot
```

In a second machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/car-pipeline-multi-runtime.yaml --runtime cuda
```

In a third machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/car-pipeline-multi-runtime.yaml --runtime macbook
```

:warning: If you change the name of the runtime in the yaml file, the name(s) passed as argument of the previous commands must be changed accordingly.

:warning: Without configuration, the different machines need to be on the _same local network_ for this example to work. See how to add a [Zenoh router](https://zenoh.io/docs/getting-started/key-concepts/#zenoh-router) if you want to connect them through the internet.
