import argparse
import fnmatch
import os
import platform
import shutil
import signal
import time

import psutil
import yaml
from yaml.loader import SafeLoader


class ZF_conf:
    def __init__(self):
        self.zenohd_path = None
        self.zf_plugin_path = None
        self.zf_router_path = None
        self.zenohd_process = None
        self.zfctl_path = None
        self.zfctl_json_path = None
        self.zf_deamon_yaml = None
        self.zenoh_flow_daemon = None
        self.uuid_runtimes = None
        self.libzenoh_plugin = None


zf_conf = ZF_conf()

zf_command = {
    "zenohd_cmd": None,
    "runtimes_cmd": None,
    "dataflow_yaml_cmd": None,
    "instances_cmd": None,
}
instance = {
    "uuid_instances": None,
    "flow": None,
    "operators": None,
    "sinks": None,
    "sources": None,
    "connectors": None,
    "links": None,
}


def find_dir(name, path):
    for root, dirs, files in os.walk(path):
        if name in dirs:
            return os.path.join(root, name)


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


pid_zenohd = 0
json_flag = False

zenohflow_release_flag = False

parser = argparse.ArgumentParser(
    description="Script to automatically test that zenoh-flow works"
)

parser.add_argument(
    "-b",
    "--build",
    nargs="?",
    type=str,
    default="False",
    help="Specifies a different directory for Zenoh-Flow artifacts (default=debug)",
)
parser.add_argument(
    "-z",
    "--zenohd",
    nargs="?",
    type=str,
    default="/.local/bin/zenohd",
    help="Specifies a different path for the zenohd process (default=/.local/bin/zenohd)",
)
parser.add_argument(
    "-c",
    "--zfctl",
    nargs="?",
    type=str,
    default="/.config/zenoh-flow/zfctl.json",
    help="Specifies a different path for the zenoctl process (default=/.config/zenoh-flow/zfctl.json)",
)
parser.add_argument(
    "-p",
    "--plugin",
    nargs="?",
    type=str,
    default="/.config/zenoh-flow/zenoh-zf-plugin-01.json",
    help="Specifies a different path for Zenoh-Flow plugin file (default=/.config/zenoh-flow/zenoh-zf-plugin-01.json)",
)
args = parser.parse_args()

home_path = os.path.expanduser("~")
zenoh_release_flag = args.build

if (
    args.zenohd == "/.local/bin/zenohd"
    or args.zfctl == "/.config/zenoh-flow/zfctl.json"
):
    print(home_path)
    zf_conf.zenohd_path = home_path + str(args.zenohd)
    zf_conf.zfctl_path = home_path + str(args.zfctl)
    zf_conf.zf_plugin_path = home_path + str(args.plugin)
else:
    zf_conf.zenohd_path = args.zenohd
    zf_conf.zfctl_path = args.zfctl
    zf_conf.zf_plugin_path = args.plugin

print("[Info] Looking for paths...")
manual_tests_path = os.getcwd()
zenoh_flow_path = os.path.abspath(os.path.join(manual_tests_path, os.pardir))

print(f"Path of zenohd: {zf_conf.zenohd_path}")
print(f"Path of zenoh-Flow path: {zenoh_flow_path}")

if os.path.exists("./../examples/flows/getting-started.yaml"):
    data_flow_path = os.path.join(
        zenoh_flow_path, "examples/flows/getting-started.yaml"
    )

print(f"Path of getting-started example: {data_flow_path}")

dir_zenoh_zf_plugin = zenoh_flow_path + "/zenoh-flow-plugin/etc/"
zenoh_zf_plugin_path = zenoh_flow_path + "/zenoh-flow-plugin/etc/zenoh-zf-plugin.json"
print(f"Path of plugin library: {zenoh_zf_plugin_path}")

lib_name = ""
if platform.system() == "Windows":
    lib_name = "libzenoh_plugin_zenoh_flow.dll"
elif platform.system() == "Linux":
    lib_name = "libzenoh_plugin_zenoh_flow.so"
elif platform.system() == "Darwin":
    lib_name = "libzenoh_plugin_zenoh_flow.dylib"

if zenoh_release_flag == "release":
    libzenoh_plugin_path = os.path.join(zenoh_flow_path, "target/release/")
elif zenoh_release_flag == "debug":
    libzenoh_plugin_path = os.path.join(zenoh_flow_path, "target/debug/")

print(f"Path of zfctl: {zf_conf.zfctl_path}")

p = "zenoh-zf-plugin-01.json".__len__()
if zf_conf.zf_plugin_path[-p:] != "zenoh-zf-plugin-01.json":
    print("[Info] Zenoh-Flow as Zenoh plugin")
    if os.path.exists(os.path.join(manual_tests_path, "zenoh-zf-plugin.json")) is False:
        print(zenoh_zf_plugin_path)
        shutil.copy(zenoh_zf_plugin_path, manual_tests_path)
    add_lib_folder = ""

    zf_conf.zf_plugin_path = os.path.join(manual_tests_path, "zenoh-zf-plugin.json")
    with open(zenoh_zf_plugin_path) as json_file:
        json_string = json_file.read()
        if json_string.find(dir_zenoh_zf_plugin) == -1:
            start_row = json_string.find('"plugins_search_dirs":[')
            index = (start_row - 1) + '"plugins_search_dirs": ['.__len__()
            add_lib_folder = (
                json_string[:index]
                + '"'
                + dir_zenoh_zf_plugin
                + '",'
                + json_string[index:]
            )

    if add_lib_folder != "":
        with open(zf_conf.zf_plugin_path, "w", encoding="utf8") as json_file:
            json_file.write(add_lib_folder)

print("[Info] Commands:")
if (
    zf_conf.zenohd_path != home_path + "/.local/bin/zenohd"
    or zf_conf.zfctl_path != home_path + "/.config/zenoh-flow/zfctl.json"
):
    zf_command["zenohd_cmd"] = zf_conf.zenohd_path + " -c " + zf_conf.zf_plugin_path
    print(f'Lauch Zenoh with Zenoh-Flow plugin: \n {zf_command["zenohd_cmd"]}')
    zf_command["runtimes_cmd"] = zf_conf.zfctl_path + " list runtimes"
    print(f'show the list of runtimes: \n {zf_command["runtimes_cmd"]}')
    zf_command["dataflow_yaml_cmd"] = zf_conf.zfctl_path + " launch " + data_flow_path
    print(f'Lauch Zenoh-Flow configuration: \n {zf_command["dataflow_yaml_cmd"]}')
    zf_command["instances_cmd"] = zf_conf.zfctl_path + " list instances"
    print(f'show list of Zenoh-Flow instances: \n {zf_command["instances_cmd"]}')
else:
    zf_command["zenohd_cmd"] = zf_conf.zenohd_path + " -c " + zf_conf.zf_plugin_path
    print(f'Lauch Zenoh with Zenoh-Flow plugin: \n {zf_command["zenohd_cmd"]}')
    zf_command["runtimes_cmd"] = (
        "ZFCTL_CFG="
        + zf_conf.zfctl_path
        + " "
        + home_path
        + "/.local/bin/zfctl"
        + " list runtimes"
    )
    print(f'show the list of runtimes: \n {zf_command["runtimes_cmd"]}')
    zf_command["dataflow_yaml_cmd"] = (
        "ZFCTL_CFG="
        + zf_conf.zfctl_path
        + " "
        + home_path
        + "/.local/bin/zfctl"
        + " launch "
        + data_flow_path
    )
    print(f'Lauch Zenoh-Flow configuration: \n {zf_command["dataflow_yaml_cmd"]}')
    zf_command["instances_cmd"] = (
        "ZFCTL_CFG="
        + zf_conf.zfctl_path
        + " "
        + home_path
        + "/.local/bin/zfctl"
        + " list instances"
    )
    print(f'show list of Zenoh-Flow instances: \n {zf_command["instances_cmd"]}')

print("[Info] Looking for Zenohd process...")
process_name = "zenohd"
pid = None
for process in psutil.process_iter():
    if process_name in process.name():
        pid = process.pid
        print(f"[Info] kill process: {pid}")
        os.kill(pid, signal.SIGKILL)


print(f'[Info] Launching Zenod process: {zf_command["zenohd_cmd"]}')
launch_zenohd = os.popen(zf_command["zenohd_cmd"])
print("Waiting for zenohd...")
time.sleep(5)
pid = ""
for process in psutil.process_iter():
    if process_name in process.name():
        pid = process.pid
        break

print(f"[Info] Test {process_name}  process pid {pid}")
zf_conf.zenohd_process = pid
if zf_conf.zenohd_process is not None:
    print(f'[Info] Launching list runtimes command: {zf_command["runtimes_cmd"]}')
    launch_runtimes = os.popen(zf_command["runtimes_cmd"])
    runtimes_string = launch_runtimes.read().split()
    for i in range(runtimes_string.__len__()):
        if runtimes_string[i].__len__() == 32 and runtimes_string[i - 2][0] == "+":
            print("[Info] test runtime: SUCCESS.")
            print(f"[Info] UUID runtime: {runtimes_string[i]}")
            zf_conf.uuid_runtimes = runtimes_string[i]
    if zf_conf.uuid_runtimes is None:
        print("[Info] test runtime: FAILED.")

    print(f'[Info] Launching Zenoh-Flow example: {zf_command["dataflow_yaml_cmd"]}')

    launch_zf_yaml = os.popen(zf_command["dataflow_yaml_cmd"])

    zf_example_string = launch_zf_yaml.read()
    print(f"[Info] zf_example_string: {zf_example_string}")
    if zf_example_string.__len__() == 0:
        print("[Error] Zenoh-Flow example: FAILED.")
    else:
        zf_conf.uuid_runtimes = zf_example_string
        print(f"[Info] Zenoh-Flow example UUID: {zf_conf.uuid_runtimes}")

    print(f'[Info] Launching list instances: {zf_command["instances_cmd"]}')
    launch_instances = os.popen(zf_command["instances_cmd"])
    count = 0
    instances_string = launch_instances.read().split()
    for i in range(instances_string.__len__()):
        if instances_string[i][0] == "+":
            count = count + 1
    if count <= 2:
        print("[Error] test data flow: FAILED.")
        exit()
    else:
        print("[Info] test data flow instance: SUCCESS.")
        instance["uuid_instances"] = instances_string[23]
        instance["flow"] = instances_string[25]
        instance["operators"] = instances_string[27]
        instance["sinks"] = instances_string[29]
        instance["sources"] = instances_string[31]
        instance["connectors"] = instances_string[33]
        instance["links"] = instances_string[35]
else:
    print("[Error] test zenohd process: FAILED.")


with open(data_flow_path, "r") as f:
    try:
        data = yaml.load(f, Loader=SafeLoader)
    except yaml.YAMLError as e:
        print(f"[Error] {e}")

    if str(tuple(data["sources"]).__len__()) == instance["sources"]:
        print(
            f'[Info] test active Source nodes: SUCCESS, the active Source nodes [{str(tuple(data["sources"]).__len__())}] are equal to declared Source nodes [{instance["sources"]}]'
        )
    if str(tuple(data["sinks"]).__len__()) == instance["sinks"]:
        print(
            f'[Info] test active Sink nodes: SUCCESS, the active Sink nodes [{str(tuple(data["sinks"]).__len__())}] are equal to those declared Sinks nodes [{instance["sinks"]}]'
        )
    if str(tuple(data["operators"]).__len__()) == instance["operators"]:
        print(
            f'[Info] test active Operator nodes: SUCCESS, the active Operator nodes [{str(tuple(data["operators"]).__len__())}] are equal declared Operator nodes [{instance["operators"]}]'
        )
    if (
        str(tuple(data["sources"]).__len__()) != instance["sources"]
        or str(tuple(data["sinks"]).__len__()) != instance["sinks"]
        or str(tuple(data["operators"]).__len__()) != instance["operators"]
    ):
        print("[Error] Test Zenoh-Flow configuration nodesSome nodes:  FAILED.")

    os.popen(
        "curl -X PUT -H 'content-type:text/plain' -d 'Test' http://localhost:8000/zf/getting-started/hello"
    )
    time.sleep(1)
    txt_res = open("/tmp/greetings.txt", "r")
    res = txt_res.read()
    if res.split()[1] == "Test!":
        print(f"\n[Info] Test Recive: SUCCESS, {res}")
    else:
        print("\n[Error] Test Recive:  FAILED.")

    process_name = "zenohd"
    pid = None
    for process in psutil.process_iter():
        if process_name in process.name():
            pid = process.pid
            print(f"[Info] kill process: {pid}")
            os.kill(pid, signal.SIGKILL)

    print("\n[Info] Commands:")
    print(zf_command["zenohd_cmd"])
    print(zf_command["runtimes_cmd"])
    print(zf_command["dataflow_yaml_cmd"])
    print(zf_command["instances_cmd"])
