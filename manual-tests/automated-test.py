import argparse
import fnmatch
import os
import platform
import shutil
import sys
import time

import psutil


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
    default="debug",
    help="Specifies a different directory for Zenoh-Flow artifacts (default=debug)",
)
parser.add_argument(
    "-z",
    "--zenohd",
    nargs="?",
    type=str,
    default=os.path.expanduser("~") + "/.local/bin/zenohd",
    help=f"Specifies a different path for the zenohd process (default={os.path.expanduser('~')}/.local/bin/zenohd)",
)
parser.add_argument(
    "-c",
    "--zfctl",
    nargs="?",
    type=str,
    default=os.path.expanduser("~") + "/.config/zenoh-flow/zfctl.json",
    help=f"Specifies a different path for the zenoctl process (default={os.path.expanduser('~')}/.config/zenoh-flow/zfctl.json)",
)
parser.add_argument(
    "-p",
    "--plugin",
    nargs="?",
    type=str,
    default=os.path.expanduser("~") + "/.config/zenoh-flow/zenoh-zf-plugin-01.json",
    help=f"Specifies a different path for Zenoh-Flow plugin file (default={os.path.expanduser('~')}/.config/zenoh-flow/zenoh-zf-plugin-01.json)",
)
args = parser.parse_args()
home_path = os.path.expanduser("~")
zenoh_release_flag = args.build

zf_conf.zenohd_path = args.zenohd
zf_conf.zfctl_path = args.zfctl
zf_conf.zf_plugin_path = args.plugin

print("[Info] Looking for paths...")
manual_tests_path = os.getcwd()
zenoh_flow_path = os.path.abspath(os.path.join(manual_tests_path, os.pardir))

print(f"Path of zenohd: {zf_conf.zenohd_path}")
print(f"Path of zenoh-Flow path: {zenoh_flow_path}")

data_flow_path = os.path.join(zenoh_flow_path, "examples/flows/getting-started.yaml")

print(f"Path of getting-started example: {data_flow_path}")

dir_zenoh_zf_plugin = zenoh_flow_path + "/zenoh-flow-plugin/etc/"
zenoh_zf_plugin_path = zenoh_flow_path + "/zenoh-flow-plugin/etc/zenoh-zf-plugin.json"
print(f"Path of plugin library: {zenoh_zf_plugin_path}")
print(f"Path of zfctl: {zf_conf.zfctl_path}")

p = len("zenoh-zf-plugin-01.json")
if zf_conf.zf_plugin_path[-p:] != "zenoh-zf-plugin-01.json":
    lib_name = ""
    if platform.system() == "Windows":
        lib_name = "libzenoh_plugin_zenoh_flow.dll"
    elif platform.system() == "Linux":
        lib_name = "libzenoh_plugin_zenoh_flow.so"
    elif platform.system() == "Darwin":
        lib_name = "libzenoh_plugin_zenoh_flow.dylib"

    libzenoh_plugin_path = ""
    if zenoh_release_flag == "release":
        zf_conf.libzenoh_plugin = os.path.join(zenoh_flow_path, "target/release/")
    elif zenoh_release_flag == "debug":
        zf_conf.libzenoh_plugin = os.path.join(zenoh_flow_path, "target/debug/")

    print(f"[Info] {lib_name} path: ", zf_conf.libzenoh_plugin)

    print("[Info] Zenoh-Flow as Zenoh plugin")
    if os.path.exists(os.path.join(manual_tests_path, "zenoh-zf-plugin.json")) is False:
        print(zenoh_zf_plugin_path)
        print(manual_tests_path)
        shutil.copy(zf_conf.zf_plugin_path, manual_tests_path)
    add_lib_folder = ""

    # reassign the value of the "zf_conf.zf_plugin_path" variable with
    # the path of the new file "zenoh-zf-plugin.json" created into manual-test folder
    zf_conf.zf_plugin_path = os.path.join(manual_tests_path, "zenoh-zf-plugin.json")
    print(
        f"[Info] Create new zenoh-zf-plugin.json and update the path\n[Info] New path: {zf_conf.zf_plugin_path}"
    )
    with open(zf_conf.zf_plugin_path) as json_file:
        json_string = json_file.read()
        if json_string.find(zf_conf.libzenoh_plugin) == -1:
            start_row = json_string.find('"plugins_search_dirs":[')
            index = (start_row - 1) + len('"plugins_search_dirs": [')
            add_lib_folder = (
                json_string[:index]
                + '"'
                + zf_conf.libzenoh_plugin
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
    print(f'Show runtimes list: \n {zf_command["runtimes_cmd"]}')
    zf_command["dataflow_yaml_cmd"] = zf_conf.zfctl_path + " launch " + data_flow_path
    print(f'Lauch Zenoh-Flow configuration: \n {zf_command["dataflow_yaml_cmd"]}')
    zf_command["instances_cmd"] = zf_conf.zfctl_path + " list instances"
    print(f'Show Zenoh-Flow list instances: \n {zf_command["instances_cmd"]}')
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

print("[Info] Killing extra `zenohd` processes...")
process_name = "zenohd"
pid = None
for process in psutil.process_iter():
    if process_name in process.name():
        print(f"[Info] kill process: {process.pid}")
        process.kill()

print(f'[Info] Launching Zenod process: {zf_command["zenohd_cmd"]}')
launch_zenohd = os.popen(zf_command["zenohd_cmd"])
print("Waiting for zenohd...")
time.sleep(5)
pid = ""
for process in psutil.process_iter():
    if process_name in process.name():
        pid = process.pid
        break

if zf_conf.zenohd_process != "none":
    print("[info] Launching list runtimes command:  ", zf_command["runtimes_cmd"])
    launch_runtimes = os.popen(zf_command["runtimes_cmd"])
    runtimes_string = launch_runtimes.read()


print(f"[Info] Test {process_name}  process pid {pid}")
zf_conf.zenohd_process = pid
if zf_conf.zenohd_process is not None:
    if os.path.exists("/tmp/greetings.txt"):
        os.remove("/tmp/greetings.txt")
    print(f'[Info] Launching Zenoh-Flow example: {zf_command["dataflow_yaml_cmd"]}')
    launch_zf_yaml = os.popen(zf_command["dataflow_yaml_cmd"])
    zf_example_string = launch_zf_yaml.read()
    print(f"[Info] zf_example_string: {zf_example_string}")
    if len(zf_example_string) == 0:
        print("[Error] Zenoh-Flow example: FAILED.")
        sys.exit(-1)
    else:
        zf_conf.uuid_runtimes = zf_example_string
        print(f"[Info] Zenoh-Flow example UUID: {zf_conf.uuid_runtimes}")
else:
    print("[Error] test zenohd process: FAILED.")
    sys.exit(-1)

curl_getting_started = "curl -X PUT -H 'content-type:text/plain' -d 'Test' http://localhost:8000/zf/getting-started/hello"

time.sleep(1)

if os.path.exists("/tmp/greetings.txt"):
    txt_res = open("/tmp/greetings.txt", "r")
    previous_res = txt_res.read().splitlines()
    previus_index = len(previous_res)
    previus_hello = 0
    current_hello = 0

    for i in range(len(previous_res)):
        if previous_res[i] == "Hello, Test!":
            previus_hello += 1

    curl_getting_started = "curl -X PUT -H 'content-type:text/plain' -d 'Test' http://localhost:8000/zf/getting-started/hello"
    print(
        f"[Info] Launching CURL request to `zf/getting-started/hello`: \n {curl_getting_started}\n"
    )
    os.popen(curl_getting_started)
    time.sleep(1)
    txt_res = open("/tmp/greetings.txt", "r")
    res = txt_res.read().splitlines()
    if previus_index == 1 and res[0] == "":
        result = res
    else:
        result = res[previus_index:]

    if (len(result)) != 0:
        for i in range(len(result)):
            if result[i] == "Hello, Test!":
                current_hello += 1
                print(f"\n[Info] Test Recive: SUCCESS, {result[i]}")
            else:
                print("\n[Error] Test Recive:  FAILED.")
                sys.exit(-1)
    else:
        print("\n[Error] Test Recive:  FAILED.")
        sys.exit(-1)
    os.remove("/tmp/greetings.txt")
    if current_hello <= previus_index or current_hello == 0:
        sys.exit(-1)


print("\n[Info] Killing `zenohd` processes...")
process_name = "zenohd"
pid = None
for process in psutil.process_iter():
    if process_name in process.name():
        print(f"[Info] kill process: {process.pid}")
        process.kill()

print("\n[Info] Commands:")
print(zf_command["zenohd_cmd"])
print(zf_command["runtimes_cmd"])
print(zf_command["dataflow_yaml_cmd"])
print(zf_command["instances_cmd"])
