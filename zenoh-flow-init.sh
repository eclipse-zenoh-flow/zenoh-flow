#!/usr/bin/env bash

set -e # exit on error
set -u # exit when a variable does not exist

# Name of the script.
THIS=$0

# COLORS ----------------------------------------------------------------------
NC='\033[0m'
BLACK='\033[0;30m'
DARKGRAY='\033[1;30m'
LIGHTGRAY='\033[0;37m'
RED='\033[0;31m'
LIGHTRED='\033[1;31m'
GREEN='\033[0;32m'
LIGHTGREEN='\033[1;32m'
BROWNORANGE='\033[0;33m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
LIGHTBLUE='\033[1;34m'
PURPLE='\033[0;35m'
LIGHTPURPLE='\033[1;35m'
CYAN='\033[0;36m'
LIGHTCYAN='\033[1;36m'
WHITE='\033[1;37m'

# DEFAULT VALUES --------------------------------------------------------------
BASE_INSTALL_PATH=$HOME/.local
BASE_CONFIG_PATH=$HOME/.config/zenoh-flow
ZENOH_FLOW_EXTENSIONS=$BASE_CONFIG_PATH/extensions.d
ECLIPSE_ZENOH_URL="https://github.com/eclipse-zenoh"
BUILD="release"
CARGO_CMD="cargo build --release"
ZENOH_VERSION="0.7.2-rc"
RUSTC_VERSION="1.70.0"
LOG_FILE="$(mktemp)"

err_and_exit() {
    echo -e "$THIS [${RED}err${NC}]: $1" >&2
    display_usage
    exit 1
}

info() {
    echo -e "$THIS [${GREEN}info${NC}]: $1"
}

code() {
    printf "${BROWNORANGE}%s${NC}" "$1"
}

heading() {
    printf "${LIGHTBLUE}%s${NC}" "$1"
}

display_usage() {
    cat <<EOF

A local installation script for Zenoh-Flow and, optionally, Zenoh.

USAGE:
    $THIS [OPTIONS]

OPTIONS:
    -h, --help
            Print help information.

    -c, --commands
            Print basic commands to interact with Zenoh-Flow and quit the script.

    -i, --install
            Install Zenoh and, optionally, Zenoh-Flow.

            Without any additional arguments this is equivalent to calling:
            $THIS -i --build release --install-path $BASE_INSTALL_PATH

    -b, --build
        [default: release] [possible values: debug, release]
            Change the build profile.

            'release' will take longer to compile but will yield far better performance
            and smaller binaries.

            'debug' will be faster to compile but the performance will be negatively
            impacted and the resulting binaries will be noticably bigger.

        --without-zenoh
            Do not install Zenoh in addition to Zenoh-Flow.

            /!\ Note that for Zenoh-Flow to be loaded as a Zenoh plugin, the version of
            Zenoh and of rustc must be the same as the one used internally by
            Zenoh-Flow.

            These versions are:    (zenoh)       $ZENOH_VERSION
                                   (rustc)       $RUSTC_VERSION

        --clean
            Clean a previous installation.

            It is possible to change the path where the script will look through the
            '--path' option.

        --install-path <path>
            [default: $BASE_INSTALL_PATH]
            Change the path where this script will operate.

            Note that following subdirectories will be created when installing:
            + <path>/bin
            + <path>/lib
EOF
}

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        err "command '$1' not found"
    fi
}

display_commands_and_exit() {
    cat <<EOF

$(heading "ZENOH-FLOW QUICK REFERENCE GUIDE")

$(heading "+ Starting Zenoh-Flow"):

    - As a Zenoh plugin:

        $(code "$ZENOHD -c $ZENOHD_CONFIG_ZF_PLUGIN")

    - As a Daemon:

        (Optional: if you already have Zenoh-Flow running as a Zenoh plugin,
                   you can skip this step.)
        First, start a Zenoh router with the required storages configured:

        $(code "$ZENOHD -c $ZENOHD_CONFIG_STORAGE_ZF")

        Then start the daemon:

        $(code "$ZF_DAEMON -c $ZF_DAEMON_CONFIG")

    NOTES:
    - You can have a Zenoh-Flow plugin AND a Zenoh-Flow daemon running at the
      same time on your machine.

    - You can start multiple daemons and plugins, to do so you need to
      duplicate their respective configuration files and change the "name"
      key such that each daemon/plugin has a unique name.

    - It is enough to have a single Zenoh router with the required storages.


$(heading "+ Listing the runtimes"):

        $(code "$ZFCTL_ENV $ZFCTL list runtimes")


$(heading "+ Starting a flow"):

        $(code "$ZFCTL_ENV $ZFCTL launch < path/to/your/flow.yaml >")

    NOTES:
    - This command will echo the UUID of the instance of the flow that was just
      created.

    - It is possible to launch multiple instances of the same flow.


$(heading "+ Stopping a flow instance"):

        $(code "$ZFCTL_ENV $ZFCTL destroy < UUID-of-the-flow-instance >")

EOF
    exit 0
}

update_paths() {
    BIN_PATH=$BASE_INSTALL_PATH/bin
    LIB_PATH=$BASE_INSTALL_PATH/lib

    # Binaries:
    # - zenohd
    # - zenoh-flow-daemon
    # - zfctl
    ZFCTL=$BIN_PATH/zfctl
    ZENOHD=$BIN_PATH/zenohd
    ZF_DAEMON=$BIN_PATH/zenoh-flow-daemon

    # Zenoh plugins:
    # - storage
    # - zenoh-flow
    # - rest
    ZENOH_PLUGIN_STORAGE_LIB=libzenoh_plugin_storage_manager.${EXT}
    ZENOH_PLUGIN_STORAGE=$LIB_PATH/$ZENOH_PLUGIN_STORAGE_LIB

    ZENOH_PLUGIN_REST_LIB=libzenoh_plugin_rest.${EXT}
    ZENOH_PLUGIN_REST=$LIB_PATH/$ZENOH_PLUGIN_REST_LIB

    ZENOH_FLOW_PLUGIN_LIB=libzenoh_plugin_zenoh_flow.${EXT}
    ZENOH_FLOW_PLUGIN=$LIB_PATH/$ZENOH_FLOW_PLUGIN_LIB

    # Configuration files:
    # - zenoh: storage (for ZF) + rest + zenoh-flow plugins
    # - zenoh: storage (for ZF) + rest
    # - zenoh-flow-daemon:
    #     - runtime
    #     - zenoh
    # - zfctl
    ZENOHD_CONFIG_ZF_PLUGIN=$BASE_CONFIG_PATH/zenoh-zf-plugin-01.json
    ZENOHD_CONFIG_STORAGE_ZF=$BASE_CONFIG_PATH/zenoh-storage-zf.json

    ZF_DAEMON_CONFIG=$BASE_CONFIG_PATH/zf-daemon.yaml
    ZF_DAEMON_ZENOH_CONFIG=$BASE_CONFIG_PATH/zf-daemon-zenoh.json

    ZFCTL_CONFIG=$BASE_CONFIG_PATH/zfctl.json

    # Environment variable to control zfctl:
    ZFCTL_ENV="ZFCTL_CFG=$ZFCTL_CONFIG"

    TARGETS=(
        "$ZFCTL"
        "$ZENOHD"
        "$ZF_DAEMON"
        "$ZENOH_PLUGIN_STORAGE"
        "$ZENOH_PLUGIN_REST"
        "$ZENOH_FLOW_PLUGIN"
        "$ZENOHD_CONFIG_ZF_PLUGIN"
        "$ZENOHD_CONFIG_STORAGE_ZF"
        "$ZF_DAEMON_CONFIG"
        "$ZF_DAEMON_ZENOH_CONFIG"
        "$ZFCTL_CONFIG"
    )
}

clean_install() {
    for target in "${TARGETS[@]}"; do
        if [[ -f $target ]]; then
            info "removed $(rm -v "$target")"
        fi
    done
}

install_zenoh() {
    ZENOH_TMP_DIR=$(mktemp -d)/zenoh-"$ZENOH_VERSION"
    # FIXME @J-Loudet Hack: for now the transport-quic feature generates errors.
    #
    # We thus have to disable the default-features and manually enable all of
    # them except for `transport-quic`.
    ZENOH_FEATURES="shared-memory"

    # (try to) Detect if zenohd was already installed before.
    if [[ -f $ZENOHD &&
        -f $ZENOH_PLUGIN_STORAGE &&
        -f $ZENOH_PLUGIN_REST ]]; then

        Z_VERSION=$("${ZENOHD}" --version 2>&1 | head -n 1)

        if [[ $Z_VERSION == *"b9103c3"* &&
            $Z_VERSION == *"rustc $RUSTC_VERSION"* ]]; then
            info "Zenoh $ZENOH_VERSION already installed, skipping"

            cleanup_tmp "$ZENOH_TMP_DIR"
            return 0
        fi
    fi

    info "cloning Zenoh $ZENOH_VERSION"

    git clone \
        --branch "$ZENOH_VERSION" \
        --depth 1 \
        "$ECLIPSE_ZENOH_URL/zenoh.git" "$ZENOH_TMP_DIR" >"$LOG_FILE" 2>&1

    cd "$ZENOH_TMP_DIR"

    # FIXME @J-Loudet: hack, waiting for Zenoh to bump their MSRV
    info "forcing rustc $RUSTC_VERSION"
    echo "1.70.0" >rust-toolchain
    info "building Zenoh $ZENOH_VERSION (this may take a while…)"
    $CARGO_CMD \
        --features "$ZENOH_FEATURES" \
        -p zenohd \
        -p zenoh-plugin-rest \
        -p zenoh-plugin-storage-manager >"$LOG_FILE" 2>&1

    install "$ZENOH_TMP_DIR/target/$BUILD/zenohd" "$ZENOHD"
    info "installed $ZENOHD"
    install "$ZENOH_TMP_DIR/target/$BUILD/$ZENOH_PLUGIN_REST_LIB" "$ZENOH_PLUGIN_REST"
    info "installed $ZENOH_PLUGIN_REST"
    install "$ZENOH_TMP_DIR/target/$BUILD/$ZENOH_PLUGIN_STORAGE_LIB" "$ZENOH_PLUGIN_STORAGE"
    info "installed $ZENOH_PLUGIN_STORAGE"

    cleanup_tmp "$ZENOH_TMP_DIR"
    return 0
}

cleanup_tmp() {
    # To avoid removing the floor from under our feet, let us move to some place
    # safe first.
    cd "$HOME"
    rm -rf "$1"
}

install_zenoh_flow() {
    # ZF_TMP_DIR=$(mktemp -d)/zenoh-flow-"$ZF_VERSION"

    # if [[ -f $ZF_DAEMON &&
    #     $(${ZF_DAEMON} --version 2>&1 | head -n 1) == *"$ZF_VERSION"* &&
    #     -f "$ZENOH_FLOW_PLUGIN" &&
    #     -f "$ZFCTL" ]]; then
    #     info "Zenoh-Flow $ZF_VERSION already installed, skipping"

    #     # cleanup_tmp "$ZF_TMP_DIR"
    #     return 0
    # fi

    # info "cloning Zenoh-Flow $ZF_VERSION"

    # git clone \
    #     --branch "$ZF_VERSION" \
    #     --depth 1 \
    #     "$ECLIPSE_ZENOH_URL/zenoh-flow.git" "$ZF_TMP_DIR" >"$LOG_FILE" 2>&1

    # cd "$ZF_TMP_DIR"

    info "building Zenoh-Flow in $BUILD (this may take a while…)"
    $CARGO_CMD \
        -p zenoh-flow-daemon \
        -p zenoh-flow-plugin \
        -p zfctl >"$LOG_FILE" 2>&1

    install "./target/$BUILD/zenoh-flow-daemon" "$ZF_DAEMON"
    info "installed $ZF_DAEMON"
    install "./target/$BUILD/zfctl" "$ZFCTL"
    info "installed $ZFCTL"
    install "./target/$BUILD/$ZENOH_FLOW_PLUGIN_LIB" "$ZENOH_FLOW_PLUGIN"
    info "installed $ZENOH_FLOW_PLUGIN"

    # cleanup_tmp "$ZF_TMP_DIR"
    return 0
}

generate_configuration_files() {
    if [[ ! -f $ZFCTL_CONFIG ]]; then
        echo "\
{
    \"connect\": {
        \"endpoints\": [\"tcp/0.0.0.0:7447\"]
    },
    \"mode\": \"client\"
}" >"$ZFCTL_CONFIG"

        info "generated $ZFCTL_CONFIG"
    fi

    if [[ ! -f $ZENOHD_CONFIG_ZF_PLUGIN ]]; then
        echo "\
{
    \"listen\": {
        \"endpoints\": [\"tcp/0.0.0.0:7447\"]
    },
    \"plugins_search_dirs\": [
        \"$LIB_PATH\"
    ],
    \"plugins\":{
        \"storage_manager\":{
        \"required\":true,
        \"storages\":{
            \"zfrpc\":{
                \"key_expr\":\"zf/runtime/**\",
                \"volume\": \"memory\"
                },
            \"zf\":{
                \"key_expr\":\"zenoh-flow/**\",
                \"volume\": \"memory\"
                }
            }
        },
        \"rest\":{
            \"http_port\": 8000,
        },
        \"zenoh_flow\":{
            \"required\":true,
            \"name\": \"zenoh-flow-plugin-01\",
            \"path\":\"$BIN_PATH\",
            \"pid_file\": \"/var/zenoh-flow/zenoh-flow-plugin-01.pid\",
            \"extensions\": \"$ZENOH_FLOW_EXTENSIONS\",
            \"worker_pool_size\":4,
            \"use_shm\": false
        }
    }
}" >"$ZENOHD_CONFIG_ZF_PLUGIN"
        info "generated $ZENOHD_CONFIG_ZF_PLUGIN"
    fi

    if [[ ! -f $ZENOHD_CONFIG_STORAGE_ZF ]]; then
        echo "\
{
    \"listen\": {
        \"endpoints\": [\"tcp/0.0.0.0:7447\"]
    },
    \"plugins_search_dirs\": [
        \"$LIB_PATH\"
    ],
    \"plugins\":{
        \"storage_manager\":{
        \"required\":true,
        \"storages\":{
            \"zfrpc\":{
                \"key_expr\":\"zf/runtime/**\",
                \"volume\": \"memory\"
                },
            \"zf\":{
                \"key_expr\":\"zenoh-flow/**\",
                \"volume\": \"memory\"
                }
            }
        },
        \"rest\":{
            \"http_port\": 8000,
        }
    }
}" >"$ZENOHD_CONFIG_STORAGE_ZF"
        info "generated $ZENOHD_CONFIG_STORAGE_ZF"
    fi

    if [[ ! -f $ZF_DAEMON_CONFIG ]]; then
        if command -v uuidgen >/dev/null &&
            command -v sed >/dev/null &&
            command -v tr >/dev/null; then
            UUID=$(uuidgen | sed 's/^0/a/' | sed 's/-//g' | tr '[:upper:]' '[:lower:]')
        else
            UUID=deadbeef01
        fi

        echo "\
name: zenoh-flow-daemon-01
uuid: $UUID
pid_file: /var/zenoh-flow/zenoh-flow-daemon-01.pid
path: $BIN_PATH
extensions: $ZENOH_FLOW_EXTENSIONS
zenoh_config: $ZF_DAEMON_ZENOH_CONFIG
worker_pool_size: 4
" >"$ZF_DAEMON_CONFIG"

        info "generated $ZF_DAEMON_CONFIG"
    fi

    if [[ ! -f $ZF_DAEMON_ZENOH_CONFIG ]]; then
        echo "\
{
    \"connect\": {
        \"endpoints\": [\"tcp/0.0.0.0:7447\"]
    },
    \"listen\": {
        \"endpoints\": [\"tcp/0.0.0.0:7997\"]
    },
    \"mode\": \"peer\",
}
" >"$ZF_DAEMON_ZENOH_CONFIG"
        info "generated $ZF_DAEMON_ZENOH_CONFIG"
    fi

}

main() {
    need_cmd cargo
    need_cmd git
    need_cmd mkdir
    need_cmd rm
    need_cmd mktemp

    case $(uname) in
    'Darwin')
        EXT="dylib"
        ;;
    'Linux')
        EXT="so"
        ;;
    *)
        err_and_exit 'only Linux and macOS are, for now, supported by this script'
        ;;
    esac

    if [[ $# == 0 ]]; then
        display_usage
        exit 1
    fi

    local install=0
    local install_zenoh=1
    local display_commands=0
    local clean=0

    while [[ $# -gt 0 ]]; do
        case $1 in
        -h | --help)
            display_usage
            exit 0
            ;;
        -i | --install)
            install=1
            display_commands=1
            shift 1
            ;;
        -c | --commands)
            display_commands=1
            shift 1
            ;;
        --install-path)
            BASE_INSTALL_PATH=$2
            shift 2
            ;;
        --clean)
            clean=1
            shift 1
            ;;
        -b | --build)
            case $2 in
            release) ;;
            debug)
                BUILD="debug"
                CARGO_CMD="cargo build"
                ;;
            *)
                err_and_exit "unrecognized build '$2'"
                ;;
            esac
            shift 2
            ;;
        --without-zenoh)
            install_zenoh=0
            shift 1
            ;;
        *)
            err_and_exit "unrecognized option '$1'"
            ;;
        esac
    done

    update_paths

    if [[ $clean == 1 ]]; then
        clean_install
    fi

    if [[ $install == 1 ]]; then
        mkdir -p "$BASE_CONFIG_PATH"
        mkdir -p "$BIN_PATH"
        mkdir -p "$LIB_PATH"

        info "logs will be written to $LOG_FILE"
        info "use the following command, in another terminal, to visualize them:"
        info ""
        info "$(code "tail -f $LOG_FILE")"
        echo -e "\n\n"

        install_zenoh_flow

        if [[ $install_zenoh == 1 ]]; then
            install_zenoh
        fi

        generate_configuration_files
    fi

    if [[ $display_commands == 1 ]]; then
        display_commands_and_exit
    fi
}

main "$@"
