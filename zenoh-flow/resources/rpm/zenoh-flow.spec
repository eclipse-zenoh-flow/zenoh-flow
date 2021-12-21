%define _rpmdir ./

Name: zenoh-flow
Version: 0.1.0
Release: 0.1.0
License: EPL 2.0 OR Apache 2.0
Summary: zenoh-based data-flow programming framework for computations that span from the cloud to the device.
Requires: zenohd
Requires: zenoh-plugin-storages
Requires: zenoh-backend-influxdb
Requires: zenoh-flow-daemon
Requires: zenoh-flow-ctl
%description -n zenoh-flow