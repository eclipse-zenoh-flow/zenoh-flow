##
## Copyright (c) 2022 ZettaScale Technology
##
## This program and the accompanying materials are made available under the
## terms of the Eclipse Public License 2.0 which is available at
## http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
## which is available at https://www.apache.org/licenses/LICENSE-2.0.
##
## SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
##
## Contributors:
##   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
##

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
