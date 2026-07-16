#!/usr/bin/env bash
set -euo pipefail

sudo apt-get update
sudo apt-get install -y   build-essential   cmake   git   curl   libboost-all-dev   libzmq3-dev   libgoogle-glog-dev   libcurl4-openssl-dev   libbsd-dev   libssl-dev   libsimdjson-dev

cat <<MSG

Base dependencies installed.
You still need eventpp and mini-tsdb available to build all targets.
Set MINITSDB_ROOT before building vllm_client, for example:
  export MINITSDB_ROOT=/path/to/mini-tsdb
MSG
