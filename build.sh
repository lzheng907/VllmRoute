#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"$ROOT/scripts/install_deps_ubuntu.sh"
"$ROOT/scripts/build_all.sh" "${1:-}"
