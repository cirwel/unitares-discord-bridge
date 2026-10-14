#!/usr/bin/env bash
# Compatibility entry point; these isolated tests are also collected by CI.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "${PYTHON:-python3}" -m pytest "$SCRIPT_DIR/test_ship.py" -q
