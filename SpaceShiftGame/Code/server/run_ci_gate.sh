#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
cd "${REPO_ROOT}"

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "[FAIL] Python interpreter not found: ${PYTHON_BIN}" >&2
  exit 1
fi

export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export TZ="${TZ:-UTC}"
export SPACESHIFT_DETERMINISTIC="${SPACESHIFT_DETERMINISTIC:-1}"

SIM_PROFILE="${SIM_PROFILE:-standard}"
SIM_SEED="${SIM_SEED:-20260304}"
SIM_TAG="${SIM_TAG:-ci_standard}"
SIM_STARTUP_TIMEOUT="${SIM_STARTUP_TIMEOUT:-20}"
SIM_REQUEST_TIMEOUT="${SIM_REQUEST_TIMEOUT:-8}"
SMOKE_STARTUP_TIMEOUT="${SMOKE_STARTUP_TIMEOUT:-15}"
SMOKE_REQUEST_TIMEOUT="${SMOKE_REQUEST_TIMEOUT:-5}"

if [[ "${SIM_PROFILE}" != "standard" ]]; then
  echo "[FAIL] SIM_PROFILE must be 'standard' for the CI gate (got '${SIM_PROFILE}')." >&2
  exit 1
fi

CORE_SCRIPTS=(
  "SpaceShiftGame/Code/server/mock_server.py"
  "SpaceShiftGame/Code/server/smoke_test.py"
  "SpaceShiftGame/Code/server/run_simulation_suite.py"
  "SpaceShiftGame/Code/server/check_simulation_regression.py"
  "SpaceShiftGame/Code/server/check_endurance_regression.py"
  "SpaceShiftGame/Code/server/simpy_timeflow.py"
)

echo "[INFO] Backend CI gate started"
echo "[INFO] Python: $(${PYTHON_BIN} --version 2>&1)"
echo "[INFO] Seed=${SIM_SEED} Tag=${SIM_TAG} Profile=${SIM_PROFILE}"

echo "[1/4] Compiling core server scripts"
"${PYTHON_BIN}" -m py_compile "${CORE_SCRIPTS[@]}"

echo "[2/4] Running smoke tests"
"${PYTHON_BIN}" "SpaceShiftGame/Code/server/smoke_test.py" \
  --startup-timeout "${SMOKE_STARTUP_TIMEOUT}" \
  --request-timeout "${SMOKE_REQUEST_TIMEOUT}"

echo "[3/4] Running simulation suite (${SIM_PROFILE})"
"${PYTHON_BIN}" "SpaceShiftGame/Code/server/run_simulation_suite.py" \
  --profile "${SIM_PROFILE}" \
  --seed "${SIM_SEED}" \
  --tag "${SIM_TAG}" \
  --startup-timeout "${SIM_STARTUP_TIMEOUT}" \
  --request-timeout "${SIM_REQUEST_TIMEOUT}"

echo "[4/4] Running simulation regression checks"
"${PYTHON_BIN}" "SpaceShiftGame/Code/server/check_simulation_regression.py"

echo "[PASS] Backend CI gate completed"
