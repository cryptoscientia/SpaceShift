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

SIM_PROFILE="${SIM_PROFILE:-long}"
SIM_SEEDS="${SIM_SEEDS:-20260304,20260305}"
SIM_TAG_PREFIX="${SIM_TAG_PREFIX:-ci_endurance}"
SIM_STARTUP_TIMEOUT="${SIM_STARTUP_TIMEOUT:-35}"
SIM_REQUEST_TIMEOUT="${SIM_REQUEST_TIMEOUT:-10}"
ENDURANCE_THRESHOLDS="${ENDURANCE_THRESHOLDS:-SpaceShiftGame/Reports/endurance_thresholds_v1.json}"

if [[ "${SIM_PROFILE}" != "standard" && "${SIM_PROFILE}" != "long" ]]; then
  echo "[FAIL] SIM_PROFILE must be 'standard' or 'long' (got '${SIM_PROFILE}')." >&2
  exit 1
fi

IFS=',' read -r -a RAW_SEEDS <<< "${SIM_SEEDS}"
SEEDS=()
for raw_seed in "${RAW_SEEDS[@]}"; do
  seed="$(echo "${raw_seed}" | tr -d '[:space:]')"
  if [[ -z "${seed}" ]]; then
    continue
  fi
  if ! [[ "${seed}" =~ ^[0-9]+$ ]]; then
    echo "[FAIL] SIM_SEEDS contains non-numeric seed '${seed}'." >&2
    exit 1
  fi
  SEEDS+=("${seed}")
done

if (( ${#SEEDS[@]} < 2 )); then
  echo "[FAIL] SIM_SEEDS must provide at least two seeds for endurance checks." >&2
  exit 1
fi

echo "[INFO] Backend endurance CI gate started"
echo "[INFO] Python: $(${PYTHON_BIN} --version 2>&1)"
echo "[INFO] Profile=${SIM_PROFILE} Seeds=${SEEDS[*]} TagPrefix=${SIM_TAG_PREFIX}"

REPORT_PATHS=()
for index in "${!SEEDS[@]}"; do
  run_no="$((index + 1))"
  seed="${SEEDS[${index}]}"
  tag="${SIM_TAG_PREFIX}_${SIM_PROFILE}_s${run_no}_seed${seed}"
  report_json="SpaceShiftGame/Reports/simulation_suite_${tag}.json"
  regression_json="SpaceShiftGame/Reports/simulation_regression_${tag}.json"

  echo "[INFO] [${run_no}/${#SEEDS[@]}] Running simulation suite for seed ${seed} (tag=${tag})"
  "${PYTHON_BIN}" "SpaceShiftGame/Code/server/run_simulation_suite.py" \
    --profile "${SIM_PROFILE}" \
    --seed "${seed}" \
    --tag "${tag}" \
    --startup-timeout "${SIM_STARTUP_TIMEOUT}" \
    --request-timeout "${SIM_REQUEST_TIMEOUT}"

  echo "[INFO] [${run_no}/${#SEEDS[@]}] Running regression checks for ${report_json}"
  "${PYTHON_BIN}" "SpaceShiftGame/Code/server/check_simulation_regression.py" \
    --report "${report_json}" \
    --output "${regression_json}"

  REPORT_PATHS+=("${report_json}")
done

aggregate_json="SpaceShiftGame/Reports/simulation_endurance_summary_${SIM_TAG_PREFIX}_${SIM_PROFILE}.json"
aggregate_md="SpaceShiftGame/Reports/simulation_endurance_summary_${SIM_TAG_PREFIX}_${SIM_PROFILE}.md"
aggregate_label="${SIM_TAG_PREFIX}_${SIM_PROFILE}"
endurance_regression_json="SpaceShiftGame/Reports/endurance_regression_${SIM_TAG_PREFIX}_${SIM_PROFILE}.json"
latest_aggregate_json="SpaceShiftGame/Reports/latest_endurance_${SIM_PROFILE}.json"
latest_aggregate_md="SpaceShiftGame/Reports/latest_endurance_${SIM_PROFILE}.md"
latest_regression_json="SpaceShiftGame/Reports/endurance_regression_latest_${SIM_PROFILE}.json"

echo "[INFO] Aggregating endurance reports -> ${aggregate_json}"
"${PYTHON_BIN}" "SpaceShiftGame/Code/server/aggregate_simulation_endurance.py" \
  --label "${aggregate_label}" \
  --inputs "${REPORT_PATHS[@]}" \
  --output-json "${aggregate_json}" \
  --output-md "${aggregate_md}"

echo "[INFO] Running endurance regression checks -> ${endurance_regression_json}"
"${PYTHON_BIN}" "SpaceShiftGame/Code/server/check_endurance_regression.py" \
  --report "${aggregate_json}" \
  --thresholds "${ENDURANCE_THRESHOLDS}" \
  --output "${endurance_regression_json}"

cp "${aggregate_json}" "${latest_aggregate_json}"
cp "${aggregate_md}" "${latest_aggregate_md}"
cp "${endurance_regression_json}" "${latest_regression_json}"

echo "[PASS] Backend endurance CI gate completed"
echo "[INFO] Aggregate JSON: ${aggregate_json}"
echo "[INFO] Aggregate Markdown: ${aggregate_md}"
echo "[INFO] Endurance regression JSON: ${endurance_regression_json}"
