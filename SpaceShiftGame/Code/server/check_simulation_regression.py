#!/usr/bin/env python3
"""Evaluate simulation report outputs against baseline thresholds."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class RegressionError(RuntimeError):
    """Raised when regression checks fail or inputs are invalid."""


SCRIPT_PATH = Path(__file__).resolve()
SERVER_DIR = SCRIPT_PATH.parent
SPACESHIFT_ROOT = SERVER_DIR.parent.parent
PROJECT_ROOT = SPACESHIFT_ROOT.parent
REPORTS_DIR = SPACESHIFT_ROOT / "Reports"

DEFAULT_REPORT = REPORTS_DIR / "latest_simulation_report.json"
DEFAULT_THRESHOLDS = REPORTS_DIR / "simulation_thresholds_v1.json"
DEFAULT_OUTPUT = REPORTS_DIR / "simulation_regression_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check SpaceShift simulation report against thresholds.")
    parser.add_argument(
        "--report",
        default=str(DEFAULT_REPORT),
        help="Path to simulation report JSON.",
    )
    parser.add_argument(
        "--thresholds",
        default=str(DEFAULT_THRESHOLDS),
        help="Path to thresholds JSON.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Path to write regression check JSON summary.",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise RegressionError(f"Missing required file: {path}")
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RegressionError(f"Invalid JSON at {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RegressionError(f"Expected top-level JSON object at {path}")
    return parsed


def get_nested(payload: dict[str, Any], path: list[str], default: Any = None) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return default


def as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    return default


def run_checks(report: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    req = thresholds.get("required", {})
    if not isinstance(req, dict):
        raise RegressionError("thresholds.required must be an object")

    checks: list[dict[str, Any]] = []

    def add_check(name: str, passed: bool, actual: Any, expected: Any) -> None:
        checks.append(
            {
                "name": name,
                "passed": bool(passed),
                "actual": actual,
                "expected": expected,
            }
        )

    smoke_pass_count = as_float(get_nested(report, ["smoke", "pass_count"]), 0.0)
    smoke_min = as_float(req.get("smoke_pass_count_min"), 0.0)
    add_check(
        "smoke_pass_count_min",
        smoke_pass_count >= smoke_min,
        smoke_pass_count,
        f">= {smoke_min}",
    )

    smoke_ok = as_bool(get_nested(report, ["smoke", "ok"]), False)
    add_check("smoke_ok", smoke_ok, smoke_ok, True)

    worlds_scanned = as_float(
        get_nested(report, ["discovery_world_ops", "world_totals", "total_worlds_scanned"]),
        0.0,
    )
    worlds_min = as_float(req.get("worlds_scanned_min"), 0.0)
    add_check("worlds_scanned_min", worlds_scanned >= worlds_min, worlds_scanned, f">= {worlds_min}")

    structures_built = as_float(get_nested(report, ["discovery_world_ops", "structures", "built_count"]), 0.0)
    structures_min = as_float(req.get("structures_built_min"), 0.0)
    add_check("structures_built_min", structures_built >= structures_min, structures_built, f">= {structures_min}")

    quality_sample_min = as_float(req.get("quality_sample_size_min"), 0.0)
    quality_rows = get_nested(report, ["robot_and_quality", "quality_rolls"], {})
    if not isinstance(quality_rows, dict):
        quality_rows = {}
    add_check(
        "quality_rolls_present",
        len(quality_rows) > 0,
        len(quality_rows),
        "> 0",
    )
    required_quality_modules_raw = req.get("required_quality_modules", [])
    required_quality_modules = (
        [str(row) for row in required_quality_modules_raw if isinstance(row, str)]
        if isinstance(required_quality_modules_raw, list)
        else []
    )
    for module_id in required_quality_modules:
        row = quality_rows.get(module_id)
        module_present = isinstance(row, dict)
        sample_size = as_float(get_nested(row, ["sample_size"]), 0.0) if module_present else 0.0
        add_check(
            f"quality_required_module_present::{module_id}",
            module_present and sample_size >= quality_sample_min,
            sample_size if module_present else "missing",
            f">= {quality_sample_min}",
        )
    for module_id, row in quality_rows.items():
        sample_size = as_float(get_nested(row, ["sample_size"]), 0.0)
        add_check(
            f"quality_sample_size_min::{module_id}",
            sample_size >= quality_sample_min,
            sample_size,
            f">= {quality_sample_min}",
        )

    market_delta = as_float(get_nested(report, ["market", "credits_delta"]), 0.0)
    market_delta_min = as_float(req.get("market_credits_delta_min"), -1e12)
    add_check(
        "market_credits_delta_min",
        market_delta >= market_delta_min,
        market_delta,
        f">= {market_delta_min}",
    )

    balanced_expected = as_bool(req.get("ship_space_balanced_can_fit"), True)
    balanced_actual = as_bool(get_nested(report, ["ship_space_and_engagement", "balanced_fit", "can_fit"]), False)
    add_check("ship_space_balanced_can_fit", balanced_actual == balanced_expected, balanced_actual, balanced_expected)

    overloaded_expected = as_bool(req.get("ship_space_overloaded_can_fit"), False)
    overloaded_actual = as_bool(get_nested(report, ["ship_space_and_engagement", "overloaded_fit", "can_fit"]), True)
    add_check(
        "ship_space_overloaded_can_fit",
        overloaded_actual == overloaded_expected,
        overloaded_actual,
        overloaded_expected,
    )

    progression_required = as_bool(req.get("ai_progression_monotonic_non_decreasing"), True)
    progression_rows = get_nested(report, ["ai_battle_matrix", "progression_trend"], [])
    progression_present = isinstance(progression_rows, list) and len(progression_rows) > 0
    monotonic = True
    if isinstance(progression_rows, list) and progression_rows:
        previous = None
        for row in progression_rows:
            value = as_float(get_nested(row, ["average_win_rate_vs_ai_pool"]), 0.0)
            if previous is not None and value + 1e-9 < previous:
                monotonic = False
                break
            previous = value
    add_check(
        "ai_progression_rows_present",
        (not progression_required) or progression_present,
        len(progression_rows) if isinstance(progression_rows, list) else 0,
        "> 0",
    )
    add_check(
        "ai_progression_monotonic_non_decreasing",
        (not progression_required) or (progression_present and monotonic),
        monotonic,
        progression_required,
    )

    market_trades_raw = get_nested(report, ["market", "executed_trades"])
    if isinstance(market_trades_raw, list):
        market_trades = float(len(market_trades_raw))
    else:
        market_trades = as_float(market_trades_raw, 0.0)
    market_trades_min = as_float(req.get("market_executed_trades_min"), 1.0)
    add_check(
        "market_executed_trades_min",
        market_trades >= market_trades_min,
        market_trades,
        f">= {market_trades_min}",
    )

    min_rates = req.get("ai_min_average_win_rates", {})
    max_rates = req.get("ai_max_average_win_rates", {})
    avg_rates = get_nested(report, ["ai_battle_matrix", "build_average_win_rates"], {})
    if not isinstance(avg_rates, dict):
        avg_rates = {}
    if isinstance(min_rates, dict):
        for build_id, min_value in min_rates.items():
            actual = as_float(avg_rates.get(str(build_id)), 0.0)
            expected = as_float(min_value, 0.0)
            add_check(f"ai_min_rate::{build_id}", actual >= expected, actual, f">= {expected}")
    if isinstance(max_rates, dict):
        for build_id, max_value in max_rates.items():
            actual = as_float(avg_rates.get(str(build_id)), 0.0)
            expected = as_float(max_value, 1.0)
            add_check(f"ai_max_rate::{build_id}", actual <= expected, actual, f"<= {expected}")

    failed = [row for row in checks if not bool(row.get("passed"))]
    return {
        "generated_utc": now_utc_iso(),
        "report_meta": get_nested(report, ["meta"], {}),
        "thresholds_meta": {
            "version": thresholds.get("version"),
            "name": thresholds.get("name"),
        },
        "summary": {
            "total_checks": len(checks),
            "failed_checks": len(failed),
            "passed": len(failed) == 0,
        },
        "checks": checks,
    }


def main() -> int:
    args = parse_args()
    report_path = Path(args.report).resolve()
    thresholds_path = Path(args.thresholds).resolve()
    output_path = Path(args.output).resolve()

    try:
        report = load_json(report_path)
        thresholds = load_json(thresholds_path)
        result = run_checks(report=report, thresholds=thresholds)
    except RegressionError as exc:
        print(f"[FAIL] {exc}")
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    summary = result["summary"]
    if bool(summary.get("passed")):
        print("[OK] Simulation regression checks passed.")
        print(f"[INFO] Output: {output_path}")
        return 0

    print("[FAIL] Simulation regression checks failed.")
    print(f"[INFO] Output: {output_path}")
    for row in result["checks"]:
        if not row.get("passed"):
            print(f"- {row['name']}: actual={row['actual']} expected={row['expected']}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
