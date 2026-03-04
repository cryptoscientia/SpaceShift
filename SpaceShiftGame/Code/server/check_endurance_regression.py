#!/usr/bin/env python3
"""Evaluate endurance aggregate outputs against baseline thresholds."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class EnduranceRegressionError(RuntimeError):
    """Raised when endurance regression checks fail or inputs are invalid."""


SCRIPT_PATH = Path(__file__).resolve()
SERVER_DIR = SCRIPT_PATH.parent
SPACESHIFT_ROOT = SERVER_DIR.parent.parent
REPORTS_DIR = SPACESHIFT_ROOT / "Reports"

DEFAULT_REPORT = REPORTS_DIR / "latest_endurance_standard.json"
DEFAULT_THRESHOLDS = REPORTS_DIR / "endurance_thresholds_v1.json"
DEFAULT_OUTPUT = REPORTS_DIR / "endurance_regression_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check SpaceShift endurance aggregate report against thresholds."
    )
    parser.add_argument(
        "--report",
        default=str(DEFAULT_REPORT),
        help="Path to endurance aggregate report JSON.",
    )
    parser.add_argument(
        "--thresholds",
        default=str(DEFAULT_THRESHOLDS),
        help="Path to endurance thresholds JSON.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Path to write endurance regression JSON summary.",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise EnduranceRegressionError(f"Missing required file: {path}")
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise EnduranceRegressionError(f"Invalid JSON at {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise EnduranceRegressionError(f"Expected top-level JSON object at {path}")
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


def as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    return default


def run_checks(report: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    req = thresholds.get("required", {})
    if not isinstance(req, dict):
        raise EnduranceRegressionError("thresholds.required must be an object")

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

    run_count = as_int(get_nested(report, ["smoke", "pass_count", "count"]), 0)
    run_count_min = as_int(req.get("run_count_min"), 2)
    add_check("run_count_min", run_count >= run_count_min, run_count, f">= {run_count_min}")

    smoke_mean = as_float(get_nested(report, ["smoke", "pass_count", "mean"]), 0.0)
    smoke_mean_min = as_float(req.get("smoke_pass_count_mean_min"), 0.0)
    add_check(
        "smoke_pass_count_mean_min",
        smoke_mean >= smoke_mean_min,
        smoke_mean,
        f">= {smoke_mean_min}",
    )

    regression_pass_rate = as_float(get_nested(report, ["regression", "pass_rate"]), 0.0)
    regression_pass_rate_min = as_float(req.get("regression_pass_rate_min"), 1.0)
    add_check(
        "regression_pass_rate_min",
        regression_pass_rate >= regression_pass_rate_min,
        regression_pass_rate,
        f">= {regression_pass_rate_min}",
    )

    worlds_mean = as_float(get_nested(report, ["discovery", "worlds_scanned", "mean"]), 0.0)
    worlds_mean_min = as_float(req.get("worlds_scanned_mean_min"), 0.0)
    add_check(
        "worlds_scanned_mean_min",
        worlds_mean >= worlds_mean_min,
        worlds_mean,
        f">= {worlds_mean_min}",
    )

    quality_mean = as_float(get_nested(report, ["quality", "sample_size_per_run", "mean"]), 0.0)
    quality_mean_min = as_float(req.get("quality_sample_size_per_run_mean_min"), 0.0)
    add_check(
        "quality_sample_size_per_run_mean_min",
        quality_mean >= quality_mean_min,
        quality_mean,
        f">= {quality_mean_min}",
    )

    balanced_rate = as_float(get_nested(report, ["ship_space", "balanced_can_fit_rate"]), 0.0)
    balanced_rate_min = as_float(req.get("ship_space_balanced_can_fit_rate_min"), 1.0)
    add_check(
        "ship_space_balanced_can_fit_rate_min",
        balanced_rate >= balanced_rate_min,
        balanced_rate,
        f">= {balanced_rate_min}",
    )

    overloaded_reject_rate = as_float(get_nested(report, ["ship_space", "overloaded_reject_rate"]), 0.0)
    overloaded_reject_rate_min = as_float(req.get("ship_space_overloaded_reject_rate_min"), 1.0)
    add_check(
        "ship_space_overloaded_reject_rate_min",
        overloaded_reject_rate >= overloaded_reject_rate_min,
        overloaded_reject_rate,
        f">= {overloaded_reject_rate_min}",
    )

    market_delta_mean = as_float(get_nested(report, ["market", "credits_delta", "mean"]), 0.0)
    market_delta_min = as_float(req.get("market_credits_delta_mean_min"), -1e12)
    market_delta_max = as_float(req.get("market_credits_delta_mean_max"), 1e12)
    add_check(
        "market_credits_delta_mean_min",
        market_delta_mean >= market_delta_min,
        market_delta_mean,
        f">= {market_delta_min}",
    )
    add_check(
        "market_credits_delta_mean_max",
        market_delta_mean <= market_delta_max,
        market_delta_mean,
        f"<= {market_delta_max}",
    )

    ai_rates = get_nested(report, ["ai", "mean_win_rates"], {})
    if not isinstance(ai_rates, dict):
        ai_rates = {}
    min_rates = req.get("ai_min_average_win_rates", {})
    max_rates = req.get("ai_max_average_win_rates", {})
    if isinstance(min_rates, dict):
        for build_id, min_value in min_rates.items():
            actual = as_float(ai_rates.get(str(build_id)), 0.0)
            expected = as_float(min_value, 0.0)
            add_check(f"ai_min_rate::{build_id}", actual >= expected, actual, f">= {expected}")
    if isinstance(max_rates, dict):
        for build_id, max_value in max_rates.items():
            actual = as_float(ai_rates.get(str(build_id)), 0.0)
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

    report = load_json(report_path)
    thresholds = load_json(thresholds_path)
    result = run_checks(report=report, thresholds=thresholds)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    if not bool(result.get("summary", {}).get("passed")):
        failed = int(result.get("summary", {}).get("failed_checks", 0))
        total = int(result.get("summary", {}).get("total_checks", 0))
        print(f"[FAIL] Endurance regression checks failed ({failed}/{total} failed)")
        print(f"[INFO] Output: {output_path}")
        return 1

    print("[OK] Endurance regression checks passed.")
    print(f"[INFO] Output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
