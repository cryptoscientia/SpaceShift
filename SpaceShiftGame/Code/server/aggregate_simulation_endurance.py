#!/usr/bin/env python3
"""Aggregate multiple simulation suite JSON reports into an endurance summary."""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from check_simulation_regression import run_checks


class AggregateError(RuntimeError):
    """Raised when aggregation inputs are invalid."""


SCRIPT_PATH = Path(__file__).resolve()
SPACESHIFT_ROOT = SCRIPT_PATH.parent.parent.parent
REPORTS_DIR = SPACESHIFT_ROOT / "Reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate simulation suite reports.")
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="One or more simulation suite JSON file paths.",
    )
    parser.add_argument(
        "--output-json",
        required=True,
        help="Output JSON summary path.",
    )
    parser.add_argument(
        "--output-md",
        required=True,
        help="Output markdown summary path.",
    )
    parser.add_argument(
        "--label",
        default="endurance",
        help="Label for this aggregate run.",
    )
    parser.add_argument(
        "--thresholds",
        default=str(REPORTS_DIR / "simulation_thresholds_v1.json"),
        help="Regression thresholds JSON used to evaluate each input report.",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise AggregateError(f"Missing report: {path}")
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AggregateError(f"Invalid JSON report {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise AggregateError(f"Report must be JSON object: {path}")
    return parsed


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise AggregateError(f"Missing JSON file: {path}")
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AggregateError(f"Invalid JSON at {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise AggregateError(f"Expected top-level JSON object: {path}")
    return parsed


def as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return default


def summarize(values: list[float]) -> dict[str, float]:
    if not values:
        return {"count": 0.0, "min": 0.0, "max": 0.0, "mean": 0.0}
    return {
        "count": float(len(values)),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
        "mean": round(statistics.fmean(values), 6),
    }


def build_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# SpaceShift Endurance Aggregate")
    lines.append("")
    lines.append(f"- Label: `{summary['meta']['label']}`")
    lines.append(f"- Generated UTC: `{summary['meta']['generated_utc']}`")
    lines.append(f"- Input reports: `{len(summary['meta']['input_reports'])}`")
    lines.append("")
    lines.append("## Core Stability")
    lines.append("")
    lines.append(f"- Smoke pass count mean: `{summary['smoke']['pass_count']['mean']}`")
    lines.append(f"- Regression pass rate: `{summary['regression']['pass_rate']}`")
    lines.append("")
    lines.append("## Sampling Depth")
    lines.append("")
    lines.append(f"- Worlds scanned mean: `{summary['discovery']['worlds_scanned']['mean']}`")
    lines.append(f"- Quality samples total: `{summary['quality']['total_sample_size']}`")
    lines.append("")
    lines.append("## Balance Signals")
    lines.append("")
    lines.append("- AI average win-rate means by build:")
    for build_id, value in summary["ai"]["mean_win_rates"].items():
        lines.append(f"- `{build_id}`: `{value}`")
    lines.append(f"- Market credits delta mean: `{summary['market']['credits_delta']['mean']}`")
    lines.append("")
    lines.append("## Constraint Checks")
    lines.append("")
    lines.append(
        f"- Balanced fit pass rate: `{summary['ship_space']['balanced_can_fit_rate']}`"
    )
    lines.append(
        f"- Overloaded fit failure rate: `{summary['ship_space']['overloaded_reject_rate']}`"
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    input_paths = [Path(item).resolve() for item in args.inputs]
    reports = [load_report(path) for path in input_paths]
    thresholds_path = Path(args.thresholds).resolve()
    thresholds = load_json_object(thresholds_path)

    smoke_counts: list[float] = []
    worlds_scanned: list[float] = []
    market_deltas: list[float] = []
    quality_totals: list[float] = []
    balanced_flags: list[float] = []
    overloaded_reject_flags: list[float] = []
    regression_pass_flags: list[float] = []

    ai_rates_by_build: dict[str, list[float]] = {}

    for report in reports:
        smoke_counts.append(as_float(report.get("smoke", {}).get("pass_count")))
        worlds_scanned.append(
            as_float(
                report.get("discovery_world_ops", {})
                .get("world_totals", {})
                .get("total_worlds_scanned")
            )
        )
        market_deltas.append(as_float(report.get("market", {}).get("credits_delta")))
        quality_rows = report.get("robot_and_quality", {}).get("quality_rolls", {})
        total_samples = 0.0
        if isinstance(quality_rows, dict):
            for row in quality_rows.values():
                if isinstance(row, dict):
                    total_samples += as_float(row.get("sample_size"))
        quality_totals.append(total_samples)

        balanced = bool(
            report.get("ship_space_and_engagement", {})
            .get("balanced_fit", {})
            .get("can_fit")
        )
        overloaded = bool(
            report.get("ship_space_and_engagement", {})
            .get("overloaded_fit", {})
            .get("can_fit")
        )
        balanced_flags.append(1.0 if balanced else 0.0)
        overloaded_reject_flags.append(1.0 if (not overloaded) else 0.0)

        ai_rates = report.get("ai_battle_matrix", {}).get("build_average_win_rates", {})
        if isinstance(ai_rates, dict):
            for build_id, value in ai_rates.items():
                if not isinstance(build_id, str):
                    continue
                ai_rates_by_build.setdefault(build_id, []).append(as_float(value))

        regression_payload = report.get("regression")
        if isinstance(regression_payload, dict):
            passed = bool(regression_payload.get("summary", {}).get("passed"))
            regression_pass_flags.append(1.0 if passed else 0.0)
        elif isinstance(report.get("regression_passed"), bool):
            regression_pass_flags.append(1.0 if bool(report.get("regression_passed")) else 0.0)
        else:
            regression_result = run_checks(report=report, thresholds=thresholds)
            passed = bool(regression_result.get("summary", {}).get("passed"))
            regression_pass_flags.append(1.0 if passed else 0.0)

    mean_win_rates = {
        build_id: round(statistics.fmean(values), 6) if values else 0.0
        for build_id, values in sorted(ai_rates_by_build.items())
    }

    summary: dict[str, Any] = {
        "meta": {
            "label": args.label,
            "generated_utc": now_utc_iso(),
            "input_reports": [str(path) for path in input_paths],
            "thresholds_path": str(thresholds_path),
        },
        "smoke": {
            "pass_count": summarize(smoke_counts),
        },
        "discovery": {
            "worlds_scanned": summarize(worlds_scanned),
        },
        "quality": {
            "total_sample_size": int(round(sum(quality_totals))),
            "sample_size_per_run": summarize(quality_totals),
        },
        "ai": {
            "mean_win_rates": mean_win_rates,
        },
        "market": {
            "credits_delta": summarize(market_deltas),
        },
        "ship_space": {
            "balanced_can_fit_rate": round(statistics.fmean(balanced_flags), 6) if balanced_flags else 0.0,
            "overloaded_reject_rate": round(statistics.fmean(overloaded_reject_flags), 6)
            if overloaded_reject_flags
            else 0.0,
        },
        "regression": {
            "samples": len(regression_pass_flags),
            "pass_rate": round(statistics.fmean(regression_pass_flags), 6) if regression_pass_flags else 0.0,
        },
    }

    output_json = Path(args.output_json).resolve()
    output_md = Path(args.output_md).resolve()
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    output_md.write_text(build_markdown(summary), encoding="utf-8")

    print("[OK] Endurance aggregation complete.")
    print(f"[INFO] JSON: {output_json}")
    print(f"[INFO] MD: {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
