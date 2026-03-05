#!/usr/bin/env python3
"""Run multi-scenario SimPy sweeps for SpaceShift balance analysis."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from simpy_timeflow import run_simpy_timeflow


SCRIPT_PATH = Path(__file__).resolve()
SPACESHIFT_ROOT = SCRIPT_PATH.parent.parent.parent
REPORTS_DIR = SPACESHIFT_ROOT / "Reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SpaceShift SimPy scenario sweeps.")
    parser.add_argument(
        "--profiles",
        nargs="+",
        choices=["standard", "long"],
        default=["standard", "long"],
        help="Profiles to run. Defaults to both.",
    )
    parser.add_argument(
        "--runs-per-scenario",
        type=int,
        default=8,
        help="Number of seeded runs per scenario/profile.",
    )
    parser.add_argument(
        "--seed-base",
        type=int,
        default=20260304,
        help="Base deterministic seed.",
    )
    parser.add_argument(
        "--tag",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d_simpy_sweep"),
        help="Output tag used in report filenames.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional explicit output JSON path.",
    )
    parser.add_argument(
        "--output-md",
        default="",
        help="Optional explicit output markdown path.",
    )
    return parser.parse_args()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    pos = (max(0.0, min(100.0, pct)) / 100.0) * (len(values) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(values[lo])
    w = pos - lo
    return float(values[lo] * (1.0 - w) + values[hi] * w)


def summarize(values: list[float]) -> dict[str, float]:
    ordered = sorted(float(v) for v in values)
    if not ordered:
        return {
            "count": 0.0,
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "p10": 0.0,
            "p50": 0.0,
            "p90": 0.0,
        }
    return {
        "count": float(len(ordered)),
        "min": round(min(ordered), 6),
        "max": round(max(ordered), 6),
        "mean": round(statistics.fmean(ordered), 6),
        "p10": round(percentile(ordered, 10.0), 6),
        "p50": round(percentile(ordered, 50.0), 6),
        "p90": round(percentile(ordered, 90.0), 6),
    }


def profile_defaults(profile: str) -> dict[str, Any]:
    if str(profile).casefold() == "long":
        return {
            "simpy_horizon_hours": 720.0,
            "simpy_players": 128,
            "simpy_compute_slots": 18,
            "simpy_fab_slots": 14,
            "simpy_market_liquidity": 2600.0,
        }
    return {
        "simpy_horizon_hours": 240.0,
        "simpy_players": 48,
        "simpy_compute_slots": 12,
        "simpy_fab_slots": 9,
        "simpy_market_liquidity": 1800.0,
    }


def scenario_overrides(profile: str, scenario: str) -> dict[str, Any]:
    base = profile_defaults(profile)
    if scenario == "baseline":
        return dict(base)
    if scenario == "queue_pressure":
        return {
            **base,
            "simpy_players": int(round(float(base["simpy_players"]) * 1.35)),
            "simpy_compute_slots": max(1, int(base["simpy_compute_slots"]) - 2),
            "simpy_fab_slots": max(1, int(base["simpy_fab_slots"]) - 2),
            "simpy_market_liquidity": float(base["simpy_market_liquidity"]) * 0.95,
        }
    if scenario == "infra_expansion":
        return {
            **base,
            "simpy_players": int(round(float(base["simpy_players"]) * 1.15)),
            "simpy_compute_slots": int(round(float(base["simpy_compute_slots"]) * 1.45)),
            "simpy_fab_slots": int(round(float(base["simpy_fab_slots"]) * 1.4)),
            "simpy_market_liquidity": float(base["simpy_market_liquidity"]) * 1.2,
        }
    if scenario == "thin_market":
        return {
            **base,
            "simpy_market_liquidity": float(base["simpy_market_liquidity"]) * 0.55,
        }
    raise ValueError(f"Unsupported scenario: {scenario}")


def collect_metrics(payload: dict[str, Any]) -> dict[str, float]:
    queue = payload.get("queue_dynamics", {})
    market = payload.get("market_dynamics", {})
    extraction = payload.get("extraction_logistics", {})
    cross = payload.get("cross_effects", {})
    research = queue.get("research", {})
    manufacturing = queue.get("manufacturing", {})
    trades = market.get("trades", {})
    totals = extraction.get("totals", {})
    return {
        "queue_stress_jobs_per_player": float(
            cross.get("queue_stress_index_jobs_per_player", 0.0)
        ),
        "supply_pressure_index": float(cross.get("supply_pressure_index", 0.0)),
        "market_price_ratio": float(cross.get("market_price_vs_anchor_ratio", 0.0)),
        "research_wait_p95_h": float(research.get("wait_p95_h", 0.0)),
        "manufacturing_wait_p95_h": float(manufacturing.get("wait_p95_h", 0.0)),
        "research_backlog_jobs": float(research.get("backlog_jobs", 0.0)),
        "manufacturing_backlog_jobs": float(manufacturing.get("backlog_jobs", 0.0)),
        "market_vol_day": float(market.get("annualized_like_volatility_day", 0.0)),
        "market_shortage_events": float(trades.get("shortage_events", 0.0)),
        "delivered_units": float(totals.get("delivered_units", 0.0)),
        "credits_generated": float(totals.get("credits_generated", 0.0)),
        "craftable_batches_eq": float(totals.get("craftable_module_batches_eq", 0.0)),
    }


def analyze_runs(
    *,
    profile: str,
    scenario: str,
    runs_per_scenario: int,
    seed_base: int,
) -> dict[str, Any]:
    run_rows: list[dict[str, Any]] = []
    metric_values: dict[str, list[float]] = {}
    cfg = scenario_overrides(profile, scenario)

    for idx in range(max(1, runs_per_scenario)):
        seed = int(seed_base + (idx * 7919))
        payload = run_simpy_timeflow(seed=seed, profile=profile, config=cfg)
        metrics = collect_metrics(payload)
        for key, value in metrics.items():
            metric_values.setdefault(key, []).append(float(value))
        run_rows.append(
            {
                "seed": seed,
                "metrics": {k: round(float(v), 6) for k, v in metrics.items()},
            }
        )

    summary = {key: summarize(values) for key, values in sorted(metric_values.items())}
    return {
        "profile": profile,
        "scenario": scenario,
        "runs": int(len(run_rows)),
        "config": cfg,
        "summary": summary,
        "sample_runs_first3": run_rows[:3],
        "sample_runs_last3": run_rows[-3:],
    }


def build_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# SpaceShift SimPy Sweep")
    lines.append("")
    lines.append(f"- Generated UTC: `{payload['meta']['generated_utc']}`")
    lines.append(f"- Runs per scenario: `{payload['meta']['runs_per_scenario']}`")
    lines.append(f"- Profiles: `{', '.join(payload['meta']['profiles'])}`")
    lines.append("")
    for profile, scenarios in payload.get("results", {}).items():
        lines.append(f"## Profile: {profile}")
        lines.append("")
        for scenario_name, row in scenarios.items():
            lines.append(f"### Scenario: {scenario_name}")
            lines.append("")
            summary = row.get("summary", {})
            lines.append(
                f"- Queue stress mean (jobs/player): `{summary.get('queue_stress_jobs_per_player', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Supply pressure mean: `{summary.get('supply_pressure_index', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Research wait p95 mean (h): `{summary.get('research_wait_p95_h', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Manufacturing wait p95 mean (h): `{summary.get('manufacturing_wait_p95_h', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Market ratio mean (final/anchor): `{summary.get('market_price_ratio', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Market volatility/day mean: `{summary.get('market_vol_day', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Shortage events mean: `{summary.get('market_shortage_events', {}).get('mean', 0.0)}`"
            )
            lines.append(
                f"- Delivered units mean: `{summary.get('delivered_units', {}).get('mean', 0.0)}`"
            )
            lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    runs_per_scenario = max(1, int(args.runs_per_scenario))
    profiles = [str(item).casefold() for item in args.profiles]
    scenarios = ["baseline", "queue_pressure", "infra_expansion", "thin_market"]

    results: dict[str, dict[str, Any]] = {}
    seed_cursor = int(args.seed_base)
    for profile in profiles:
        profile_rows: dict[str, Any] = {}
        for scenario in scenarios:
            profile_rows[scenario] = analyze_runs(
                profile=profile,
                scenario=scenario,
                runs_per_scenario=runs_per_scenario,
                seed_base=seed_cursor,
            )
            seed_cursor += runs_per_scenario * 97
        results[profile] = profile_rows

    summary = {
        "meta": {
            "generated_utc": now_utc_iso(),
            "runs_per_scenario": runs_per_scenario,
            "profiles": profiles,
            "scenarios": scenarios,
            "seed_base": int(args.seed_base),
        },
        "results": results,
    }

    output_json = (
        Path(args.output_json).resolve()
        if args.output_json
        else (REPORTS_DIR / f"simpy_sweep_{args.tag}.json").resolve()
    )
    output_md = (
        Path(args.output_md).resolve()
        if args.output_md
        else (REPORTS_DIR / f"simpy_sweep_{args.tag}.md").resolve()
    )
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    output_md.write_text(build_markdown(summary), encoding="utf-8")

    print("[OK] SimPy sweep complete.")
    print(f"[INFO] JSON: {output_json}")
    print(f"[INFO] MD: {output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
