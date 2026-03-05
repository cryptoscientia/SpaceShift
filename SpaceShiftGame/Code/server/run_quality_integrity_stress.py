#!/usr/bin/env python3
"""Stress-test quality roll rarity and stat-family crossover integrity."""

from __future__ import annotations

import argparse
import json
import math
import random
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from mock_server import MockServerHandler, SeedStore, resolve_default_seed_dir, stable_hash_int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run 10k+ quality roll stress tests across modules/hulls and detect "
            "stat-family crossover issues."
        )
    )
    parser.add_argument(
        "--seed-dir",
        type=Path,
        default=resolve_default_seed_dir(),
        help="Seed directory used by mock_server.py",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=10_000,
        help="Total quality roll samples to generate",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(time.time()),
        help="Deterministic RNG seed",
    )
    parser.add_argument(
        "--tag",
        default=time.strftime("%Y-%m-%d"),
        help="Output tag used in report file names",
    )
    return parser.parse_args()


def percentile_from_sorted(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if p <= 0:
        return values[0]
    if p >= 100:
        return values[-1]
    idx = (len(values) - 1) * (p / 100.0)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return values[lo]
    frac = idx - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def rarity_from_roll(
    *,
    score: float,
    percentile_estimate: float,
    impact_score: float,
    jackpot: bool,
) -> str:
    rarity_index = score
    rarity_index += max(0.0, (percentile_estimate - 50.0) / 1200.0)
    rarity_index += min(0.06, impact_score / 5500.0)
    if jackpot:
        rarity_index += 0.035
    if rarity_index >= 1.27:
        return "mythic"
    if rarity_index >= 1.17:
        return "legendary"
    if rarity_index >= 1.08:
        return "rare"
    if rarity_index >= 0.98:
        return "uncommon"
    return "common"


def module_stat_crossover(
    *,
    family: str,
    stat_key: str,
    value: float,
) -> str | None:
    family = family.casefold().strip()
    stat_key = stat_key.strip()

    if family.startswith("weapon_"):
        disallowed = {
            "shield",
            "hull",
            "cargo",
            "cargo_capacity_tons",
            "crew_capacity",
            "passenger_capacity",
            "market_efficiency",
            "mining_yield",
            "repair_rate_pct",
            "action_energy_max",
            "action_energy_regen",
            "fighter_bay",
            "launch_tube",
        }
        if stat_key in disallowed:
            return "weapon_non_weapon_domain"
    if family == "scanner" and stat_key == "attack":
        return "scanner_attack_domain"
    if family == "reactor" and stat_key in {"cloak"}:
        return "reactor_cloak_domain"
    if family == "reactor" and stat_key == "stealth_signature_pct" and value < 0:
        return "reactor_stealth_buff_domain"
    if family in {"shield", "armor"} and stat_key == "attack":
        return "defense_attack_domain"
    return None


def ensure_report_dir(seed_dir: Path) -> Path:
    project_root = seed_dir.parents[1]
    report_dir = project_root / "Reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def build_catalog(store: SeedStore) -> list[tuple[str, dict[str, Any]]]:
    catalog: list[tuple[str, dict[str, Any]]] = []
    for row in store.modules:
        if not isinstance(row, dict):
            continue
        if not isinstance(row.get("id"), str):
            continue
        if not isinstance(row.get("family"), str):
            continue
        catalog.append(("module", row))
    for row in store.ship_hulls:
        if not isinstance(row, dict):
            continue
        if not isinstance(row.get("id"), str):
            continue
        catalog.append(("hull", row))
    return catalog


def run_stress(store: SeedStore, samples: int, seed: int) -> dict[str, Any]:
    if samples <= 0:
        raise ValueError("--samples must be > 0")
    catalog = build_catalog(store)
    if not catalog:
        raise RuntimeError("No module/hull entries available in seeds")

    rng = random.Random(seed)
    handler = MockServerHandler.__new__(MockServerHandler)
    handler.seed_store = store
    rng.shuffle(catalog)

    quality_tier_counts: Counter[str] = Counter()
    rarity_counts: Counter[str] = Counter()
    family_rarity_counts: dict[str, Counter[str]] = defaultdict(Counter)
    item_rarity_counts: dict[str, Counter[str]] = defaultdict(Counter)
    jackpot_count = 0
    scores: list[float] = []
    percentile_scores: list[float] = []
    impact_scores: list[float] = []

    seed_crossover_violations: list[dict[str, Any]] = []
    roll_crossover_violations: list[dict[str, Any]] = []
    mythic_crossover_exceptions: list[dict[str, Any]] = []

    for kind, item in catalog:
        if kind != "module":
            continue
        family = str(item.get("family", "unknown"))
        stats = item.get("stat_bonuses", {})
        if not isinstance(stats, dict):
            continue
        for stat_key, raw in stats.items():
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue
            violation = module_stat_crossover(
                family=family,
                stat_key=str(stat_key),
                value=float(raw),
            )
            if violation is None:
                continue
            seed_crossover_violations.append(
                {
                    "item_id": item.get("id"),
                    "family": family,
                    "stat_key": stat_key,
                    "value": float(raw),
                    "rule": violation,
                }
            )

    for idx in range(samples):
        if idx < len(catalog):
            kind, item = catalog[idx]
        else:
            kind, item = catalog[rng.randrange(0, len(catalog))]
        item_id = str(item.get("id", f"{kind}.unknown"))
        tier = int(item.get("tier", 1)) if isinstance(item.get("tier"), int) else 1
        local_rng = random.Random(stable_hash_int(seed, idx, item_id, tier))
        quality = handler._roll_quality_profile(
            item_kind=kind,
            item=item,
            rng=local_rng,
            player_id=None,
        )

        score = float(quality.get("quality_score", 1.0))
        percentile_estimate = float(quality.get("quality_percentile_estimate", 50.0))
        jackpot = bool(quality.get("jackpot_triggered", False))
        rolled_stats = quality.get("rolled_stats_preview", {})
        impact = 0.0
        if isinstance(rolled_stats, dict):
            for stat_key in ("attack", "defense", "hull", "shield", "energy", "scan", "cloak"):
                raw = rolled_stats.get(stat_key)
                if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                    continue
                impact += abs(float(raw))

        rarity = rarity_from_roll(
            score=score,
            percentile_estimate=percentile_estimate,
            impact_score=impact,
            jackpot=jackpot,
        )
        quality_tier = str(quality.get("quality_tier", "unknown"))
        family = str(item.get("family", "hull")) if kind == "module" else "hull"

        quality_tier_counts[quality_tier] += 1
        rarity_counts[rarity] += 1
        family_rarity_counts[family][rarity] += 1
        item_rarity_counts[item_id][rarity] += 1
        if jackpot:
            jackpot_count += 1
        scores.append(score)
        percentile_scores.append(percentile_estimate)
        impact_scores.append(impact)

        if kind == "module":
            affix_stats = quality.get("affix_stat_bonuses", {})
            if isinstance(affix_stats, dict) and affix_stats:
                template = handler._module_synergy_template(item)
                focus_stat = str(template.get("focus_stat", "attack"))
                for affix_key, raw in affix_stats.items():
                    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                        continue
                    if str(affix_key) == focus_stat:
                        continue
                    row = {
                        "item_id": item_id,
                        "family": family,
                        "affix_stat": str(affix_key),
                        "focus_stat_expected": focus_stat,
                        "value": float(raw),
                        "rarity": rarity,
                        "quality_tier": quality_tier,
                        "sample_index": idx,
                        "rule": "affix_focus_mismatch",
                    }
                    if rarity == "mythic":
                        mythic_crossover_exceptions.append(row)
                    else:
                        roll_crossover_violations.append(row)

            if isinstance(rolled_stats, dict):
                for stat_key, raw in rolled_stats.items():
                    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                        continue
                    rule = module_stat_crossover(
                        family=family,
                        stat_key=str(stat_key),
                        value=float(raw),
                    )
                    if rule is None:
                        continue
                    row = {
                        "item_id": item_id,
                        "family": family,
                        "stat_key": str(stat_key),
                        "value": float(raw),
                        "rarity": rarity,
                        "quality_tier": quality_tier,
                        "sample_index": idx,
                        "rule": rule,
                    }
                    if rarity == "mythic":
                        mythic_crossover_exceptions.append(row)
                    else:
                        roll_crossover_violations.append(row)

    scores_sorted = sorted(scores)
    top_items = sorted(
        item_rarity_counts.items(),
        key=lambda row: (int(row[1].get("mythic", 0)), int(row[1].get("legendary", 0))),
        reverse=True,
    )[:20]

    return {
        "seed": seed,
        "samples": samples,
        "catalog_sizes": {
            "modules": len([1 for kind, _ in catalog if kind == "module"]),
            "hulls": len([1 for kind, _ in catalog if kind == "hull"]),
            "total": len(catalog),
        },
        "quality_tier_distribution": dict(sorted(quality_tier_counts.items())),
        "rarity_distribution": {
            key: int(rarity_counts.get(key, 0))
            for key in ("common", "uncommon", "rare", "legendary", "mythic")
        },
        "jackpot": {
            "hits": jackpot_count,
            "hit_rate": round(jackpot_count / max(1, samples), 6),
        },
        "score_summary": {
            "mean": round(sum(scores) / max(1, len(scores)), 6),
            "min": round(scores_sorted[0] if scores_sorted else 0.0, 6),
            "p50": round(percentile_from_sorted(scores_sorted, 50.0), 6),
            "p90": round(percentile_from_sorted(scores_sorted, 90.0), 6),
            "p95": round(percentile_from_sorted(scores_sorted, 95.0), 6),
            "p99": round(percentile_from_sorted(scores_sorted, 99.0), 6),
            "max": round(scores_sorted[-1] if scores_sorted else 0.0, 6),
        },
        "percentile_estimate_summary": {
            "mean": round(sum(percentile_scores) / max(1, len(percentile_scores)), 4),
            "p95": round(percentile_from_sorted(sorted(percentile_scores), 95.0), 4),
            "p99": round(percentile_from_sorted(sorted(percentile_scores), 99.0), 4),
        },
        "impact_summary": {
            "mean": round(sum(impact_scores) / max(1, len(impact_scores)), 4),
            "p95": round(percentile_from_sorted(sorted(impact_scores), 95.0), 4),
            "p99": round(percentile_from_sorted(sorted(impact_scores), 99.0), 4),
        },
        "family_rarity_distribution": {
            family: {
                key: int(counter.get(key, 0))
                for key in ("common", "uncommon", "rare", "legendary", "mythic")
            }
            for family, counter in sorted(family_rarity_counts.items())
        },
        "top_items_by_high_rarity": [
            {
                "item_id": item_id,
                "mythic": int(counter.get("mythic", 0)),
                "legendary": int(counter.get("legendary", 0)),
                "rare": int(counter.get("rare", 0)),
            }
            for item_id, counter in top_items
        ],
        "crossover_integrity": {
            "seed_violations_total": len(seed_crossover_violations),
            "roll_violations_total": len(roll_crossover_violations),
            "mythic_exception_total": len(mythic_crossover_exceptions),
            "seed_violations": seed_crossover_violations[:120],
            "roll_violations": roll_crossover_violations[:120],
            "mythic_exceptions": mythic_crossover_exceptions[:120],
        },
    }


def render_markdown(report: dict[str, Any], json_file: Path) -> str:
    rarity = report.get("rarity_distribution", {})
    jackpot = report.get("jackpot", {})
    score = report.get("score_summary", {})
    crossover = report.get("crossover_integrity", {})
    lines = [
        "# SpaceShift Quality Integrity Stress Report",
        "",
        f"- Source report JSON: `{json_file.name}`",
        f"- Samples: `{report.get('samples')}`",
        f"- Seed: `{report.get('seed')}`",
        "",
        "## Jackpot + Rarity",
        "",
        f"- Jackpot hits: `{jackpot.get('hits')}`",
        f"- Jackpot hit rate: `{jackpot.get('hit_rate')}`",
        (
            "- Rarity distribution: "
            f"`common={rarity.get('common', 0)}`, "
            f"`uncommon={rarity.get('uncommon', 0)}`, "
            f"`rare={rarity.get('rare', 0)}`, "
            f"`legendary={rarity.get('legendary', 0)}`, "
            f"`mythic={rarity.get('mythic', 0)}`"
        ),
        "",
        "## Score Summary",
        "",
        (
            f"- score mean=`{score.get('mean')}` "
            f"p95=`{score.get('p95')}` "
            f"p99=`{score.get('p99')}` "
            f"max=`{score.get('max')}`"
        ),
        "",
        "## Crossover Integrity",
        "",
        f"- Seed rule violations: `{crossover.get('seed_violations_total')}`",
        f"- Roll-time violations (non-mythic): `{crossover.get('roll_violations_total')}`",
        f"- Mythic exceptions observed: `{crossover.get('mythic_exception_total')}`",
        "",
        "## Notes",
        "",
        "- Cross-domain effects are expected only for mythic-tier edge cases.",
        "- If non-mythic roll violations are non-zero, tighten family stat maps in module seeds or roll logic.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    store = SeedStore.load(args.seed_dir)
    report = run_stress(store=store, samples=int(args.samples), seed=int(args.seed))
    report_dir = ensure_report_dir(args.seed_dir)
    json_path = report_dir / f"quality_integrity_stress_{args.tag}.json"
    md_path = report_dir / f"quality_integrity_stress_{args.tag}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report, json_path), encoding="utf-8")
    print(f"[OK] Wrote {json_path}")
    print(f"[OK] Wrote {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
