#!/usr/bin/env python3
"""Run reproducible SpaceShift simulation suites and export report artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

from simpy_timeflow import run_simpy_timeflow


class SuiteError(RuntimeError):
    """Raised when the simulation suite fails."""


SCRIPT_PATH = Path(__file__).resolve()
SERVER_DIR = SCRIPT_PATH.parent
SPACESHIFT_ROOT = SERVER_DIR.parent.parent
PROJECT_ROOT = SPACESHIFT_ROOT.parent
MOCK_SERVER = SERVER_DIR / "mock_server.py"
SMOKE_SCRIPT = SERVER_DIR / "smoke_test.py"
REPORTS_DIR = SPACESHIFT_ROOT / "Reports"
PLAYER_TOKENS: dict[str, str] = {}
REQUEST_POLICY_DEFAULTS: dict[str, float | int] = {
    "timeout_scale": 1.0,
    "max_attempts": 3,
    "retry_backoff_base_seconds": 0.2,
    "retry_backoff_cap_seconds": 2.0,
}
REQUEST_POLICY: dict[str, float | int] = dict(REQUEST_POLICY_DEFAULTS)


def stable_hash_int(*parts: Any) -> int:
    digest = hashlib.sha256(
        "|".join(str(part) for part in parts).encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SpaceShift simulation suites and write reports.")
    parser.add_argument("--host", default="127.0.0.1", help="Host for managed server.")
    parser.add_argument("--port", type=int, default=0, help="Port for managed server (0 = auto).")
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=8.0,
        help="HTTP timeout seconds per request.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=20.0,
        help="Server startup timeout seconds.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260302,
        help="Deterministic seed for suite randomness.",
    )
    parser.add_argument(
        "--tag",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Report file tag/date suffix.",
    )
    parser.add_argument(
        "--profile",
        choices=["standard", "long"],
        default="standard",
        help="Simulation profile. 'long' runs significantly larger Monte Carlo/sample sweeps.",
    )
    return parser.parse_args()


def simulation_profile_config(profile: str) -> dict[str, Any]:
    if profile == "long":
        return {
            "name": "long",
            "discovery_scans_per_class": 14,
            "discovery_count_per_scan": 7,
            "celestial_scans_per_class": 8,
            "celestial_scan_count": 6,
            "celestial_inventory_limit": 60,
            "quality_batches_per_module": 14,
            "quality_quantity_min": 20,
            "quality_quantity_max": 26,
            "fitting_runs_robot": 220,
            "fitting_runs_ai": 220,
            "fitting_runs_ship_space": 220,
            "contacts_count": 20,
            "market_trade_cycles": 8,
            "market_trade_cycle_pause_seconds": 0.03,
            "covert_iterations": 72,
            "celestial_scan_pause_seconds": 0.02,
            "celestial_quote_timeout": 120.0,
            "http_timeout_scale": 2.5,
            "http_max_attempts": 6,
            "http_retry_backoff_base_seconds": 0.35,
            "http_retry_backoff_cap_seconds": 4.0,
            "simpy_horizon_hours": 720.0,
            "simpy_players": 128,
            "simpy_compute_slots": 18,
            "simpy_fab_slots": 14,
            "simpy_market_liquidity": 2600.0,
        }
    return {
        "name": "standard",
        "discovery_scans_per_class": 8,
        "discovery_count_per_scan": 6,
        "celestial_scans_per_class": 4,
        "celestial_scan_count": 5,
        "celestial_inventory_limit": 60,
        "quality_batches_per_module": 8,
        "quality_quantity_min": 18,
        "quality_quantity_max": 24,
        "fitting_runs_robot": 100,
        "fitting_runs_ai": 120,
        "fitting_runs_ship_space": 120,
        "contacts_count": 14,
        "market_trade_cycles": 1,
        "market_trade_cycle_pause_seconds": 0.0,
        "covert_iterations": 30,
        "celestial_scan_pause_seconds": 0.0,
        "celestial_quote_timeout": 60.0,
        "http_timeout_scale": 1.0,
        "http_max_attempts": 3,
        "http_retry_backoff_base_seconds": 0.2,
        "http_retry_backoff_cap_seconds": 2.0,
        "simpy_horizon_hours": 240.0,
        "simpy_players": 48,
        "simpy_compute_slots": 12,
        "simpy_fab_slots": 9,
        "simpy_market_liquidity": 1800.0,
    }


def configure_request_policy(config: dict[str, Any]) -> dict[str, float | int]:
    REQUEST_POLICY.clear()
    REQUEST_POLICY.update(REQUEST_POLICY_DEFAULTS)
    timeout_scale = config.get("http_timeout_scale")
    max_attempts = config.get("http_max_attempts")
    backoff_base = config.get("http_retry_backoff_base_seconds")
    backoff_cap = config.get("http_retry_backoff_cap_seconds")
    if isinstance(timeout_scale, (int, float)) and not isinstance(timeout_scale, bool):
        REQUEST_POLICY["timeout_scale"] = max(0.5, min(10.0, float(timeout_scale)))
    if isinstance(max_attempts, int) and not isinstance(max_attempts, bool):
        REQUEST_POLICY["max_attempts"] = max(1, min(12, int(max_attempts)))
    if isinstance(backoff_base, (int, float)) and not isinstance(backoff_base, bool):
        REQUEST_POLICY["retry_backoff_base_seconds"] = max(0.05, min(5.0, float(backoff_base)))
    if isinstance(backoff_cap, (int, float)) and not isinstance(backoff_cap, bool):
        REQUEST_POLICY["retry_backoff_cap_seconds"] = max(0.1, min(10.0, float(backoff_cap)))
    return dict(REQUEST_POLICY)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def build_url(base_url: str, path: str, query: dict[str, Any] | None = None) -> str:
    base = base_url.rstrip("/")
    if not query:
        return f"{base}{path}"
    return f"{base}{path}?{urlencode(query)}"


def request_json(
    *,
    base_url: str,
    method: str,
    path: str,
    timeout: float,
    query: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = build_url(base_url, path, query=query)
    headers = {"Accept": "application/json"}
    player_id: str | None = None
    parsed_url = urlparse(url)
    query_rows = parse_qs(parsed_url.query, keep_blank_values=True)
    query_player_values = query_rows.get("player_id", [])
    if query_player_values and isinstance(query_player_values[0], str):
        query_player = query_player_values[0].strip()
        if query_player:
            player_id = query_player
    if isinstance(payload, dict):
        payload_player = payload.get("player_id")
        if isinstance(payload_player, str) and payload_player.strip():
            player_id = payload_player.strip()
    if isinstance(player_id, str):
        token = PLAYER_TOKENS.get(player_id)
        if isinstance(token, str) and token.strip():
            headers["Authorization"] = f"Bearer {token.strip()}"
    body: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    request = Request(url=url, method=method, headers=headers, data=body)
    raw = ""
    timeout_scale = float(REQUEST_POLICY.get("timeout_scale", 1.0))
    max_attempts = int(REQUEST_POLICY.get("max_attempts", 3))
    retry_base = float(REQUEST_POLICY.get("retry_backoff_base_seconds", 0.2))
    retry_cap = float(REQUEST_POLICY.get("retry_backoff_cap_seconds", 2.0))
    effective_timeout = max(0.25, float(timeout) * max(0.5, timeout_scale))
    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(request, timeout=effective_timeout) as response:
                raw = response.read().decode("utf-8", errors="replace")
            break
        except TimeoutError as exc:
            if attempt < max_attempts:
                delay = min(max(0.05, retry_cap), max(0.05, retry_base) * (2 ** (attempt - 1)))
                time.sleep(delay)
                continue
            raise SuiteError(
                f"{method} {url} timed out after {round(effective_timeout, 3)}s (attempts={max_attempts})"
            ) from exc
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SuiteError(f"{method} {url} failed with HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            reason_text = str(exc.reason).casefold() if exc.reason is not None else ""
            if "timed out" in reason_text and attempt < max_attempts:
                delay = min(max(0.05, retry_cap), max(0.05, retry_base) * (2 ** (attempt - 1)))
                time.sleep(delay)
                continue
            raise SuiteError(f"{method} {url} failed: {exc.reason}") from exc
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SuiteError(f"{method} {url} returned invalid JSON: {raw[:240]}") from exc
    if not isinstance(parsed, dict):
        raise SuiteError(f"{method} {url} returned JSON that was not an object")
    auth_payload = parsed.get("auth")
    if isinstance(auth_payload, dict):
        auth_player = auth_payload.get("player_id")
        auth_token = auth_payload.get("access_token")
        if isinstance(auth_player, str) and auth_player.strip() and isinstance(auth_token, str) and auth_token.strip():
            PLAYER_TOKENS[auth_player.strip()] = auth_token.strip()
    return parsed


def wait_for_server_health(
    *,
    process: subprocess.Popen[str],
    base_url: str,
    startup_timeout: float,
    request_timeout: float,
) -> None:
    deadline = time.monotonic() + startup_timeout
    last_error: str | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = ""
            if process.stdout is not None:
                output = process.stdout.read()[-4000:]
            raise SuiteError(
                f"Managed server exited early with code {process.returncode}. Output:\n{output}"
            )
        try:
            health = request_json(
                base_url=base_url,
                method="GET",
                path="/health",
                timeout=request_timeout,
            )
            if health.get("status") == "ok":
                return
            last_error = f"Unexpected /health payload: {health}"
        except SuiteError as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise SuiteError(
        f"Timed out waiting for server health after {startup_timeout:.1f}s. Last error: {last_error}"
    )


def ensure_profile(base_url: str, timeout: float, player_id: str, captain_name: str) -> dict[str, Any]:
    if player_id == "admin":
        login = request_json(
            base_url=base_url,
            method="POST",
            path="/api/admin/login",
            timeout=timeout,
            payload={"username": "admin", "password": "admin"},
        )
        if login.get("player_id") != "admin":
            raise SuiteError("admin login did not return admin player_id")
        token = PLAYER_TOKENS.get("admin")
        if not isinstance(token, str) or not token.strip():
            raise SuiteError("admin login did not return auth token")
        return {
            "player_id": "admin",
            "captain_name": "Administrator",
            "auth_mode": "guest",
            "god_mode": bool(login.get("god_mode", False)),
        }

    payload = request_json(
        base_url=base_url,
        method="POST",
        path="/api/profile/save",
        timeout=timeout,
        payload={
            "player_id": player_id,
            "captain_name": captain_name,
            "auth_mode": "guest",
            "email": "",
        },
    )
    profile = payload.get("profile")
    if not isinstance(profile, dict):
        raise SuiteError("profile/save did not return profile object")
    token = PLAYER_TOKENS.get(player_id)
    if not isinstance(token, str) or not token.strip():
        raise SuiteError(f"profile/save did not return auth token for '{player_id}'")
    return profile


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    idx = (len(ordered) - 1) * max(0.0, min(1.0, fraction))
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] + ((ordered[hi] - ordered[lo]) * (idx - lo)))


def summarize_numeric(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0.0,
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "stdev": 0.0,
            "p10": 0.0,
            "p50": 0.0,
            "p90": 0.0,
        }
    return {
        "count": float(len(values)),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
        "mean": round(statistics.fmean(values), 6),
        "stdev": round(statistics.pstdev(values), 6) if len(values) > 1 else 0.0,
        "p10": round(percentile(values, 0.10), 6),
        "p50": round(percentile(values, 0.50), 6),
        "p90": round(percentile(values, 0.90), 6),
    }


def run_smoke_suite(base_url: str, timeout: float) -> dict[str, Any]:
    command = [
        sys.executable,
        str(SMOKE_SCRIPT),
        "--base-url",
        base_url,
        "--request-timeout",
        str(timeout),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    output = (completed.stdout or "") + (completed.stderr or "")
    pass_lines = [line for line in output.splitlines() if line.startswith("[PASS]")]
    return {
        "ok": completed.returncode == 0,
        "return_code": completed.returncode,
        "pass_count": len(pass_lines),
        "output_tail": "\n".join(output.splitlines()[-40:]),
    }


def run_discovery_world_ops(
    *,
    base_url: str,
    timeout: float,
    player_id: str,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    body_classes = ["planet", "moon", "asteroid", "comet", "gas_giant", "star"]
    scan_power_by_class = {
        "planet": 145.0,
        "moon": 130.0,
        "asteroid": 120.0,
        "comet": 138.0,
        "gas_giant": 155.0,
        "star": 170.0,
    }
    scans_per_class = int(config.get("discovery_scans_per_class", 8))
    count_per_scan = int(config.get("discovery_count_per_scan", 6))
    all_worlds: list[dict[str, Any]] = []
    by_class_counts: dict[str, int] = {}
    for class_idx, body_class in enumerate(body_classes):
        class_worlds: list[dict[str, Any]] = []
        for i in range(scans_per_class):
            payload = request_json(
                base_url=base_url,
                method="GET",
                path="/api/discovery/scan",
                timeout=timeout,
                query={
                    "player_id": player_id,
                    "body_class": body_class,
                    "count": count_per_scan,
                    "scan_power": scan_power_by_class[body_class],
                    "seed": seed + (class_idx * 1000) + i,
                },
            )
            items = payload.get("items")
            if isinstance(items, list):
                class_worlds.extend([row for row in items if isinstance(row, dict)])
        by_class_counts[body_class] = len(class_worlds)
        all_worlds.extend(class_worlds)

    if not all_worlds:
        raise SuiteError("discovery simulation returned zero worlds")

    habitability_values = [
        float(row.get("habitability_score"))
        for row in all_worlds
        if isinstance(row.get("habitability_score"), (int, float)) and not isinstance(row.get("habitability_score"), bool)
    ]
    hazard_values = [
        float(row.get("environment_hazard"))
        for row in all_worlds
        if isinstance(row.get("environment_hazard"), (int, float)) and not isinstance(row.get("environment_hazard"), bool)
    ]
    richness_values = [
        float(row.get("richness_multiplier"))
        for row in all_worlds
        if isinstance(row.get("richness_multiplier"), (int, float)) and not isinstance(row.get("richness_multiplier"), bool)
    ]
    difficulty_values = [
        float(row.get("scan_difficulty"))
        for row in all_worlds
        if isinstance(row.get("scan_difficulty"), (int, float)) and not isinstance(row.get("scan_difficulty"), bool)
    ]

    all_elements_counter: Counter[str] = Counter()
    for world in all_worlds:
        lodes = world.get("element_lodes")
        if not isinstance(lodes, list):
            continue
        for lode in lodes:
            if isinstance(lode, dict) and isinstance(lode.get("symbol"), str):
                all_elements_counter[lode["symbol"]] += 1

    preferred = [
        row
        for row in all_worlds
        if str(row.get("body_class", "")).strip() in {"planet", "moon"}
    ]
    if not preferred:
        preferred = list(all_worlds)

    preferred.sort(
        key=lambda row: (
            float(row.get("habitability_score", 0.0)),
            float(row.get("richness_multiplier", 0.0)),
            -float(row.get("environment_hazard", 0.0)),
        ),
        reverse=True,
    )
    chosen_world = preferred[0]
    chosen_world_id = chosen_world.get("world_id")
    if not isinstance(chosen_world_id, str) or not chosen_world_id.strip():
        raise SuiteError("discovery candidate did not include world_id")

    claimed = request_json(
        base_url=base_url,
        method="POST",
        path="/api/worlds/claim",
        timeout=timeout,
        payload={"player_id": player_id, "world_id": chosen_world_id.strip()},
    ).get("world")
    if not isinstance(claimed, dict):
        raise SuiteError("world claim did not return world object")
    world_id = claimed.get("world_id")
    if not isinstance(world_id, str):
        raise SuiteError("claimed world did not include world_id")

    structure_rows = request_json(
        base_url=base_url,
        method="GET",
        path="/api/structures",
        timeout=timeout,
        query={"domain": "planet"},
    ).get("items")
    if not isinstance(structure_rows, list):
        raise SuiteError("structures endpoint did not return items list")

    def structure_score(row: dict[str, Any]) -> float:
        modifiers = row.get("modifiers", {})
        if not isinstance(modifiers, dict):
            return 0.0
        score = 0.0
        for key, raw in modifiers.items():
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue
            value = float(raw)
            key_fold = key.casefold()
            if "population_capacity" in key_fold:
                score += value * 2.2
            elif "population_growth" in key_fold:
                score += value * 22.0
            elif "mining_yield" in key_fold:
                score += value * 7.0
            elif "research_compute" in key_fold or "compute" in key_fold:
                score += value * 3.5
            elif "market_efficiency" in key_fold:
                score += value * 3.0
            elif "rare_yield" in key_fold:
                score += value * 6.5
            else:
                score += value * 0.35
        return score

    candidate_structures = [
        row for row in structure_rows if isinstance(row, dict) and isinstance(row.get("id"), str)
    ]
    candidate_structures.sort(key=structure_score, reverse=True)

    built: list[dict[str, Any]] = []
    failed_builds: list[dict[str, Any]] = []
    for row in candidate_structures[:12]:
        if len(built) >= 5:
            break
        structure_id = str(row["id"])
        try:
            result = request_json(
                base_url=base_url,
                method="POST",
                path="/api/worlds/build-structure",
                timeout=timeout,
                payload={
                    "player_id": player_id,
                    "world_id": world_id,
                    "structure_id": structure_id,
                },
            )
            built.append(
                {
                    "id": structure_id,
                    "name": row.get("name"),
                    "category": row.get("category"),
                    "projection_summary": result.get("projection", {}).get("summary", {}),
                }
            )
        except SuiteError as exc:
            failed_builds.append({"id": structure_id, "error": str(exc)})

    world_detail = request_json(
        base_url=base_url,
        method="GET",
        path="/api/worlds/detail",
        timeout=timeout,
        query={"player_id": player_id, "world_id": world_id},
    )
    projection = world_detail.get("projection", {})

    population_projection: dict[str, Any] = {}
    for days in (7, 30, 90, 365):
        response = request_json(
            base_url=base_url,
            method="GET",
            path="/api/worlds/population-projection",
            timeout=timeout,
            query={"player_id": player_id, "world_id": world_id, "days": days},
        )
        population_projection[str(days)] = response.get("projection", {})

    harvest_runs: list[dict[str, Any]] = []
    for hours in (1.0, 8.0, 24.0):
        harvest = request_json(
            base_url=base_url,
            method="POST",
            path="/api/worlds/harvest",
            timeout=timeout,
            payload={"player_id": player_id, "world_id": world_id, "hours": hours},
        )
        harvested = harvest.get("harvested", [])
        top_symbol = None
        top_units = 0.0
        if isinstance(harvested, list):
            for row in harvested:
                if not isinstance(row, dict):
                    continue
                units = float(row.get("units", 0.0))
                if units > top_units and isinstance(row.get("symbol"), str):
                    top_units = units
                    top_symbol = row.get("symbol")
        harvest_runs.append(
            {
                "hours": hours,
                "top_symbol": top_symbol,
                "top_units": round(top_units, 4),
                "line_items": len(harvested) if isinstance(harvested, list) else 0,
            }
        )

    return {
        "scan_config": {
            "body_classes": body_classes,
            "scans_per_class": scans_per_class,
            "count_per_scan": count_per_scan,
            "scan_power_by_class": scan_power_by_class,
        },
        "world_totals": {
            "total_worlds_scanned": len(all_worlds),
            "worlds_by_class": by_class_counts,
            "habitability": summarize_numeric(habitability_values),
            "hazard": summarize_numeric(hazard_values),
            "richness": summarize_numeric(richness_values),
            "scan_difficulty": summarize_numeric(difficulty_values),
            "top_elements_seen": all_elements_counter.most_common(12),
        },
        "claimed_world": {
            "world_id": world_id,
            "name": claimed.get("name"),
            "body_class": claimed.get("body_class"),
            "subtype": claimed.get("subtype"),
            "habitability_score": claimed.get("habitability_score"),
            "environment_hazard": claimed.get("environment_hazard"),
            "rarity_score": claimed.get("rarity_score"),
        },
        "structures": {
            "built_count": len(built),
            "built": built,
            "failed_attempts": failed_builds[:8],
            "projection_summary": projection.get("summary", {}),
        },
        "population_projection": population_projection,
        "harvest_runs": harvest_runs,
    }


def run_celestial_resource_economy(
    *,
    base_url: str,
    timeout: float,
    player_id: str,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Simulate class-specific extraction loops and downstream economy/crafting/research impact."""
    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=player_id,
        captain_name="Sim Extractor",
    )
    rng = random.Random(seed)

    scan_cycles = max(1, int(config.get("celestial_scans_per_class", 4)))
    scan_count = max(1, int(config.get("celestial_scan_count", 5)))
    body_classes = ["planet", "moon", "asteroid", "comet", "gas_giant", "star"]
    scan_power_by_class = {
        "planet": 155.0,
        "moon": 142.0,
        "asteroid": 132.0,
        "comet": 138.0,
        "gas_giant": 168.0,
        "star": 178.0,
    }
    harvest_hours_by_mode = {
        "ship_mining": 8.0,
        "structure_mining": 24.0,
        "atmospheric_harvest": 12.0,
        "stellar_hydrogen_harvest": 6.0,
    }
    inventory_limit = min(118, max(1, int(config.get("celestial_inventory_limit", 60))))
    fetch_inventory_after = bool(config.get("celestial_fetch_inventory_after", False))
    slow_timeout = max(timeout, 60.0)
    default_quote_timeout = max(
        slow_timeout,
        float(config.get("celestial_quote_timeout", slow_timeout)),
    )
    scan_pause_seconds = max(0.0, float(config.get("celestial_scan_pause_seconds", 0.0)))
    extraction_focus_by_class = {
        "asteroid": "solid_mineral_lodes",
        "comet": "volatile_ice_and_metal_lodes",
        "moon": "mixed_surface_and_subsurface_lodes",
        "planet": "deep_crust_structure_mining",
        "gas_giant": "atmospheric_gas_harvesting",
        "star": "hydrogen_plasma_energy_harvesting",
    }
    depletable_classes = {"asteroid", "comet"}

    def structure_score(row: dict[str, Any]) -> float:
        modifiers = row.get("modifiers", {})
        if not isinstance(modifiers, dict):
            return 0.0
        score = 0.0
        for key, raw in modifiers.items():
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                continue
            value = float(raw)
            key_fold = str(key).casefold()
            if "mining_yield" in key_fold:
                score += value * 8.0
            elif "rare_find" in key_fold:
                score += value * 6.2
            elif "research" in key_fold or "compute" in key_fold:
                score += value * 3.4
            elif "population_capacity" in key_fold:
                score += value * 2.0
            elif "population_growth" in key_fold:
                score += value * 22.0
            elif "scan" in key_fold:
                score += value * 2.4
            else:
                score += value * 0.3
        return score

    def safe_float(value: Any, default: float = 0.0) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return default
        return float(value)

    def choose_best_world(rows: list[dict[str, Any]]) -> dict[str, Any]:
        ranked = sorted(
            rows,
            key=lambda row: (
                safe_float(row.get("richness_multiplier"), 0.0)
                * max(1.0, safe_float(row.get("estimated_total_units"), 0.0))
                * (1.0 + (safe_float(row.get("rarity_score"), 0.0) * 0.35))
                * (1.0 + (safe_float(row.get("detection_confidence"), 0.0) * 0.12))
                * (1.0 + (safe_float(row.get("habitability_score"), 0.0) * 0.08))
            ),
            reverse=True,
        )
        return ranked[0]

    def quote_item(
        item_id: str,
        world_id: str | None = None,
        quote_player_id: str | None = None,
        quote_timeout: float | None = None,
        allow_failure: bool = False,
    ) -> dict[str, Any]:
        player_for_quote = quote_player_id if isinstance(quote_player_id, str) and quote_player_id.strip() else player_id
        payload: dict[str, Any] = {
            "player_id": player_for_quote,
            "item_id": item_id,
            "quantity": 1,
        }
        if isinstance(world_id, str) and world_id.strip():
            payload["world_id"] = world_id.strip()
        try:
            return request_json(
                base_url=base_url,
                method="POST",
                path="/api/crafting/quote",
                timeout=quote_timeout
                if isinstance(quote_timeout, (int, float))
                else default_quote_timeout,
                payload=payload,
            )
        except SuiteError as exc:
            if not allow_failure:
                raise
            return {
                "item_id": item_id,
                "can_craft": False,
                "can_afford_credits": False,
                "cost": {"credits": 0.0},
                "missing_elements": [],
                "error": str(exc),
            }

    def quote_summary(quote: dict[str, Any]) -> dict[str, Any]:
        missing_rows = quote.get("missing_elements", [])
        missing_total = 0.0
        if isinstance(missing_rows, list):
            for row in missing_rows:
                if not isinstance(row, dict):
                    continue
                missing_total += safe_float(row.get("shortfall"), 0.0)
        return {
            "item_id": quote.get("item_id"),
            "can_craft": bool(quote.get("can_craft", False)),
            "can_afford_credits": bool(quote.get("can_afford_credits", False)),
            "credits_cost": safe_float(quote.get("cost", {}).get("credits"), 0.0)
            if isinstance(quote.get("cost"), dict)
            else 0.0,
            "missing_shortfall_total": round(missing_total, 4),
            "missing_elements_count": len(missing_rows) if isinstance(missing_rows, list) else 0,
        }

    wallet_before_extract: dict[str, Any] = {}
    wallet_before_extract_fetch_error: str | None = None
    try:
        wallet_before_extract = request_json(
            base_url=base_url,
            method="GET",
            path="/api/economy/wallet",
            timeout=timeout,
            query={"player_id": player_id},
        )
    except SuiteError as exc:
        wallet_before_extract_fetch_error = str(exc)
        wallet_before_extract = {
            "player_id": player_id,
            "credits": 0.0,
            "voidcoin": 0.0,
            "fetch_error": wallet_before_extract_fetch_error,
        }
    inventory_before_extract: dict[str, Any] = {"items": []}
    inventory_before_fetch_error: str | None = None
    try:
        inventory_before_extract = request_json(
            base_url=base_url,
            method="GET",
            path="/api/economy/inventory",
            timeout=slow_timeout,
            query={"player_id": player_id, "limit": inventory_limit},
        )
    except SuiteError as exc:
        inventory_before_fetch_error = str(exc)
    pre_quotes = {
        "module.weapon_laser_bank_mk1": quote_item(
            "module.weapon_laser_bank_mk1",
            allow_failure=True,
        ),
        "module.special_command_ai_mk5": quote_item(
            "module.special_command_ai_mk5",
            allow_failure=True,
        ),
    }

    extracted_totals: Counter[str] = Counter()
    class_rows: list[dict[str, Any]] = []
    world_id_by_class: dict[str, str] = {}

    for class_idx, body_class in enumerate(body_classes):
        discovered: list[dict[str, Any]] = []
        for cycle in range(scan_cycles):
            scan_seed = seed + (class_idx * 1000) + cycle
            payload = request_json(
                base_url=base_url,
                method="GET",
                path="/api/discovery/scan",
                timeout=timeout,
                query={
                    "player_id": player_id,
                    "body_class": body_class,
                    "count": scan_count,
                    "scan_power": scan_power_by_class[body_class],
                    "seed": scan_seed,
                },
            )
            items = payload.get("items", [])
            if isinstance(items, list):
                discovered.extend([row for row in items if isinstance(row, dict)])
            if scan_pause_seconds > 0.0:
                time.sleep(scan_pause_seconds)

        if not discovered:
            class_rows.append(
                {
                    "body_class": body_class,
                    "error": "no_discovered_worlds",
                    "scanned_world_count": 0,
                }
            )
            continue

        chosen = choose_best_world(discovered)
        world_id = chosen.get("world_id")
        if not isinstance(world_id, str) or not world_id.strip():
            class_rows.append(
                {
                    "body_class": body_class,
                    "error": "chosen_world_missing_world_id",
                    "scanned_world_count": len(discovered),
                }
            )
            continue
        world_id = world_id.strip()
        world_id_by_class[body_class] = world_id

        claimed = request_json(
            base_url=base_url,
            method="POST",
            path="/api/worlds/claim",
            timeout=timeout,
            payload={"player_id": player_id, "world_id": world_id},
        ).get("world")
        if not isinstance(claimed, dict):
            raise SuiteError(f"world claim failed for body_class '{body_class}'")

        moon_population_m = safe_float(claimed.get("population_potential_millions"), 0.0)
        moon_habitability = safe_float(claimed.get("habitability_score"), 0.0)
        if body_class in {"asteroid", "comet"}:
            extraction_mode = "ship_mining"
        elif body_class == "planet":
            extraction_mode = "structure_mining"
        elif body_class == "moon":
            extraction_mode = (
                "structure_mining"
                if moon_population_m >= 80.0 or moon_habitability >= 0.34
                else "ship_mining"
            )
        elif body_class == "gas_giant":
            extraction_mode = "atmospheric_harvest"
        else:
            extraction_mode = "stellar_hydrogen_harvest"

        built_structures: list[str] = []
        build_errors: list[str] = []
        if extraction_mode == "structure_mining":
            structures_payload = request_json(
                base_url=base_url,
                method="GET",
                path="/api/structures",
                timeout=timeout,
                query={"domain": body_class},
            )
            structures = structures_payload.get("items", [])
            rows = [row for row in structures if isinstance(row, dict) and isinstance(row.get("id"), str)]
            rows.sort(key=structure_score, reverse=True)
            build_limit = 3 if body_class == "planet" else 2
            for row in rows[:build_limit]:
                structure_id = str(row["id"])
                try:
                    request_json(
                        base_url=base_url,
                        method="POST",
                        path="/api/worlds/build-structure",
                        timeout=timeout,
                        payload={
                            "player_id": player_id,
                            "world_id": world_id,
                            "structure_id": structure_id,
                        },
                    )
                    built_structures.append(structure_id)
                except SuiteError as exc:
                    build_errors.append(f"{structure_id}: {exc}")

        harvest_hours = harvest_hours_by_mode[extraction_mode]
        # Add a tiny deterministic variance so each class does not use identical cycle lengths.
        varied_hours = harvest_hours * (0.94 + (rng.random() * 0.12))
        harvest_hours = round(min(24.0, max(0.25, varied_hours)), 3)
        harvest_payload = request_json(
            base_url=base_url,
            method="POST",
            path="/api/worlds/harvest",
            timeout=timeout,
            payload={
                "player_id": player_id,
                "world_id": world_id,
                "hours": harvest_hours,
            },
        )
        harvested = harvest_payload.get("harvested", [])
        total_units = 0.0
        top_symbol = None
        top_amount = 0.0
        extracted_symbols: list[dict[str, Any]] = []
        if isinstance(harvested, list):
            for row in harvested:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                amount = safe_float(row.get("amount"), 0.0)
                if not isinstance(symbol, str) or amount <= 0.0:
                    continue
                total_units += amount
                extracted_totals[symbol] += amount
                extracted_symbols.append(
                    {
                        "symbol": symbol,
                        "amount": round(amount, 3),
                        "rare_class": bool(row.get("rare_class", False)),
                    }
                )
                if amount > top_amount:
                    top_amount = amount
                    top_symbol = symbol

        class_rows.append(
            {
                "body_class": body_class,
                "extraction_mode": extraction_mode,
                "extraction_focus": extraction_focus_by_class[body_class],
                "scanned_world_count": len(discovered),
                "chosen_world": {
                    "world_id": world_id,
                    "name": claimed.get("name"),
                    "subtype": claimed.get("subtype"),
                    "richness_multiplier": claimed.get("richness_multiplier"),
                    "rarity_score": claimed.get("rarity_score"),
                    "habitability_score": claimed.get("habitability_score"),
                    "population_potential_millions": claimed.get("population_potential_millions"),
                },
                "built_structures": built_structures,
                "build_errors": build_errors[:4],
                "harvest_hours": harvest_hours,
                "harvest_summary": {
                    "line_items": len(extracted_symbols),
                    "total_units": round(total_units, 3),
                    "top_symbol": top_symbol,
                    "top_amount": round(top_amount, 3),
                    "top_symbols": extracted_symbols[:6],
                },
                "depletion_model": (
                    {
                        "is_depletable": True,
                        "model": "finite_ore_body",
                        "resource_pool_units": round(
                            safe_float(claimed.get("estimated_total_units"), 0.0), 3
                        ),
                        "harvested_this_run_units": round(total_units, 3),
                        "remaining_units_after_run": round(
                            max(
                                0.0,
                                safe_float(claimed.get("estimated_total_units"), 0.0)
                                - total_units,
                            ),
                            3,
                        ),
                        "estimated_depletion_hours": (
                            round(
                                (
                                    safe_float(claimed.get("estimated_total_units"), 0.0)
                                    / max(0.001, total_units)
                                )
                                * harvest_hours,
                                2,
                            )
                            if total_units > 0.0
                            else None
                        ),
                    }
                    if body_class in depletable_classes
                    else {
                        "is_depletable": False,
                        "model": "renewable_or_deep_cycle_reservoir",
                        "resource_pool_units": None,
                        "harvested_this_run_units": round(total_units, 3),
                        "remaining_units_after_run": None,
                        "estimated_depletion_hours": None,
                    }
                ),
            }
        )

    regions_payload = request_json(
        base_url=base_url,
        method="GET",
        path="/api/market/regions",
        timeout=timeout,
    )
    market_regions = regions_payload.get("items", [])
    market_region_id = None
    if isinstance(market_regions, list):
        for row in market_regions:
            if isinstance(row, dict) and isinstance(row.get("id"), str):
                market_region_id = str(row["id"])
                break

    wallet_before_market: dict[str, Any] = {}
    wallet_before_market_fetch_error: str | None = None
    try:
        wallet_before_market = request_json(
            base_url=base_url,
            method="GET",
            path="/api/economy/wallet",
            timeout=timeout,
            query={"player_id": player_id},
        )
    except SuiteError as exc:
        wallet_before_market_fetch_error = str(exc)
        wallet_before_market = {
            "player_id": player_id,
            "credits": 0.0,
            "voidcoin": 0.0,
            "fetch_error": wallet_before_market_fetch_error,
        }
    market_sales: list[dict[str, Any]] = []
    for symbol, total_amount in extracted_totals.most_common(12):
        quantity = max(1.0, min(180.0, float(total_amount) * 0.22))
        payload: dict[str, Any] = {
            "player_id": player_id,
            "symbol": symbol,
            "quantity": quantity,
            "currency": "credits",
        }
        if isinstance(market_region_id, str):
            payload["region_id"] = market_region_id
        try:
            sale = request_json(
                base_url=base_url,
                method="POST",
                path="/api/market/sell",
                timeout=timeout,
                payload=payload,
            )
            market_sales.append(
                {
                    "symbol": symbol,
                    "quantity": round(quantity, 3),
                    "net_total": sale.get("net_total"),
                    "unit_price": sale.get("unit_price"),
                    "region_id": market_region_id,
                }
            )
        except SuiteError as exc:
            market_sales.append(
                {
                    "symbol": symbol,
                    "quantity": round(quantity, 3),
                    "error": str(exc),
                }
            )
    wallet_after_market: dict[str, Any] = {}
    wallet_after_market_fetch_error: str | None = None
    try:
        wallet_after_market = request_json(
            base_url=base_url,
            method="GET",
            path="/api/economy/wallet",
            timeout=timeout,
            query={"player_id": player_id},
        )
    except SuiteError as exc:
        wallet_after_market_fetch_error = str(exc)
        wallet_after_market = {
            "player_id": player_id,
            "credits": safe_float(wallet_before_market.get("credits"), 0.0),
            "voidcoin": safe_float(wallet_before_market.get("voidcoin"), 0.0),
            "fetch_error": wallet_after_market_fetch_error,
        }

    planet_world_id = world_id_by_class.get("planet")
    post_quotes: dict[str, dict[str, Any]] = {
        "module.weapon_laser_bank_mk1": quote_item(
            "module.weapon_laser_bank_mk1",
            allow_failure=True,
        ),
        "module.special_command_ai_mk5": quote_item(
            "module.special_command_ai_mk5",
            allow_failure=True,
        ),
    }
    if isinstance(planet_world_id, str):
        post_quotes["structure.orbital_logistics_node"] = quote_item(
            "structure.orbital_logistics_node",
            world_id=planet_world_id,
            allow_failure=True,
        )

    market_buys_for_crafting: list[dict[str, Any]] = []
    special_quote = post_quotes.get("module.special_command_ai_mk5", {})
    missing_rows = special_quote.get("missing_elements", [])
    if isinstance(missing_rows, list):
        for row in missing_rows[:3]:
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol")
            shortfall = safe_float(row.get("shortfall"), 0.0)
            if not isinstance(symbol, str) or shortfall <= 0:
                continue
            payload = {
                "player_id": player_id,
                "symbol": symbol,
                "quantity": max(1.0, min(80.0, shortfall)),
                "currency": "credits",
            }
            if isinstance(market_region_id, str):
                payload["region_id"] = market_region_id
            try:
                buy = request_json(
                    base_url=base_url,
                    method="POST",
                    path="/api/market/buy",
                    timeout=timeout,
                    payload=payload,
                )
                market_buys_for_crafting.append(
                    {
                        "symbol": symbol,
                        "quantity": round(float(payload["quantity"]), 3),
                        "net_total": buy.get("net_total"),
                        "region_id": market_region_id,
                    }
                )
            except SuiteError as exc:
                market_buys_for_crafting.append(
                    {
                        "symbol": symbol,
                        "quantity": round(float(payload["quantity"]), 3),
                        "error": str(exc),
                    }
                )
    post_quotes["module.special_command_ai_mk5"] = quote_item(
        "module.special_command_ai_mk5",
        allow_failure=True,
    )

    build_attempts: list[dict[str, Any]] = []
    build_targets: list[tuple[str, str | None]] = [("module.weapon_laser_bank_mk1", None)]
    if isinstance(planet_world_id, str):
        build_targets.append(("structure.orbital_logistics_node", planet_world_id))
    for item_id, world_id in build_targets:
        quote = post_quotes.get(item_id) or quote_item(
            item_id,
            world_id=world_id,
            allow_failure=True,
        )
        entry: dict[str, Any] = {
            "item_id": item_id,
            "world_id": world_id,
            "can_craft": bool(quote.get("can_craft", False)),
        }
        if bool(quote.get("can_craft", False)):
            payload = {"player_id": player_id, "item_id": item_id, "quantity": 1}
            if isinstance(world_id, str):
                payload["world_id"] = world_id
            try:
                built = request_json(
                    base_url=base_url,
                    method="POST",
                    path="/api/crafting/build",
                    timeout=timeout,
                    payload=payload,
                )
                entry["build_id"] = built.get("build_id")
                entry["built"] = True
            except SuiteError as exc:
                entry["built"] = False
                entry["error"] = str(exc)
        else:
            entry["built"] = False
            entry["missing_summary"] = quote_summary(quote)
        build_attempts.append(entry)

    research_probe_player_id = str(
        config.get("celestial_research_probe_player_id", "player.sim.celestial_research")
    ).strip()
    if not research_probe_player_id or research_probe_player_id == "admin":
        research_probe_player_id = "player.sim.celestial_research"
    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=research_probe_player_id,
        captain_name="Sim Research Probe",
    )

    unlocks_before_payload = request_json(
        base_url=base_url,
        method="GET",
        path="/api/research/unlocks",
        timeout=timeout,
        query={"player_id": research_probe_player_id},
    )
    unlocked_before = set(
        row
        for row in unlocks_before_payload.get("items", [])
        if isinstance(row, str)
    )
    tech_rows = request_json(
        base_url=base_url,
        method="GET",
        path="/api/tech-tree",
        timeout=timeout,
        query={"limit": 200},
    ).get("items", [])
    candidate_techs: list[str] = []
    preferred_tokens = ("geolog", "mining", "orbital", "sensor", "stellar", "power")
    if isinstance(tech_rows, list):
        rows = [row for row in tech_rows if isinstance(row, dict)]
        rows.sort(
            key=lambda row: (
                0
                if any(token in str(row.get("id", "")).casefold() for token in preferred_tokens)
                else 1,
                safe_float(row.get("tier"), 999.0),
                safe_float(row.get("rp_cost"), 999999.0),
            )
        )

        def append_candidate_techs(rows_input: list[dict[str, Any]], *, preferred_only: bool) -> None:
            for row in rows_input:
                tech_id = row.get("id")
                prereqs = row.get("prerequisites", [])
                if not isinstance(tech_id, str) or not tech_id.strip():
                    continue
                if tech_id in unlocked_before or tech_id in candidate_techs:
                    continue
                if preferred_only and not any(token in tech_id.casefold() for token in preferred_tokens):
                    continue
                if not isinstance(prereqs, list):
                    continue
                prereq_set = {str(item) for item in prereqs if isinstance(item, str)}
                if not prereq_set.issubset(unlocked_before):
                    continue
                candidate_techs.append(tech_id)
                if len(candidate_techs) >= 3:
                    return

        append_candidate_techs(rows, preferred_only=True)
        if not candidate_techs:
            append_candidate_techs(rows, preferred_only=False)

    research_starts: list[dict[str, Any]] = []
    research_market_buys: list[dict[str, Any]] = []
    for tech_id in candidate_techs:
        try:
            quote_before = quote_item(
                tech_id,
                quote_player_id=research_probe_player_id,
                quote_timeout=slow_timeout,
            )
        except SuiteError as exc:
            research_starts.append(
                {
                    "tech_id": tech_id,
                    "started": False,
                    "error": f"quote_before_failed: {exc}",
                }
            )
            continue
        missing_before = quote_before.get("missing_elements", [])
        if isinstance(missing_before, list):
            for row in missing_before[:6]:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                shortfall = safe_float(row.get("shortfall"), 0.0)
                if not isinstance(symbol, str) or shortfall <= 0:
                    continue
                payload = {
                    "player_id": research_probe_player_id,
                    "symbol": symbol,
                    "quantity": max(1.0, min(120.0, shortfall)),
                    "currency": "credits",
                }
                if isinstance(market_region_id, str):
                    payload["region_id"] = market_region_id
                try:
                    buy = request_json(
                        base_url=base_url,
                        method="POST",
                        path="/api/market/buy",
                        timeout=timeout,
                        payload=payload,
                    )
                    research_market_buys.append(
                        {
                            "tech_id": tech_id,
                            "symbol": symbol,
                            "quantity": round(float(payload["quantity"]), 3),
                            "net_total": buy.get("net_total"),
                            "region_id": market_region_id,
                        }
                    )
                except SuiteError as exc:
                    research_market_buys.append(
                        {
                            "tech_id": tech_id,
                            "symbol": symbol,
                            "quantity": round(float(payload["quantity"]), 3),
                            "error": str(exc),
                        }
                    )

        try:
            quote_after = quote_item(
                tech_id,
                quote_player_id=research_probe_player_id,
                quote_timeout=slow_timeout,
            )
        except SuiteError as exc:
            quote_after = {
                "item_id": tech_id,
                "can_craft": False,
                "missing_elements": [],
                "error": f"quote_after_failed: {exc}",
            }
        start_entry: dict[str, Any] = {
            "tech_id": tech_id,
            "quote_before": quote_summary(quote_before),
            "quote_after": quote_summary(quote_after),
        }
        try:
            started = request_json(
                base_url=base_url,
                method="POST",
                path="/api/research/start",
                timeout=slow_timeout,
                payload={"player_id": research_probe_player_id, "tech_id": tech_id},
            )
            job = started.get("job", {})
            start_entry["started"] = True
            start_entry["job_id"] = job.get("job_id")
            start_entry["duration_seconds"] = job.get("duration_seconds")
            start_entry["remaining_seconds"] = job.get("remaining_seconds")
            start_entry["cost_credits"] = (
                started.get("quote", {}).get("cost", {}).get("credits")
                if isinstance(started.get("quote"), dict)
                else None
            )
        except SuiteError as exc:
            start_entry["started"] = False
            start_entry["error"] = str(exc)
        research_starts.append(start_entry)

    research_starts_success_total = len(
        [row for row in research_starts if isinstance(row, dict) and bool(row.get("started"))]
    )

    active_rows: list[dict[str, Any]] = []
    forecast_windows = {"1h": 0, "6h": 0, "24h": 0}
    research_jobs_fetch_error: str | None = None
    unlocks_after_fetch_error: str | None = None
    unlocked_after = set(unlocked_before)
    if research_starts_success_total > 0:
        active_jobs_payload: dict[str, Any] = {}
        try:
            active_jobs_payload = request_json(
                base_url=base_url,
                method="GET",
                path="/api/research/jobs",
                timeout=slow_timeout,
                query={"player_id": research_probe_player_id, "status": "active", "limit": 20},
            )
        except SuiteError as exc:
            research_jobs_fetch_error = str(exc)
        active_jobs = active_jobs_payload.get("items", [])
        active_rows = [row for row in active_jobs if isinstance(row, dict)] if isinstance(active_jobs, list) else []
        for row in active_rows:
            remaining = safe_float(row.get("remaining_seconds"), 0.0)
            if remaining <= 3600:
                forecast_windows["1h"] += 1
            if remaining <= 21600:
                forecast_windows["6h"] += 1
            if remaining <= 86400:
                forecast_windows["24h"] += 1

        unlocks_after_payload: dict[str, Any] = {}
        try:
            unlocks_after_payload = request_json(
                base_url=base_url,
                method="GET",
                path="/api/research/unlocks",
                timeout=slow_timeout,
                query={"player_id": research_probe_player_id},
            )
        except SuiteError as exc:
            unlocks_after_fetch_error = str(exc)
            unlocks_after_payload = {"items": list(unlocked_before)}
        unlocked_after = set(
            row
            for row in unlocks_after_payload.get("items", [])
            if isinstance(row, str)
        )

    inventory_after_all: dict[str, Any] = {"items": []}
    inventory_after_fetch_error: str | None = None
    if fetch_inventory_after:
        try:
            inventory_after_all = request_json(
                base_url=base_url,
                method="GET",
                path="/api/economy/inventory",
                timeout=slow_timeout,
                query={"player_id": player_id, "limit": inventory_limit},
            )
        except SuiteError as exc:
            inventory_after_fetch_error = str(exc)

    credits_before_market = safe_float(wallet_before_market.get("credits"), 0.0)
    credits_after_market = safe_float(wallet_after_market.get("credits"), credits_before_market)

    return {
        "config": {
            "scan_cycles_per_class": scan_cycles,
            "scan_count_per_call": scan_count,
            "scan_power_by_class": scan_power_by_class,
            "harvest_hours_by_mode": harvest_hours_by_mode,
            "inventory_limit": inventory_limit,
            "fetch_inventory_after": fetch_inventory_after,
        },
        "class_results": class_rows,
        "extracted_symbol_totals": [
            {"symbol": symbol, "amount": round(amount, 3)}
            for symbol, amount in extracted_totals.most_common(20)
        ],
        "economy_effects": {
            "wallet_before_extract": wallet_before_extract,
            "wallet_before_market": wallet_before_market,
            "wallet_after_market": wallet_after_market,
            "wallet_before_extract_fetch_error": wallet_before_extract_fetch_error,
            "wallet_before_market_fetch_error": wallet_before_market_fetch_error,
            "wallet_after_market_fetch_error": wallet_after_market_fetch_error,
            "market_sales": market_sales,
            "market_buys_for_crafting": market_buys_for_crafting,
            "credits_delta_market_phase": round(credits_after_market - credits_before_market, 6),
            "inventory_before_extract": inventory_before_extract.get("items", []),
            "inventory_after_all": inventory_after_all.get("items", []),
            "inventory_before_fetch_error": inventory_before_fetch_error,
            "inventory_after_fetch_error": inventory_after_fetch_error,
        },
        "crafting_and_building": {
            "quotes_before_extract": {
                key: quote_summary(value) for key, value in pre_quotes.items()
            },
            "quotes_after_market": {
                key: quote_summary(value) for key, value in post_quotes.items()
            },
            "build_attempts": build_attempts,
            "planet_world_id": planet_world_id,
        },
        "research_unlocking": {
            "probe_player_id": research_probe_player_id,
            "unlocked_before_total": len(unlocked_before),
            "research_start_attempts": research_starts,
            "research_start_success_total": research_starts_success_total,
            "market_buys_for_research": research_market_buys,
            "research_jobs_fetch_error": research_jobs_fetch_error,
            "unlocks_after_fetch_error": unlocks_after_fetch_error,
            "active_jobs_total": len(active_rows),
            "unlock_forecast_windows": forecast_windows,
            "unlocked_after_total": len(unlocked_after),
            "unlocked_delta_immediate": len(unlocked_after - unlocked_before),
        },
    }


def run_quality_and_robot_simulation(
    *,
    base_url: str,
    timeout: float,
    player_id: str,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    rng = random.Random(seed)
    quality_modules = {
        "module.special_command_ai_mk5": "Command AI Core Mk 5",
        "module.special_microhangar_swarm_bay_mk6": "Microhangar Swarm Bay Mk 6",
        "module.special_singularity_battle_oracle_mk8": "Singularity Battle Oracle Mk 8",
    }
    quality_results: dict[str, Any] = {}
    for module_id, label in quality_modules.items():
        all_scores: list[float] = []
        tier_counts: Counter[str] = Counter()
        samples: list[dict[str, Any]] = []
        batch_count = int(config.get("quality_batches_per_module", 8))
        qty_min = int(config.get("quality_quantity_min", 18))
        qty_max = int(config.get("quality_quantity_max", 24))
        if qty_max < qty_min:
            qty_max = qty_min
        qty_span = max(1, (qty_max - qty_min + 1))
        for run_idx in range(batch_count):
            quantity = qty_min + int(rng.random() * qty_span)
            built = request_json(
                base_url=base_url,
                method="POST",
                path="/api/crafting/build",
                timeout=timeout,
                payload={
                    "player_id": player_id,
                    "item_id": module_id,
                    "quantity": quantity,
                },
            )
            rows = built.get("quality_instances")
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    score = row.get("quality_score")
                    if isinstance(score, (int, float)) and not isinstance(score, bool):
                        all_scores.append(float(score))
                    tier_name = row.get("quality_tier")
                    if isinstance(tier_name, str):
                        tier_counts[tier_name] += 1
                if rows:
                    samples.append(rows[0])
        quality_results[module_id] = {
            "name": label,
            "sample_size": len(all_scores),
            "score_summary": summarize_numeric(all_scores),
            "tier_distribution": dict(sorted(tier_counts.items())),
            "sample_instances": samples[:6],
        }

    robot_builds = {
        "robotic_interceptor": {
            "hull_id": "hull.settler_scout",
            "hull_level": 2,
            "modules": [
                {"id": "module.weapon_laser_bank_mk1", "quantity": 1, "level": 2},
                {"id": "module.weapon_missile_battery_mk1", "quantity": 1, "level": 2},
                {"id": "module.shield_basic_barrier_mk1", "quantity": 1, "level": 2},
                {"id": "module.reactor_fission_core_mk1", "quantity": 1, "level": 2},
                {"id": "module.jammer_spectrum_mk1", "quantity": 1, "level": 2},
            ],
        },
        "assault_drone_command": {
            "hull_id": "hull.vanguard_cruiser",
            "hull_level": 5,
            "modules": [
                {"id": "module.weapon_railgun_lance_mk5", "quantity": 2, "level": 5},
                {"id": "module.armor_lattice_mk5", "quantity": 2, "level": 5},
                {"id": "module.shield_matrix_mk5", "quantity": 1, "level": 5},
                {"id": "module.reactor_core_mk5", "quantity": 1, "level": 5},
                {"id": "module.special_command_ai_mk5", "quantity": 1, "level": 5},
            ],
        },
        "autonomous_carrier": {
            "hull_id": "hull.carrier_command_t6",
            "hull_level": 6,
            "modules": [
                {"id": "module.special_microhangar_swarm_bay_mk6", "quantity": 1, "level": 6},
                {"id": "module.weapon_missile_battery_mk5", "quantity": 2, "level": 6},
                {"id": "module.scanner_multiband_aesa_tracker_mk5", "quantity": 1, "level": 6},
                {"id": "module.shield_nanoceramic_mesh_mk6", "quantity": 1, "level": 6},
                {"id": "module.reactor_aneutronic_fusion_torus_mk6", "quantity": 1, "level": 6},
            ],
        },
    }

    fit_results: dict[str, Any] = {}
    for name, payload in robot_builds.items():
        fit = request_json(
            base_url=base_url,
            method="POST",
            path="/api/fitting/simulate",
            timeout=timeout,
            payload={
                **payload,
                "runs": int(config.get("fitting_runs_robot", 100)),
                "seed": seed + (stable_hash_int(name) % 100000),
            },
        )
        fit_results[name] = {
            "can_fit": fit.get("can_fit"),
            "violations": fit.get("violations", []),
            "combat_score": fit.get("combat_score"),
            "role_projection": fit.get("role_projection", {}),
            "ship_space": fit.get("ship_space", {}),
            "simulation_summary": fit.get("simulation_summary", {}),
            "top_effects": fit.get("ship_effects", [])[:5],
        }

    return {"quality_rolls": quality_results, "robot_build_fits": fit_results}


def run_ai_battle_matrix(
    *,
    base_url: str,
    timeout: float,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    opponents = request_json(
        base_url=base_url,
        method="GET",
        path="/api/ai/opponents",
        timeout=timeout,
        query={"limit": 40},
    ).get("items")
    if not isinstance(opponents, list) or not opponents:
        raise SuiteError("ai/opponents returned no items")

    def build_fit(payload: dict[str, Any], local_seed: int) -> dict[str, Any]:
        return request_json(
            base_url=base_url,
            method="POST",
            path="/api/fitting/simulate",
            timeout=timeout,
            payload={**payload, "runs": int(config.get("fitting_runs_ai", 120)), "seed": local_seed},
        )

    ai_profiles: dict[str, dict[str, Any]] = {}
    for idx, row in enumerate(opponents):
        if not isinstance(row, dict):
            continue
        opp_id = row.get("id")
        modules = row.get("modules")
        hull_id = row.get("hull_id")
        if not isinstance(opp_id, str) or not isinstance(hull_id, str) or not isinstance(modules, list):
            continue
        max_level = 1
        for mod in modules:
            if isinstance(mod, dict) and isinstance(mod.get("level"), int):
                max_level = max(max_level, int(mod["level"]))
        fit = build_fit(
            {
                "hull_id": hull_id,
                "hull_level": max_level,
                "modules": modules,
            },
            local_seed=seed + 1000 + idx,
        )
        ai_profiles[opp_id] = {
            "id": opp_id,
            "name": row.get("name", opp_id),
            "role": row.get("role"),
            "threat_rating": row.get("threat_rating"),
            "fit": fit,
        }

    player_builds: dict[str, dict[str, Any]] = {
        "build.expedition_t2": {
            "name": "Expedition T2",
            "hull_id": "hull.settler_scout",
            "hull_level": 2,
            "modules": [
                {"id": "module.weapon_laser_bank_mk1", "quantity": 1, "level": 2},
                {"id": "module.weapon_missile_battery_mk1", "quantity": 1, "level": 2},
                {"id": "module.shield_basic_barrier_mk1", "quantity": 1, "level": 2},
                {"id": "module.reactor_fission_core_mk1", "quantity": 1, "level": 2},
                {"id": "module.jammer_spectrum_mk1", "quantity": 1, "level": 2},
            ],
        },
        "build.battleline_t5": {
            "name": "Battleline T5",
            "hull_id": "hull.vanguard_cruiser",
            "hull_level": 5,
            "modules": [
                {"id": "module.weapon_railgun_lance_mk5", "quantity": 2, "level": 5},
                {"id": "module.armor_lattice_mk5", "quantity": 2, "level": 5},
                {"id": "module.shield_matrix_mk5", "quantity": 1, "level": 5},
                {"id": "module.reactor_core_mk5", "quantity": 1, "level": 5},
                {"id": "module.special_command_ai_mk5", "quantity": 1, "level": 5},
            ],
        },
        "build.carrier_t6": {
            "name": "Carrier T6",
            "hull_id": "hull.carrier_command_t6",
            "hull_level": 6,
            "modules": [
                {"id": "module.special_microhangar_swarm_bay_mk6", "quantity": 1, "level": 6},
                {"id": "module.weapon_missile_battery_mk5", "quantity": 2, "level": 6},
                {"id": "module.scanner_multiband_aesa_tracker_mk5", "quantity": 1, "level": 6},
                {"id": "module.shield_nanoceramic_mesh_mk6", "quantity": 1, "level": 6},
                {"id": "module.reactor_aneutronic_fusion_torus_mk6", "quantity": 1, "level": 6},
            ],
        },
        "build.nullspace_t7": {
            "name": "Nullspace T7",
            "hull_id": "hull.synaptic_battleship_t7",
            "hull_level": 7,
            "modules": [
                {"id": "module.weapon_neural_rail_lance_mk7", "quantity": 2, "level": 7},
                {"id": "module.scanner_nullspace_resonator_mk7", "quantity": 1, "level": 7},
                {"id": "module.jammer_predictive_ghost_cloak_mk7", "quantity": 1, "level": 7},
                {"id": "module.shield_phase_metamaterial_weave_mk7", "quantity": 1, "level": 7},
                {"id": "module.reactor_plasma_casimir_cell_mk7", "quantity": 1, "level": 7},
                {"id": "module.special_quantum_navigation_core_mk7", "quantity": 1, "level": 7},
            ],
        },
        "build.singularity_t8": {
            "name": "Singularity T8",
            "hull_id": "hull.singularity_flagship_t8",
            "hull_level": 8,
            "modules": [
                {"id": "module.weapon_antimatter_torpedo_spine_mk7", "quantity": 2, "level": 8},
                {"id": "module.weapon_psionic_resonance_emitter_mk8", "quantity": 1, "level": 8},
                {"id": "module.armor_hea_smart_lattice_mk7", "quantity": 2, "level": 8},
                {"id": "module.shield_phase_metamaterial_weave_mk7", "quantity": 1, "level": 8},
                {"id": "module.reactor_antimatter_bottle_mk8", "quantity": 1, "level": 8},
                {"id": "module.special_singularity_battle_oracle_mk8", "quantity": 1, "level": 8},
            ],
        },
    }

    baseline_fits: dict[str, Any] = {}
    matrix: dict[str, dict[str, Any]] = defaultdict(dict)
    build_avg_wins: dict[str, float] = {}

    for idx, (build_id, payload) in enumerate(player_builds.items()):
        fit = build_fit(payload, local_seed=seed + 2100 + idx)
        baseline_fits[build_id] = {
            "name": payload["name"],
            "can_fit": fit.get("can_fit"),
            "violations": fit.get("violations", []),
            "combat_score": fit.get("combat_score"),
            "role_projection": fit.get("role_projection", {}),
            "ship_space": fit.get("ship_space", {}),
        }
        rates: list[float] = []
        for opp_idx, (opp_id, opp_payload) in enumerate(ai_profiles.items()):
            enemy_fit = opp_payload["fit"]
            matchup = build_fit(
                {
                    **payload,
                    "enemy": {
                        "name": opp_payload["name"],
                        "stats": enemy_fit.get("merged_stats", {}),
                        "profiles": enemy_fit.get("profiles", {}),
                    },
                },
                local_seed=seed + 3100 + (idx * 100) + opp_idx,
            )
            sim_summary = matchup.get("simulation_summary", {})
            odds = matchup.get("odds", {})
            win_rate = float(sim_summary.get("attacker_win_rate", 0.0))
            rates.append(win_rate)
            matrix[build_id][opp_id] = {
                "opponent_name": opp_payload["name"],
                "opponent_role": opp_payload.get("role"),
                "opponent_threat_rating": opp_payload.get("threat_rating"),
                "attacker_win_rate": round(win_rate, 4),
                "defender_win_rate": round(float(sim_summary.get("defender_win_rate", 0.0)), 4),
                "draw_rate": round(float(sim_summary.get("draw_rate", 0.0)), 4),
                "avg_rounds": round(float(sim_summary.get("avg_rounds", 0.0)), 4),
                "avg_ttk_rounds_when_win": round(float(sim_summary.get("avg_ttk_rounds_when_win", 0.0)), 4),
                "odds_attacker_win_probability": round(
                    float(odds.get("attacker_win_probability", 0.0)),
                    4,
                ),
            }
        build_avg_wins[build_id] = round(statistics.fmean(rates) if rates else 0.0, 4)

    ordered_builds = [
        "build.expedition_t2",
        "build.battleline_t5",
        "build.carrier_t6",
        "build.nullspace_t7",
        "build.singularity_t8",
    ]
    progression_steps: list[dict[str, Any]] = []
    previous = None
    for build_id in ordered_builds:
        value = build_avg_wins.get(build_id, 0.0)
        if previous is None:
            delta = 0.0
        else:
            delta = round(value - previous, 4)
        progression_steps.append(
            {
                "build_id": build_id,
                "name": player_builds[build_id]["name"],
                "average_win_rate_vs_ai_pool": value,
                "delta_from_previous": delta,
            }
        )
        previous = value

    return {
        "ai_opponents_evaluated": len(ai_profiles),
        "player_builds_evaluated": len(player_builds),
        "player_baseline_fits": baseline_fits,
        "matrix": matrix,
        "build_average_win_rates": build_avg_wins,
        "progression_trend": progression_steps,
    }


def run_market_simulation(
    *,
    base_url: str,
    timeout: float,
    player_id: str,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    rng = random.Random(seed)
    ensure_profile(base_url=base_url, timeout=timeout, player_id=player_id, captain_name="Sim Trader")
    regions_payload = request_json(
        base_url=base_url,
        method="GET",
        path="/api/market/regions",
        timeout=timeout,
    )
    region_rows = regions_payload.get("items")
    if not isinstance(region_rows, list) or not region_rows:
        raise SuiteError("market regions endpoint returned no rows")

    region_ids = [
        str(row["id"])
        for row in region_rows
        if isinstance(row, dict) and isinstance(row.get("id"), str)
    ][:5]
    if not region_ids:
        raise SuiteError("Unable to resolve region ids")

    symbols = ["H", "He", "C", "O", "Si", "Fe", "Ni", "Au", "Pt", "U"]
    price_book: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for region_id in region_ids:
        snapshot = request_json(
            base_url=base_url,
            method="GET",
            path="/api/market/snapshot",
            timeout=timeout,
            query={"player_id": player_id, "limit": 118, "region_id": region_id},
        )
        items = snapshot.get("items")
        if not isinstance(items, list):
            continue
        for row in items:
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol")
            if not isinstance(symbol, str) or symbol not in symbols:
                continue
            ask = row.get("ask_credits")
            bid = row.get("bid_credits")
            if isinstance(ask, (int, float)) and isinstance(bid, (int, float)):
                price_book[symbol][region_id] = {
                    "ask_credits": float(ask),
                    "bid_credits": float(bid),
                }

    arbitrage: list[dict[str, Any]] = []
    for symbol in symbols:
        region_prices = price_book.get(symbol, {})
        if len(region_prices) < 2:
            continue
        min_region = min(region_prices.items(), key=lambda row: row[1]["ask_credits"])
        max_region = max(region_prices.items(), key=lambda row: row[1]["bid_credits"])
        spread = max_region[1]["bid_credits"] - min_region[1]["ask_credits"]
        spread_pct = (spread / max(0.0001, min_region[1]["ask_credits"])) * 100.0
        arbitrage.append(
            {
                "symbol": symbol,
                "buy_region": min_region[0],
                "buy_ask_credits": round(min_region[1]["ask_credits"], 6),
                "sell_region": max_region[0],
                "sell_bid_credits": round(max_region[1]["bid_credits"], 6),
                "spread_credits": round(spread, 6),
                "spread_pct": round(spread_pct, 4),
            }
        )
    arbitrage.sort(key=lambda row: row["spread_pct"], reverse=True)

    wallet_before_fetch_error: str | None = None
    wallet_before_payload: dict[str, Any] = {}
    try:
        wallet_before_payload = request_json(
            base_url=base_url,
            method="GET",
            path="/api/economy/wallet",
            timeout=timeout,
            query={"player_id": player_id},
        )
    except SuiteError as exc:
        wallet_before_fetch_error = str(exc)
        wallet_before_payload = {
            "player_id": player_id,
            "credits": 0.0,
            "voidcoin": 0.0,
            "fetch_error": wallet_before_fetch_error,
        }
    wallet_before = wallet_before_payload if isinstance(wallet_before_payload, dict) else {}

    executed_trades: list[dict[str, Any]] = []
    credits_start = float(wallet_before.get("credits", 0.0)) if isinstance(wallet_before, dict) else 0.0

    trade_cycles = max(1, int(config.get("market_trade_cycles", 1)))
    trade_cycle_pause_seconds = max(0.0, float(config.get("market_trade_cycle_pause_seconds", 0.0)))
    top_routes = arbitrage[:6]
    if not top_routes:
        for symbol in symbols:
            region_prices = price_book.get(symbol, {})
            if not region_prices:
                continue
            region_id, pricing = next(iter(region_prices.items()))
            ask_fallback = float(pricing.get("ask_credits", 0.0))
            bid_fallback = float(pricing.get("bid_credits", 0.0))
            if ask_fallback <= 0 or bid_fallback <= 0:
                continue
            top_routes.append(
                {
                    "symbol": symbol,
                    "buy_region": region_id,
                    "buy_ask_credits": round(ask_fallback, 6),
                    "sell_region": region_id,
                    "sell_bid_credits": round(bid_fallback, 6),
                    "spread_credits": round(bid_fallback - ask_fallback, 6),
                    "spread_pct": round(
                        ((bid_fallback - ask_fallback) / max(0.0001, ask_fallback)) * 100.0,
                        4,
                    ),
                }
            )
            break
    for cycle in range(trade_cycles):
        if not top_routes:
            break
        pick = top_routes[cycle % len(top_routes)]
        symbol = pick["symbol"]
        ask = float(pick["buy_ask_credits"])
        bid = float(pick["sell_bid_credits"])
        if ask <= 0 or bid <= 0:
            continue
        cycle_weight = 0.009 + (0.002 * (cycle % 3))
        quantity = max(6.0, min(120.0, credits_start * cycle_weight / ask))
        quantity *= (0.92 + (rng.random() * 0.16))
        try:
            buy_result = request_json(
                base_url=base_url,
                method="POST",
                path="/api/market/buy",
                timeout=timeout,
                payload={
                    "player_id": player_id,
                    "symbol": symbol,
                    "quantity": quantity,
                    "currency": "credits",
                    "region_id": pick["buy_region"],
                },
            )
        except SuiteError as exc:
            executed_trades.append(
                {
                    "cycle": cycle + 1,
                    "symbol": symbol,
                    "quantity": round(quantity, 4),
                    "buy_region": pick["buy_region"],
                    "sell_region": pick["sell_region"],
                    "error": f"buy_failed: {exc}",
                }
            )
            if trade_cycle_pause_seconds > 0.0:
                time.sleep(trade_cycle_pause_seconds)
            continue

        try:
            sell_result = request_json(
                base_url=base_url,
                method="POST",
                path="/api/market/sell",
                timeout=timeout,
                payload={
                    "player_id": player_id,
                    "symbol": symbol,
                    "quantity": quantity,
                    "currency": "credits",
                    "region_id": pick["sell_region"],
                },
            )
            executed_trades.append(
                {
                    "cycle": cycle + 1,
                    "symbol": symbol,
                    "quantity": round(quantity, 4),
                    "buy_region": pick["buy_region"],
                    "sell_region": pick["sell_region"],
                    "buy_net_total": buy_result.get("net_total"),
                    "sell_net_total": sell_result.get("net_total"),
                    "wallet_after_sell": sell_result.get("wallet", {}),
                }
            )
        except SuiteError as exc:
            executed_trades.append(
                {
                    "cycle": cycle + 1,
                    "symbol": symbol,
                    "quantity": round(quantity, 4),
                    "buy_region": pick["buy_region"],
                    "sell_region": pick["sell_region"],
                    "buy_net_total": buy_result.get("net_total"),
                    "error": f"sell_failed: {exc}",
                }
            )
        if trade_cycle_pause_seconds > 0.0:
            time.sleep(trade_cycle_pause_seconds)

    wallet_after_fetch_error: str | None = None
    wallet_after_payload: dict[str, Any] = {}
    try:
        wallet_after_payload = request_json(
            base_url=base_url,
            method="GET",
            path="/api/economy/wallet",
            timeout=timeout,
            query={"player_id": player_id},
        )
    except SuiteError as exc:
        wallet_after_fetch_error = str(exc)
        wallet_after_payload = {
            "player_id": player_id,
            "credits": safe_float(wallet_before.get("credits"), 0.0),
            "voidcoin": safe_float(wallet_before.get("voidcoin"), 0.0),
            "fetch_error": wallet_after_fetch_error,
        }
    wallet_after = wallet_after_payload if isinstance(wallet_after_payload, dict) else {}
    credits_end = float(wallet_after.get("credits", 0.0)) if isinstance(wallet_after, dict) else credits_start

    return {
        "regions_sampled": region_ids,
        "symbols_sampled": symbols,
        "trade_cycles": trade_cycles,
        "top_arbitrage_opportunities": arbitrage[:8],
        "executed_trades": executed_trades,
        "wallet_before": wallet_before,
        "wallet_after": wallet_after,
        "wallet_before_fetch_error": wallet_before_fetch_error,
        "wallet_after_fetch_error": wallet_after_fetch_error,
        "credits_delta": round(credits_end - credits_start, 6),
    }


def run_covert_ops_simulation(
    *,
    base_url: str,
    timeout: float,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    iterations = max(9, int(config.get("covert_iterations", 30)))
    rng = random.Random(seed ^ stable_hash_int("covert_ops"))
    target_player_id = f"player.sim.covert.target.{seed}"
    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=target_player_id,
        captain_name="Covert Target",
    )
    _ = request_json(
        base_url=base_url,
        method="GET",
        path="/api/covert/policy",
        timeout=timeout,
        query={"player_id": target_player_id},
    )
    op_cycle = ["steal", "sabotage", "hack"]
    runs: list[dict[str, Any]] = []
    totals = {
        "total_runs": 0,
        "success": 0,
        "failed": 0,
        "blocked": 0,
        "detected": 0,
    }
    by_op: dict[str, dict[str, Any]] = {
        "steal": {"runs": 0, "success": 0, "failed": 0, "blocked": 0, "detected": 0},
        "sabotage": {"runs": 0, "success": 0, "failed": 0, "blocked": 0, "detected": 0},
        "hack": {"runs": 0, "success": 0, "failed": 0, "blocked": 0, "detected": 0},
    }
    for idx in range(iterations):
        op_type = op_cycle[idx % len(op_cycle)]
        actor_player_id = f"player.sim.covert.actor.{seed}.{idx}"
        ensure_profile(
            base_url=base_url,
            timeout=timeout,
            player_id=actor_player_id,
            captain_name=f"Covert Actor {idx + 1}",
        )
        payload: dict[str, Any] = {
            "player_id": actor_player_id,
            "target_player_id": target_player_id,
            "seed": seed + (idx * 37) + rng.randrange(1, 400),
        }
        if op_type == "steal":
            payload["quantity"] = 1 + (idx % 3)
        result = request_json(
            base_url=base_url,
            method="POST",
            path=f"/api/covert/{op_type}",
            timeout=timeout,
            payload=payload,
        )
        status = str(result.get("status", "")).casefold()
        detected = bool(result.get("detected"))
        energy_cost = float(result.get("energy_cost", 0.0))
        totals["total_runs"] += 1
        by_op[op_type]["runs"] += 1
        if status == "success":
            totals["success"] += 1
            by_op[op_type]["success"] += 1
        elif status == "blocked":
            totals["blocked"] += 1
            by_op[op_type]["blocked"] += 1
        else:
            totals["failed"] += 1
            by_op[op_type]["failed"] += 1
        if detected:
            totals["detected"] += 1
            by_op[op_type]["detected"] += 1
        runs.append(
            {
                "index": idx + 1,
                "op_type": op_type,
                "actor_player_id": actor_player_id,
                "status": status,
                "detected": detected,
                "success_probability": result.get("probabilities", {}).get("success_probability"),
                "detection_probability": result.get("probabilities", {}).get("detection_probability"),
                "energy_cost": round(energy_cost, 3),
                "cooldown_seconds": result.get("cooldown_after", {}).get("seconds_remaining"),
            }
        )
    logs_payload = request_json(
        base_url=base_url,
        method="GET",
        path="/api/covert/logs",
        timeout=timeout,
        query={"player_id": target_player_id, "perspective": "target", "limit": 118},
    )
    logs_total = int(logs_payload.get("total", 0)) if isinstance(logs_payload.get("total"), int) else 0
    cooldown_payload = request_json(
        base_url=base_url,
        method="GET",
        path="/api/covert/cooldowns",
        timeout=timeout,
        query={"player_id": target_player_id},
    )
    return {
        "iterations": int(iterations),
        "target_player_id": target_player_id,
        "totals": totals,
        "by_op": by_op,
        "runs_sample": runs[:24],
        "target_perspective_log_total": int(logs_total),
        "target_cooldowns": cooldown_payload.get("items", []),
    }


def run_ship_space_and_engagement_checks(
    *,
    base_url: str,
    timeout: float,
    player_id: str,
    seed: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    balanced_payload = {
        "hull_id": "hull.carrier_command_t6",
        "hull_level": 6,
        "crew_assigned_total": 739.0,
        "passenger_assigned_total": 0.0,
        "cargo_load_tons": 60.0,
        "modules": [
            {"id": "module.special_microhangar_swarm_bay_mk6", "quantity": 1, "level": 6},
            {"id": "module.weapon_missile_battery_mk5", "quantity": 2, "level": 6},
            {"id": "module.scanner_multiband_aesa_tracker_mk5", "quantity": 1, "level": 6},
            {"id": "module.shield_nanoceramic_mesh_mk6", "quantity": 1, "level": 6},
            {"id": "module.reactor_aneutronic_fusion_torus_mk6", "quantity": 1, "level": 6},
            {"id": "module.utility_modular_habitat_ring_mk5", "quantity": 1, "level": 6},
        ],
        "runs": int(config.get("fitting_runs_ship_space", 120)),
        "seed": seed + 41,
    }
    overloaded_payload = {
        **balanced_payload,
        "crew_assigned_total": 2600.0,
        "passenger_assigned_total": 500.0,
        "cargo_load_tons": 5000.0,
        "modules": balanced_payload["modules"] + [
            {"id": "module.special_microhangar_swarm_bay_mk6", "quantity": 2, "level": 6},
            {"id": "module.utility_mining_refinery_pod_mk5", "quantity": 2, "level": 6},
        ],
        "seed": seed + 42,
    }

    balanced = request_json(
        base_url=base_url,
        method="POST",
        path="/api/fitting/simulate",
        timeout=timeout,
        payload=balanced_payload,
    )
    overloaded = request_json(
        base_url=base_url,
        method="POST",
        path="/api/fitting/simulate",
        timeout=timeout,
        payload=overloaded_payload,
    )

    contacts = request_json(
        base_url=base_url,
        method="GET",
        path="/api/combat/contacts",
        timeout=timeout,
        query={
            "player_id": player_id,
            "count": int(config.get("contacts_count", 14)),
            "seed": seed + 99,
        },
    ).get("items", [])
    if not isinstance(contacts, list):
        contacts = []
    contact_rows = [row for row in contacts if isinstance(row, dict)]

    high_gap = None
    low_gap = None
    for row in sorted(contact_rows, key=lambda r: float(r.get("level_gap", 0.0)), reverse=True):
        if high_gap is None and float(row.get("level_gap", 0.0)) > 0:
            high_gap = row
        if low_gap is None and float(row.get("level_gap", 0.0)) < 0:
            low_gap = row
        if high_gap is not None and low_gap is not None:
            break

    engagement_samples: list[dict[str, Any]] = []
    for row in [high_gap, low_gap]:
        if not isinstance(row, dict):
            continue
        engage = request_json(
            base_url=base_url,
            method="POST",
            path="/api/combat/engage",
            timeout=timeout,
            payload={"player_id": player_id, "contact": row, "action": "fight"},
        )
        engagement_samples.append(
            {
                "contact_id": row.get("contact_id"),
                "contact_name": row.get("name"),
                "contact_combat_level": row.get("combat_level"),
                "level_gap": row.get("level_gap"),
                "engagement_balance": engage.get("engagement_balance", {}),
                "winner": engage.get("battle", {}).get("winner"),
                "reward_scaling": (
                    engage.get("engagement_balance", {}).get("reward_scaling", {})
                    if isinstance(engage.get("engagement_balance"), dict)
                    else {}
                ),
            }
        )

    return {
        "balanced_fit": {
            "can_fit": balanced.get("can_fit"),
            "violations": balanced.get("violations", []),
            "combat_score": balanced.get("combat_score"),
            "ship_space": balanced.get("ship_space", {}),
            "thermal": balanced.get("thermal", {}),
            "simulation_summary": balanced.get("simulation_summary", {}),
        },
        "overloaded_fit": {
            "can_fit": overloaded.get("can_fit"),
            "violations": overloaded.get("violations", []),
            "combat_score": overloaded.get("combat_score"),
            "ship_space": overloaded.get("ship_space", {}),
            "thermal": overloaded.get("thermal", {}),
            "simulation_summary": overloaded.get("simulation_summary", {}),
        },
        "engagement_samples": engagement_samples,
    }


def build_markdown_report(report: dict[str, Any]) -> str:
    meta = report["meta"]
    smoke = report["smoke"]
    discovery = report["discovery_world_ops"]
    celestial = report["celestial_resource_economy"]
    robot = report["robot_and_quality"]
    ai = report["ai_battle_matrix"]
    market = report["market"]
    covert = report["covert_ops"]
    simpy = report["simpy_timeflow"]
    space = report["ship_space_and_engagement"]

    lines: list[str] = []
    lines.append("# SpaceShift Simulation Suite Report")
    lines.append("")
    lines.append(f"- Generated UTC: `{meta['generated_utc']}`")
    lines.append(f"- Seed: `{meta['seed']}`")
    lines.append(f"- Profile: `{meta['profile']}`")
    lines.append(f"- Managed base URL: `{meta['base_url']}`")
    lines.append("")
    lines.append("## 1) Smoke Suite")
    lines.append("")
    lines.append(f"- Passed: `{smoke['ok']}`")
    lines.append(f"- Pass count: `{smoke['pass_count']}`")
    lines.append(f"- Return code: `{smoke['return_code']}`")
    lines.append("")
    lines.append("## 2) Discovery + World Ops")
    lines.append("")
    lines.append(f"- Total worlds scanned: `{discovery['world_totals']['total_worlds_scanned']}`")
    lines.append(f"- Worlds by class: `{json.dumps(discovery['world_totals']['worlds_by_class'])}`")
    lines.append(
        "- Richness mean / p90: "
        f"`{discovery['world_totals']['richness']['mean']}` / "
        f"`{discovery['world_totals']['richness']['p90']}`"
    )
    lines.append(
        "- Habitability mean / p90: "
        f"`{discovery['world_totals']['habitability']['mean']}` / "
        f"`{discovery['world_totals']['habitability']['p90']}`"
    )
    claimed = discovery["claimed_world"]
    lines.append(
        f"- Claimed world: `{claimed['name']}` ({claimed['body_class']}/{claimed['subtype']}) "
        f"`habitability={claimed['habitability_score']}` `hazard={claimed['environment_hazard']}`"
    )
    lines.append(f"- Structures built: `{discovery['structures']['built_count']}`")
    lines.append("")
    lines.append("Population projection snapshots:")
    for days in ("7", "30", "90", "365"):
        row = discovery["population_projection"].get(days, {})
        pop = row.get("population", {}) if isinstance(row, dict) else {}
        start_pop = pop.get("start_current")
        end_pop = pop.get("projected_current")
        cap = pop.get("capacity")
        lines.append(f"- `{days}d`: `{start_pop} -> {end_pop}` (cap `{cap}`)")
    lines.append("")
    lines.append("## 3) Celestial Resource Economy Chain")
    lines.append("")
    lines.append(
        f"- Class loops executed: `{len(celestial['class_results'])}` "
        f"(scan cycles/class `{celestial['config']['scan_cycles_per_class']}`)"
    )
    lines.append("- Extraction results by class:")
    for row in celestial["class_results"]:
        if not isinstance(row, dict):
            continue
        body_class = row.get("body_class")
        if row.get("error"):
            lines.append(f"- `{body_class}` error: `{row.get('error')}`")
            continue
        harvest = row.get("harvest_summary", {})
        depletion = row.get("depletion_model", {})
        depletion_tag = "finite" if bool(depletion.get("is_depletable")) else "non-depleting"
        depletion_eta = (
            f" eta_h={depletion.get('estimated_depletion_hours')}"
            if bool(depletion.get("is_depletable"))
            else ""
        )
        lines.append(
            f"- `{body_class}` mode `{row.get('extraction_mode')}` "
            f"world `{row.get('chosen_world', {}).get('name')}` "
            f"units `{harvest.get('total_units')}` top `{harvest.get('top_symbol')}` "
            f"depletion `{depletion_tag}`{depletion_eta}"
        )
    lines.append("")
    economy = celestial["economy_effects"]
    lines.append("- Economy/crafting/research downstream effects:")
    lines.append(
        f"- Market phase credits delta: `{economy['credits_delta_market_phase']}` "
        f"(sales `{len(economy['market_sales'])}`, crafting buys `{len(economy['market_buys_for_crafting'])}`)"
    )
    if economy.get("inventory_before_fetch_error"):
        lines.append(f"- Inventory-before fetch warning: `{economy.get('inventory_before_fetch_error')}`")
    if economy.get("inventory_after_fetch_error"):
        lines.append(f"- Inventory-after fetch warning: `{economy.get('inventory_after_fetch_error')}`")
    crafting = celestial["crafting_and_building"]
    post_quote = crafting["quotes_after_market"].get("module.special_command_ai_mk5", {})
    lines.append(
        f"- Command AI Mk5 post-market: can_craft=`{post_quote.get('can_craft')}` "
        f"missing_total=`{post_quote.get('missing_shortfall_total')}`"
    )
    lines.append(
        f"- Build attempts executed: `{len(crafting['build_attempts'])}`; "
        f"planet context world `{crafting.get('planet_world_id')}`"
    )
    research = celestial["research_unlocking"]
    lines.append(
        f"- Research starts attempted: `{len(research['research_start_attempts'])}`; "
        f"started successfully `{research.get('research_start_success_total')}`; "
        f"probe player `{research.get('probe_player_id')}`; "
        f"active jobs `{research['active_jobs_total']}`; "
        f"unlock forecast `{json.dumps(research['unlock_forecast_windows'])}`"
    )
    lines.append(
        f"- Research market buys executed: `{len(research.get('market_buys_for_research', []))}`"
    )
    if research.get("research_jobs_fetch_error"):
        lines.append(f"- Research jobs fetch warning: `{research.get('research_jobs_fetch_error')}`")
    if research.get("unlocks_after_fetch_error"):
        lines.append(f"- Research unlocks-after fetch warning: `{research.get('unlocks_after_fetch_error')}`")
    lines.append("")
    lines.append("## 4) Robot Builds + Gaussian Quality Rolls")
    lines.append("")
    for module_id, module_row in robot["quality_rolls"].items():
        summary = module_row["score_summary"]
        lines.append(
            f"- `{module_id}` sample `{module_row['sample_size']}` "
            f"mean `{summary['mean']}` stdev `{summary['stdev']}` "
            f"p10/p50/p90 `{summary['p10']}`/`{summary['p50']}`/`{summary['p90']}`"
        )
    lines.append("")
    lines.append("Robot-fit outcomes:")
    for fit_name, fit_row in robot["robot_build_fits"].items():
        role = fit_row.get("role_projection", {}).get("primary_role")
        lines.append(
            f"- `{fit_name}` can_fit=`{fit_row.get('can_fit')}` "
            f"role=`{role}` combat_score=`{fit_row.get('combat_score')}`"
        )
    lines.append("")
    lines.append("## 5) AI Battle Matrix")
    lines.append("")
    lines.append(
        f"- AI opponents evaluated: `{ai['ai_opponents_evaluated']}`; "
        f"player builds evaluated: `{ai['player_builds_evaluated']}`"
    )
    lines.append("- Average attacker win rate by build vs AI pool:")
    for row in ai["progression_trend"]:
        lines.append(
            f"- `{row['name']}`: `{row['average_win_rate_vs_ai_pool']}` "
            f"(delta `{row['delta_from_previous']}`)"
        )
    lines.append("")
    lines.append("## 6) Market Simulation")
    lines.append("")
    lines.append(f"- Regions sampled: `{', '.join(market['regions_sampled'])}`")
    lines.append(f"- Credits delta after executed arbitrage sample trades: `{market['credits_delta']}`")
    lines.append("Top regional spreads:")
    for row in market["top_arbitrage_opportunities"][:5]:
        lines.append(
            f"- `{row['symbol']}` buy `{row['buy_region']}` @{row['buy_ask_credits']} "
            f"sell `{row['sell_region']}` @{row['sell_bid_credits']} "
            f"spread `{row['spread_pct']}%`"
        )
    lines.append("")
    lines.append("## 7) Covert Ops Runtime")
    lines.append("")
    lines.append(
        f"- Iterations: `{covert['iterations']}` target logs observed: `{covert['target_perspective_log_total']}`"
    )
    totals = covert.get("totals", {})
    lines.append(
        f"- Totals: success=`{totals.get('success')}` failed=`{totals.get('failed')}` "
        f"blocked=`{totals.get('blocked')}` detected=`{totals.get('detected')}`"
    )
    lines.append("- By operation:")
    by_op = covert.get("by_op", {})
    for op_name in ("steal", "sabotage", "hack"):
        row = by_op.get(op_name, {})
        lines.append(
            f"- `{op_name}` runs=`{row.get('runs')}` success=`{row.get('success')}` "
            f"failed=`{row.get('failed')}` blocked=`{row.get('blocked')}` detected=`{row.get('detected')}`"
        )
    lines.append("")
    lines.append("")
    lines.append("## 8) SimPy Timeflow Simulation")
    lines.append("")
    q = simpy.get("queue_dynamics", {})
    q_r = q.get("research", {})
    q_m = q.get("manufacturing", {})
    q_u = q.get("utilization", {})
    lines.append(
        f"- Queue horizon/player load: `{q.get('horizon_hours')}`h / players `{q.get('players')}`"
    )
    lines.append(
        f"- Research queue: created `{q_r.get('created_jobs')}` completed `{q_r.get('completed_jobs')}` "
        f"backlog `{q_r.get('backlog_jobs')}` wait_p95 `{q_r.get('wait_p95_h')}`h"
    )
    lines.append(
        f"- Manufacturing queue: created `{q_m.get('created_jobs')}` completed `{q_m.get('completed_jobs')}` "
        f"backlog `{q_m.get('backlog_jobs')}` wait_p95 `{q_m.get('wait_p95_h')}`h"
    )
    lines.append(
        f"- Resource utilization: compute `{q_u.get('compute')}` fab `{q_u.get('manufacturing')}`"
    )
    mkt = simpy.get("market_dynamics", {})
    lines.append(
        f"- SimPy market: final `{mkt.get('final_price')}` avg `{mkt.get('avg_price')}` "
        f"vol `{mkt.get('annualized_like_volatility_day')}` shortages `{mkt.get('trades', {}).get('shortage_events')}`"
    )
    ext = simpy.get("extraction_logistics", {})
    ext_totals = ext.get("totals", {})
    lines.append(
        f"- Extraction logistics totals: delivered `{ext_totals.get('delivered_units')}` "
        f"credits `{ext_totals.get('credits_generated')}` "
        f"craft_eq `{ext_totals.get('craftable_module_batches_eq')}`"
    )
    cross = simpy.get("cross_effects", {})
    lines.append(
        f"- Cross-effects: supply_pressure `{cross.get('supply_pressure_index')}` "
        f"queue_stress `{cross.get('queue_stress_index_jobs_per_player')}` "
        f"price_ratio `{cross.get('market_price_vs_anchor_ratio')}`"
    )
    lines.append("")
    lines.append("## 9) Ship Space + Engagement Fairness")
    lines.append("")
    lines.append(
        f"- Balanced fit can_fit=`{space['balanced_fit']['can_fit']}` "
        f"equipment_util=`{space['balanced_fit']['ship_space'].get('equipment_utilization_ratio')}` "
        f"habitable_util=`{space['balanced_fit']['ship_space'].get('habitable_utilization_ratio')}`"
    )
    lines.append(
        f"- Overloaded fit can_fit=`{space['overloaded_fit']['can_fit']}` "
        f"violations=`{len(space['overloaded_fit']['violations'])}`"
    )
    lines.append("- Engagement scaling samples (fight results):")
    for row in space["engagement_samples"]:
        lines.append(
            f"- `{row['contact_name']}` level_gap=`{row['level_gap']}` winner=`{row['winner']}` "
            f"reward_scaling=`{json.dumps(row.get('reward_scaling', {}))}`"
        )
    lines.append("")
    lines.append("## 10) Artifact Paths")
    lines.append("")
    lines.append(f"- JSON: `{meta['json_path']}`")
    lines.append(f"- Markdown: `{meta['md_path']}`")
    return "\n".join(lines) + "\n"


def run_suite(args: argparse.Namespace) -> dict[str, Any]:
    if args.port != 0 and not (1 <= args.port <= 65535):
        raise SuiteError("--port must be 0 or in range 1-65535")
    if not MOCK_SERVER.exists():
        raise SuiteError(f"Cannot find mock server at {MOCK_SERVER}")
    if not SMOKE_SCRIPT.exists():
        raise SuiteError(f"Cannot find smoke script at {SMOKE_SCRIPT}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    sim_config = simulation_profile_config(args.profile)
    request_policy = configure_request_policy(sim_config)
    PLAYER_TOKENS.clear()

    port = args.port or pick_free_port(args.host)
    base_url = f"http://{args.host}:{port}"

    with tempfile.TemporaryDirectory(prefix="spaceshift_sim_suite_") as temp_dir:
        state_db = Path(temp_dir) / "suite_state.sqlite3"
        command = [
            sys.executable,
            str(MOCK_SERVER),
            "--host",
            args.host,
            "--port",
            str(port),
            "--state-db",
            str(state_db),
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env={
                **os.environ,
                "SPACESHIFT_AUTH_REQUIRED": "1",
                "SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN": "1",
                "SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN": "1",
                "SPACESHIFT_ENABLE_ADMIN_GOD_MODE": "1",
                "SPACESHIFT_SESSION_TTL_SECONDS": "86400",
                "SPACESHIFT_DETERMINISTIC": "1",
            },
        )
        try:
            wait_for_server_health(
                process=process,
                base_url=base_url,
                startup_timeout=args.startup_timeout,
                request_timeout=args.request_timeout,
            )
            sim_player_id = "admin"
            trader_player_id = "player.sim.trader"

            smoke = run_smoke_suite(base_url=base_url, timeout=args.request_timeout)
            admin_login = request_json(
                base_url=base_url,
                method="POST",
                path="/api/admin/login",
                timeout=args.request_timeout,
                payload={"username": "admin", "password": "admin"},
            )
            if admin_login.get("player_id") != "admin":
                raise SuiteError("admin login did not return admin player_id")
            discovery = run_discovery_world_ops(
                base_url=base_url,
                timeout=args.request_timeout,
                player_id=sim_player_id,
                seed=args.seed + 10,
                config=sim_config,
            )
            robot = run_quality_and_robot_simulation(
                base_url=base_url,
                timeout=args.request_timeout,
                player_id=sim_player_id,
                seed=args.seed + 20,
                config=sim_config,
            )
            ai_matrix = run_ai_battle_matrix(
                base_url=base_url,
                timeout=args.request_timeout,
                seed=args.seed + 30,
                config=sim_config,
            )
            market = run_market_simulation(
                base_url=base_url,
                timeout=args.request_timeout,
                player_id=trader_player_id,
                seed=args.seed + 40,
                config=sim_config,
            )
            covert = run_covert_ops_simulation(
                base_url=base_url,
                timeout=args.request_timeout,
                seed=args.seed + 45,
                config=sim_config,
            )
            ship_space = run_ship_space_and_engagement_checks(
                base_url=base_url,
                timeout=args.request_timeout,
                player_id=sim_player_id,
                seed=args.seed + 50,
                config=sim_config,
            )
            celestial = run_celestial_resource_economy(
                base_url=base_url,
                timeout=args.request_timeout,
                player_id=sim_player_id,
                seed=args.seed + 15,
                config=sim_config,
            )
            simpy_timeflow = run_simpy_timeflow(
                seed=args.seed + 60,
                profile=args.profile,
                config=sim_config,
            )

            json_path = REPORTS_DIR / f"simulation_suite_{args.tag}.json"
            md_path = REPORTS_DIR / f"simulation_suite_{args.tag}.md"
            latest_json_path = REPORTS_DIR / "latest_simulation_report.json"

            report: dict[str, Any] = {
                "meta": {
                    "generated_utc": now_utc_iso(),
                    "seed": args.seed,
                    "profile": args.profile,
                    "profile_config": sim_config,
                    "request_policy": request_policy,
                    "base_url": base_url,
                    "json_path": str(json_path.relative_to(PROJECT_ROOT)),
                    "md_path": str(md_path.relative_to(PROJECT_ROOT)),
                },
                "smoke": smoke,
                "discovery_world_ops": discovery,
                "celestial_resource_economy": celestial,
                "robot_and_quality": robot,
                "ai_battle_matrix": ai_matrix,
                "market": market,
                "covert_ops": covert,
                "simpy_timeflow": simpy_timeflow,
                "ship_space_and_engagement": ship_space,
            }
            markdown = build_markdown_report(report)

            json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            latest_json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
            md_path.write_text(markdown, encoding="utf-8")

            return report
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5.0)


def main() -> int:
    args = parse_args()
    try:
        report = run_suite(args)
    except SuiteError as exc:
        print(f"[FAIL] {exc}")
        return 1

    meta = report["meta"]
    print("[OK] Simulation suite completed.")
    print(f"[INFO] JSON report: {meta['json_path']}")
    print(f"[INFO] Markdown report: {meta['md_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
