#!/usr/bin/env python3
"""Stdlib smoke tests for SpaceShift mock server endpoints."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen


class SmokeTestError(RuntimeError):
    """Raised when a smoke check fails."""


SMOKE_PLAYER_ID = f"player.smoke.{time.time_ns()}"
PLAYER_TOKENS: dict[str, str] = {}
JWT_SMOKE_ISSUER = "https://auth.spaceshift.smoke.local"
JWT_SMOKE_AUDIENCE = "spaceshift-smoke-suite"
JWT_SMOKE_SECRET = "spaceshift-smoke-hs256-secret"
JWT_SMOKE_SUBJECT = "smoke.jwt.user"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run lightweight smoke tests against the SpaceShift mock server"
    )
    parser.add_argument(
        "--base-url",
        help="Use an already-running server (example: http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind when launching a temporary local server",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port to bind when launching a local server (0 picks a free port)",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=15.0,
        help="Seconds to wait for server health check when launching local server",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds per request",
    )
    return parser.parse_args()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeTestError(message)


def build_url(base_url: str, path: str, query: dict[str, Any] | None = None) -> str:
    base = base_url.rstrip("/")
    if not query:
        return f"{base}{path}"
    query_text = urlencode(query)
    return f"{base}{path}?{query_text}"


def request_json(
    method: str,
    url: str,
    timeout: float,
    payload: dict[str, Any] | None = None,
    bearer_token: str | None = None,
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    player_id: str | None = None
    parsed_url = urlparse(url)
    query = parse_qs(parsed_url.query, keep_blank_values=True)
    query_player_values = query.get("player_id", [])
    if query_player_values and isinstance(query_player_values[0], str):
        query_player = query_player_values[0].strip()
        if query_player:
            player_id = query_player
    if isinstance(payload, dict):
        payload_player = payload.get("player_id")
        if isinstance(payload_player, str) and payload_player.strip():
            player_id = payload_player.strip()
        payload_admin_player = payload.get("admin_player_id")
        if isinstance(payload_admin_player, str) and payload_admin_player.strip():
            player_id = payload_admin_player.strip()
    if isinstance(player_id, str):
        token = PLAYER_TOKENS.get(player_id)
        if isinstance(token, str) and token.strip():
            headers["Authorization"] = f"Bearer {token.strip()}"
    if isinstance(bearer_token, str) and bearer_token.strip():
        headers["Authorization"] = f"Bearer {bearer_token.strip()}"
    body: bytes | None = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")

    request = Request(url=url, method=method, headers=headers, data=body)
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace").strip()
        raise SmokeTestError(
            f"{method} {url} failed with HTTP {exc.code}: {response_text}"
        ) from exc
    except URLError as exc:
        raise SmokeTestError(f"{method} {url} failed: {exc.reason}") from exc

    response_text = raw.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise SmokeTestError(
            f"{method} {url} returned non-JSON response: {response_text[:240]}"
        ) from exc

    require(
        isinstance(parsed, dict),
        f"{method} {url} returned JSON that was not an object",
    )
    auth_payload = parsed.get("auth")
    if isinstance(auth_payload, dict):
        auth_player = auth_payload.get("player_id")
        auth_token = auth_payload.get("access_token")
        if isinstance(auth_player, str) and auth_player.strip() and isinstance(auth_token, str) and auth_token.strip():
            PLAYER_TOKENS[auth_player.strip()] = auth_token.strip()
    return parsed


def jwt_player_key(issuer: str, subject: str) -> str:
    digest = hashlib.sha256(f"{issuer}|{subject}".encode("utf-8")).hexdigest()
    return f"player.idp.{digest[:24]}"


def jwt_base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def build_hs256_jwt(
    *,
    issuer: str,
    audience: str,
    subject: str,
    secret: str,
    issued_at: int,
    not_before: int,
    expires_at: int,
    role: str = "player",
) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "iss": issuer,
        "aud": audience,
        "sub": subject,
        "iat": int(issued_at),
        "nbf": int(not_before),
        "exp": int(expires_at),
        "role": role,
    }
    header_part = jwt_base64url_encode(
        json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    payload_part = jwt_base64url_encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{header_part}.{payload_part}".encode("ascii")
    signature = hmac.new(
        secret.encode("utf-8"),
        signing_input,
        digestmod=hashlib.sha256,
    ).digest()
    signature_part = jwt_base64url_encode(signature)
    return f"{header_part}.{payload_part}.{signature_part}"


def ensure_smoke_profile(base_url: str, timeout: float) -> None:
    profile_payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/profile/save"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "captain_name": "Smoke Test Captain",
            "display_name": "Smoke Test Captain",
            "auth_mode": "guest",
            "email": "",
            "starting_ship_id": "ship.pathfinder_frigate",
            "tutorial_mode": "guided",
            "player_memory": {
                "onboarding": {"completed": True, "mode": "guided"},
                "notes": {"smoke": True},
            },
        },
    )
    profile = profile_payload.get("profile")
    require(
        isinstance(profile, dict) and profile.get("player_id") == SMOKE_PLAYER_ID,
        "Failed to create or load smoke-test profile",
    )
    require(
        isinstance(PLAYER_TOKENS.get(SMOKE_PLAYER_ID), str)
        and len(str(PLAYER_TOKENS[SMOKE_PLAYER_ID]).strip()) >= 10,
        "Profile save did not return a usable auth token",
    )


def ensure_profile(base_url: str, timeout: float, player_id: str, captain_name: str) -> None:
    payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/profile/save"),
        timeout=timeout,
        payload={
            "player_id": player_id,
            "captain_name": captain_name,
            "display_name": captain_name,
            "auth_mode": "guest",
            "email": "",
        },
    )
    profile = payload.get("profile")
    require(
        isinstance(profile, dict) and profile.get("player_id") == player_id,
        f"Failed to create/load profile '{player_id}'",
    )
    require(
        isinstance(PLAYER_TOKENS.get(player_id), str)
        and len(str(PLAYER_TOKENS[player_id]).strip()) >= 10,
        f"Profile save did not return usable token for '{player_id}'",
    )


def check_health(base_url: str, timeout: float) -> None:
    payload = request_json(
        method="GET",
        url=build_url(base_url, "/health"),
        timeout=timeout,
    )
    require(payload.get("status") == "ok", "Health status was not 'ok'")
    counts = payload.get("counts")
    require(isinstance(counts, dict), "Health response missing counts object")
    for key in ("missions", "materials", "crafting_substitutions"):
        require(key in counts, f"Health counts missing '{key}'")
        require(
            isinstance(counts.get(key), int) and int(counts[key]) >= 0,
            f"Health counts['{key}'] must be a non-negative integer",
        )


def check_missions(base_url: str, timeout: float) -> None:
    payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/missions", {"limit": 200}),
        timeout=timeout,
    )
    items = payload.get("items")
    require(isinstance(items, list) and len(items) > 0, "Missions list is empty")
    mission_ids = [
        row.get("id")
        for row in items
        if isinstance(row, dict) and isinstance(row.get("id"), str)
    ]
    require(len(mission_ids) > 0, "Missions payload did not include mission ids")
    ai_mission_ids = [mission_id for mission_id in mission_ids if mission_id.startswith("mission.ai_")]
    require(
        len(ai_mission_ids) > 0,
        "No AI missions found in /api/missions response",
    )
    require(
        "mission.ai_bootstrap_protocol" in mission_ids,
        "Expected AI mission 'mission.ai_bootstrap_protocol' not found",
    )


def check_elements_descriptions(base_url: str, timeout: float) -> None:
    payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/elements", {"limit": 200}),
        timeout=timeout,
    )
    items = payload.get("items")
    require(
        isinstance(items, list) and len(items) == 118,
        "Elements endpoint did not return the full 118-element set",
    )
    sample = next(
        (
            row
            for row in items
            if isinstance(row, dict) and row.get("symbol") in {"H", "Fe", "U"}
        ),
        None,
    )
    require(isinstance(sample, dict), "Elements response missing required sample rows")
    require(
        isinstance(sample.get("real_world_summary"), str)
        and len(sample["real_world_summary"].strip()) >= 12,
        "Element row missing real_world_summary text",
    )
    require(
        isinstance(sample.get("lore_hook"), str) and len(sample["lore_hook"].strip()) >= 12,
        "Element row missing lore_hook text",
    )


def check_materials(base_url: str, timeout: float) -> None:
    all_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/materials", {"limit": 200}),
        timeout=timeout,
    )
    all_items = all_payload.get("items")
    require(
        isinstance(all_items, list) and len(all_items) > 0,
        "Materials list is empty",
    )
    material_ids = [
        row.get("id")
        for row in all_items
        if isinstance(row, dict) and isinstance(row.get("id"), str)
    ]
    require(
        "material.ram_metamaterial_skin" in material_ids,
        "Expected material 'material.ram_metamaterial_skin' not found",
    )

    metamaterial_payload = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/materials",
            {"category": "metamaterial", "limit": 40},
        ),
        timeout=timeout,
    )
    metamaterial_items = metamaterial_payload.get("items")
    require(
        isinstance(metamaterial_items, list) and len(metamaterial_items) > 0,
        "No materials returned for category=metamaterial",
    )
    require(
        all(
            isinstance(row, dict)
            and isinstance(row.get("category"), str)
            and row["category"].casefold() == "metamaterial"
            for row in metamaterial_items
        ),
        "One or more category=metamaterial rows had an unexpected category",
    )


def check_substitutions(base_url: str, timeout: float) -> None:
    all_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/crafting/substitutions", {"limit": 200}),
        timeout=timeout,
    )
    all_items = all_payload.get("items")
    require(
        isinstance(all_items, list) and len(all_items) > 0,
        "Crafting substitutions list is empty",
    )
    substitution_ids = [
        row.get("id")
        for row in all_items
        if isinstance(row, dict) and isinstance(row.get("id"), str)
    ]
    require(
        "sub.cognitive_core_palladium_saver" in substitution_ids,
        "Expected substitution 'sub.cognitive_core_palladium_saver' not found",
    )

    filtered_payload = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/crafting/substitutions",
            {
                "item_id": "module.special_cognitive_battle_core_mk6",
                "limit": 20,
            },
        ),
        timeout=timeout,
    )
    filtered_items = filtered_payload.get("items")
    require(
        isinstance(filtered_items, list) and len(filtered_items) > 0,
        "No substitutions returned for module.special_cognitive_battle_core_mk6",
    )
    require(
        all(
            isinstance(row, dict)
            and row.get("item_id") == "module.special_cognitive_battle_core_mk6"
            for row in filtered_items
        ),
        "Filtered substitutions included rows for a different item_id",
    )
    require(
        any(
            isinstance(row, dict) and row.get("id") == "sub.cognitive_core_palladium_saver"
            for row in filtered_items
        ),
        "Filtered substitutions did not include sub.cognitive_core_palladium_saver",
    )


def check_crafting_quote_with_substitution(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)

    payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/crafting/quote"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "item_id": "module.special_cognitive_battle_core_mk6",
            "quantity": 1,
            "substitution_id": "sub.cognitive_core_palladium_saver",
        },
    )
    require(
        payload.get("item_id") == "module.special_cognitive_battle_core_mk6",
        "Crafting quote returned unexpected item_id",
    )
    require(
        payload.get("substitution_id") == "sub.cognitive_core_palladium_saver",
        "Crafting quote did not apply requested substitution_id",
    )
    selected = payload.get("selected_substitution")
    require(
        isinstance(selected, dict)
        and selected.get("id") == "sub.cognitive_core_palladium_saver",
        "selected_substitution missing expected substitution id",
    )
    available = payload.get("available_substitutions")
    require(
        isinstance(available, list)
        and any(
            isinstance(row, dict) and row.get("id") == "sub.cognitive_core_palladium_saver"
            for row in available
        ),
        "available_substitutions did not include requested substitution id",
    )
    cost = payload.get("cost")
    require(isinstance(cost, dict), "Crafting quote missing cost object")
    credits = cost.get("credits")
    require(
        isinstance(credits, (int, float)) and float(credits) > 0,
        "Crafting quote cost.credits must be numeric and > 0",
    )


def check_research_compute(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    compute = request_json(
        method="GET",
        url=build_url(base_url, "/api/research/compute", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    require(
        isinstance(compute.get("compute_power_per_hour"), (int, float))
        and float(compute["compute_power_per_hour"]) > 0,
        "Research compute profile missing compute_power_per_hour",
    )
    components = compute.get("components")
    require(isinstance(components, dict), "Research compute profile missing components object")
    jobs = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/research/jobs",
            {"player_id": SMOKE_PLAYER_ID, "status": "active", "limit": 20},
        ),
        timeout=timeout,
    )
    require(isinstance(jobs.get("items"), list), "Research jobs endpoint missing items list")


def check_research_tracks(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    payload = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/research/tracks",
            {"player_id": SMOKE_PLAYER_ID, "limit": 20},
        ),
        timeout=timeout,
    )
    items = payload.get("items")
    require(
        isinstance(items, list) and len(items) > 0,
        "Research tracks endpoint returned no tracks",
    )
    sample = items[0]
    require(isinstance(sample, dict), "Research tracks item was not an object")
    require(
        isinstance(sample.get("entry_tech_id"), str)
        and sample["entry_tech_id"].startswith("tech."),
        "Research tracks item missing entry_tech_id",
    )
    require(
        isinstance(sample.get("stages"), list) and len(sample["stages"]) > 0,
        "Research tracks item missing stages",
    )
    require(
        isinstance(sample.get("is_unlocked"), bool),
        "Research tracks item missing player lock state",
    )


def check_profile_identity_and_memory(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    starter_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/starter-ships", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    starter_items = starter_payload.get("items")
    require(
        isinstance(starter_items, list) and len(starter_items) > 0,
        "Starter ships endpoint returned no starter ship options",
    )
    selected_starter = starter_payload.get("selected_starting_ship_id")
    require(
        isinstance(selected_starter, str) and len(selected_starter.strip()) > 0,
        "Starter ships endpoint missing selected_starting_ship_id",
    )
    starter_row = starter_items[0] if isinstance(starter_items[0], dict) else {}
    require(
        isinstance(starter_row.get("growth_profile"), dict),
        "Starter ships payload missing growth_profile object",
    )
    profile_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/profile", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    display_name = profile_payload.get("display_name")
    require(
        isinstance(display_name, str) and len(display_name.strip()) >= 3,
        "Profile payload missing display_name",
    )
    tutorial_mode = profile_payload.get("tutorial_mode")
    require(
        tutorial_mode in {None, "guided", "quick", "skip"},
        "Profile payload tutorial_mode invalid",
    )
    memory_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/profile/memory", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    require(
        isinstance(memory_payload.get("player_memory"), dict),
        "Profile memory endpoint did not return object player_memory",
    )
    update_payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/profile/memory"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "merge": True,
            "player_memory": {
                "smoke_runtime": {
                    "last_check": "profile_memory",
                    "ok": True,
                }
            },
        },
    )
    updated_memory = update_payload.get("player_memory")
    require(isinstance(updated_memory, dict), "Profile memory update returned invalid payload")
    runtime_block = updated_memory.get("smoke_runtime") if isinstance(updated_memory, dict) else None
    require(
        isinstance(runtime_block, dict) and runtime_block.get("ok") is True,
        "Profile memory merge did not persist smoke_runtime payload",
    )


def check_dev_player_login(base_url: str, timeout: float) -> None:
    payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/player/login"),
        timeout=timeout,
        payload={"username": "player", "password": "player"},
    )
    require(payload.get("ok") is True, "Player dev login did not return ok=true")
    require(payload.get("player_id") == "player", "Player dev login returned wrong player_id")
    auth = payload.get("auth")
    require(
        isinstance(auth, dict) and isinstance(auth.get("access_token"), str),
        "Player dev login missing auth token",
    )
    require(
        isinstance(auth.get("issued_utc"), str) and str(auth.get("issued_utc")).strip(),
        "Player dev login missing token issued timestamp",
    )
    expires_utc = auth.get("expires_utc")
    if expires_utc is not None:
        require(
            isinstance(expires_utc, str) and expires_utc.strip(),
            "Player dev login returned invalid token expiry timestamp",
        )


def check_combat_contacts_and_flee(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    contacts_payload = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/combat/contacts",
            {"player_id": SMOKE_PLAYER_ID, "count": 4, "seed": 77},
        ),
        timeout=timeout,
    )
    contacts = contacts_payload.get("items")
    require(isinstance(contacts, list) and len(contacts) > 0, "No combat contacts returned")
    contact = contacts[0]
    require(isinstance(contact, dict), "Combat contact row was not an object")
    odds = contact.get("odds")
    require(isinstance(odds, dict), "Combat contact missing odds payload")
    require(
        isinstance(odds.get("attacker_win_probability"), (int, float)),
        "Combat odds payload missing attacker_win_probability",
    )
    require(
        isinstance(contact.get("combat_level"), int) and int(contact["combat_level"]) > 0,
        "Combat contact missing combat_level",
    )
    require(
        isinstance(contact.get("level_gap"), int),
        "Combat contact missing level_gap",
    )
    flee = request_json(
        method="POST",
        url=build_url(base_url, "/api/combat/engage"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "contact": contact,
            "action": "flee",
        },
    )
    require(flee.get("action") == "flee", "Combat flee action response mismatch")
    require(
        isinstance(flee.get("flee_probability"), (int, float)),
        "Combat flee response missing flee_probability",
    )
    fight = request_json(
        method="POST",
        url=build_url(base_url, "/api/combat/engage"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "contact": contact,
            "action": "fight",
            "player_initiated_attack": True,
            "context": {
                "tactical_commands": {
                    "attacker": [{"round": 1, "action": "stealth_burst", "magnitude": 1.0}]
                }
            },
        },
    )
    require(fight.get("action") == "fight", "Combat fight action response mismatch")
    balance = fight.get("engagement_balance")
    require(isinstance(balance, dict), "Combat fight missing engagement_balance payload")
    reward_scaling = balance.get("reward_scaling")
    require(
        isinstance(reward_scaling, dict)
        and isinstance(reward_scaling.get("reward_scale"), (int, float)),
        "Combat fight missing reward scaling details",
    )


def check_combat_authoritative_persisted_loadout(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    memory_update = request_json(
        method="POST",
        url=build_url(base_url, "/api/profile/memory"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "merge": True,
            "player_memory": {
                "combat_loadout": {
                    "hull_id": "hull.settler_scout",
                    "modules": [
                        {"id": "module.scanner_longrange_array_mk1", "quantity": 1},
                        {"id": "module.relay_grid_mk1", "quantity": 1},
                    ],
                }
            },
        },
    )
    updated_memory = memory_update.get("player_memory")
    require(isinstance(updated_memory, dict), "Profile memory update did not return player_memory")

    contacts_a = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/combat/contacts",
            {"player_id": SMOKE_PLAYER_ID, "count": 3, "seed": 177},
        ),
        timeout=timeout,
    )
    contacts_b = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/combat/contacts",
            {"player_id": SMOKE_PLAYER_ID, "count": 3, "seed": 177},
        ),
        timeout=timeout,
    )
    source = contacts_a.get("player_stats_source")
    require(
        isinstance(source, str) and source.startswith("persisted_loadout."),
        "Combat contacts did not use persisted loadout authority source",
    )
    require(
        contacts_a.get("player_stats") == contacts_b.get("player_stats"),
        "Combat contacts player_stats were not stable across repeated deterministic requests",
    )
    contact_items = contacts_a.get("items")
    require(
        isinstance(contact_items, list) and len(contact_items) > 0 and isinstance(contact_items[0], dict),
        "Combat contacts did not return at least one contact for authoritative loadout check",
    )
    defender_stats = contact_items[0].get("stats")
    require(isinstance(defender_stats, dict), "Combat contact missing stats object for odds check")

    low_attacker = {
        "attack": 1.0,
        "defense": 1.0,
        "hull": 10.0,
        "shield": 1.0,
        "energy": 10.0,
        "scan": 1.0,
        "cloak": 1.0,
    }
    baseline_odds = request_json(
        method="POST",
        url=build_url(base_url, "/api/combat/odds"),
        timeout=timeout,
        payload={
            "attacker": {"name": "Baseline", "stats": low_attacker},
            "defender": {"name": "Contact", "stats": defender_stats},
            "context": {"mode": "pvp", "max_rounds": 8, "seed": 177},
        },
    )
    authoritative_odds = request_json(
        method="POST",
        url=build_url(base_url, "/api/combat/odds"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "attacker": {"name": "Baseline", "stats": low_attacker},
            "defender": {"name": "Contact", "stats": defender_stats},
            "context": {"mode": "pvp", "max_rounds": 8, "seed": 177},
        },
    )
    authoritative_source = authoritative_odds.get("attacker_source")
    require(
        isinstance(authoritative_source, str)
        and authoritative_source.startswith("persisted_loadout."),
        "Combat odds did not report persisted loadout attacker_source",
    )
    require(
        isinstance(baseline_odds.get("attacker_score"), (int, float))
        and isinstance(authoritative_odds.get("attacker_score"), (int, float))
        and float(authoritative_odds["attacker_score"]) > float(baseline_odds["attacker_score"]),
        "Combat odds attacker_score was not upgraded by authoritative player loadout resolution",
    )


def check_fairplay_policy(base_url: str, timeout: float) -> None:
    payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/fairplay/policy"),
        timeout=timeout,
    )
    require(
        payload.get("monetization_model") == "non_pay_to_win",
        "Fairplay policy missing non_pay_to_win model",
    )
    principles = payload.get("principles")
    require(
        isinstance(principles, list)
        and any(isinstance(item, str) and "No direct stat purchases" in item for item in principles),
        "Fairplay policy principles were missing expected language",
    )


def check_economy_fleet_and_unlocks(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    wallet = request_json(
        method="GET",
        url=build_url(base_url, "/api/economy/wallet", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    require(wallet.get("player_id") == SMOKE_PLAYER_ID, "Wallet endpoint returned wrong player_id")
    require(
        isinstance(wallet.get("credits"), (int, float)) and float(wallet["credits"]) > 0,
        "Wallet endpoint missing credits balance",
    )
    require(
        isinstance(wallet.get("voidcoin"), (int, float)) and float(wallet["voidcoin"]) >= 0,
        "Wallet endpoint missing voidcoin balance",
    )
    life_support_wallet = wallet.get("life_support")
    require(
        isinstance(life_support_wallet, dict),
        "Wallet endpoint missing life_support payload",
    )
    ls_wallet_inventory = (
        life_support_wallet.get("inventory")
        if isinstance(life_support_wallet.get("inventory"), dict)
        else {}
    )
    for symbol in ("AIR", "H2O", "FOOD"):
        require(
            isinstance(ls_wallet_inventory.get(symbol), (int, float)),
            f"Wallet life_support inventory missing symbol '{symbol}'",
        )

    inventory = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/economy/inventory",
            {"player_id": SMOKE_PLAYER_ID, "limit": 20},
        ),
        timeout=timeout,
    )
    inventory_items = inventory.get("items")
    require(
        isinstance(inventory_items, list) and len(inventory_items) > 0,
        "Economy inventory endpoint returned no inventory rows",
    )
    require(
        any(isinstance(row, dict) and row.get("symbol") == "Fe" for row in inventory_items),
        "Economy inventory endpoint did not include expected starter symbol Fe",
    )
    life_support_inventory = inventory.get("life_support")
    require(
        isinstance(life_support_inventory, dict),
        "Inventory endpoint missing life_support payload",
    )
    state_payload = (
        life_support_inventory.get("state")
        if isinstance(life_support_inventory.get("state"), dict)
        else {}
    )
    require(
        isinstance(state_payload.get("shortage_stress"), (int, float)),
        "Inventory life_support payload missing shortage_stress",
    )

    life_support_status = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/life-support/status",
            {"player_id": SMOKE_PLAYER_ID, "force_tick": "true"},
        ),
        timeout=timeout,
    )
    require(
        life_support_status.get("player_id") == SMOKE_PLAYER_ID,
        "Life-support status endpoint returned wrong player_id",
    )
    ls_state = life_support_status.get("state")
    require(isinstance(ls_state, dict), "Life-support status missing state object")
    ls_inventory = life_support_status.get("inventory")
    require(
        isinstance(ls_inventory, dict)
        and all(
            isinstance(ls_inventory.get(symbol), (int, float))
            for symbol in ("AIR", "H2O", "FOOD")
        ),
        "Life-support status inventory is incomplete",
    )
    ls_tick = life_support_status.get("tick")
    require(
        isinstance(ls_tick, dict) and isinstance(ls_tick.get("effective_hours"), (int, float)),
        "Life-support status missing tick/effective_hours data",
    )

    unlocks = request_json(
        method="GET",
        url=build_url(base_url, "/api/research/unlocks", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    unlock_items = unlocks.get("items")
    require(
        isinstance(unlock_items, list) and len(unlock_items) > 0,
        "Research unlocks endpoint returned no unlocked tech ids",
    )
    require(
        all(isinstance(tech_id, str) for tech_id in unlock_items),
        "Research unlocks endpoint returned non-string tech ids",
    )

    fleet = request_json(
        method="GET",
        url=build_url(base_url, "/api/fleet/status", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    require(fleet.get("player_id") == SMOKE_PLAYER_ID, "Fleet status returned wrong player_id")
    require(
        isinstance(fleet.get("active_hull_id"), str) and len(fleet["active_hull_id"].strip()) > 0,
        "Fleet status missing active_hull_id",
    )
    require(
        isinstance(fleet.get("crew_total"), (int, float)) and float(fleet["crew_total"]) > 0,
        "Fleet status missing crew_total",
    )


def check_market_core(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    policy = request_json(
        method="GET",
        url=build_url(base_url, "/api/market/policy"),
        timeout=timeout,
    )
    supported_types = policy.get("supported_asset_types")
    require(
        isinstance(supported_types, list) and "element" in supported_types,
        "Market policy missing supported_asset_types",
    )
    regions = request_json(
        method="GET",
        url=build_url(base_url, "/api/market/regions"),
        timeout=timeout,
    )
    region_items = regions.get("items")
    require(
        isinstance(region_items, list) and len(region_items) > 0,
        "Market regions endpoint returned no regions",
    )

    snapshot = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/market/snapshot",
            {"player_id": SMOKE_PLAYER_ID, "limit": 6},
        ),
        timeout=timeout,
    )
    market_items = snapshot.get("items")
    require(
        isinstance(market_items, list) and len(market_items) > 0,
        "Market snapshot endpoint returned no rows",
    )
    symbol = None
    for row in market_items:
        if isinstance(row, dict) and isinstance(row.get("symbol"), str):
            symbol = row["symbol"]
            break
    require(isinstance(symbol, str), "Market snapshot rows were missing symbol values")

    listings = request_json(
        method="GET",
        url=build_url(base_url, "/api/market/listings", {"limit": 12, "status": "active"}),
        timeout=timeout,
    )
    listing_items = listings.get("items")
    require(isinstance(listing_items, list), "Market listings endpoint missing items list")

    exchange = request_json(
        method="POST",
        url=build_url(base_url, "/api/market/exchange"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "direction": "buy_voidcoin",
            "amount": 1.0,
        },
    )
    require(exchange.get("direction") == "buy_voidcoin", "Market exchange direction mismatch")
    require(isinstance(exchange.get("wallet"), dict), "Market exchange missing wallet payload")

    bought = request_json(
        method="POST",
        url=build_url(base_url, "/api/market/buy"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "symbol": symbol,
            "quantity": 0.25,
            "currency": "credits",
        },
    )
    require(bought.get("side") == "buy", "Market buy endpoint did not return side=buy")
    require(
        bought.get("symbol") == symbol,
        "Market buy endpoint returned the wrong symbol",
    )

    sold = request_json(
        method="POST",
        url=build_url(base_url, "/api/market/sell"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "symbol": symbol,
            "quantity": 0.25,
            "currency": "credits",
        },
    )
    require(sold.get("side") == "sell", "Market sell endpoint did not return side=sell")
    require(
        sold.get("symbol") == symbol,
        "Market sell endpoint returned the wrong symbol",
    )
    history = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/market/history",
            {
                "limit": 12,
                "asset_type": "element",
                "asset_id": symbol,
                "currency": "credits",
            },
        ),
        timeout=timeout,
    )
    history_items = history.get("items")
    require(
        isinstance(history_items, list) and len(history_items) > 0,
        "Market history endpoint returned no trades after buy/sell",
    )
    summary = history.get("price_summary")
    require(
        isinstance(summary, dict) and isinstance(summary.get("sample_size"), int),
        "Market history endpoint missing price_summary payload",
    )


def check_crafting_build_and_assets(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    quote = request_json(
        method="POST",
        url=build_url(base_url, "/api/crafting/quote"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "item_id": "hull.settler_scout",
            "quantity": 1,
        },
    )
    require(
        quote.get("item_id") == "hull.settler_scout",
        "Crafting quote returned unexpected hull id",
    )
    require(quote.get("can_craft") is True, "Expected hull.settler_scout to be craftable")

    build = request_json(
        method="POST",
        url=build_url(base_url, "/api/crafting/build"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "item_id": "hull.settler_scout",
            "quantity": 1,
        },
    )
    require(build.get("item_id") == "hull.settler_scout", "Crafting build returned wrong item_id")
    require(build.get("item_kind") == "hull", "Crafting build returned wrong item_kind")

    assets = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/assets",
            {"player_id": SMOKE_PLAYER_ID, "asset_type": "hull", "limit": 20},
        ),
        timeout=timeout,
    )
    asset_items = assets.get("items")
    require(isinstance(asset_items, list), "Assets endpoint missing items list")
    require(
        any(
            isinstance(row, dict)
            and row.get("asset_id") == "hull.settler_scout"
            and isinstance(row.get("quantity"), int)
            and row["quantity"] >= 1
            for row in asset_items
        ),
        "Assets endpoint did not include the crafted hull.settler_scout",
    )

    instances = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/assets/instances",
            {
                "player_id": SMOKE_PLAYER_ID,
                "asset_type": "hull",
                "asset_id": "hull.settler_scout",
                "limit": 20,
            },
        ),
        timeout=timeout,
    )
    instance_items = instances.get("items")
    require(
        isinstance(instance_items, list) and len(instance_items) > 0,
        "Asset instances endpoint returned no crafted hull instances",
    )
    require(
        all(
            isinstance(row, dict)
            and isinstance(row.get("instance_id"), str)
            and len(row["instance_id"].strip()) > 0
            for row in instance_items
        ),
        "Asset instances endpoint returned rows without instance_id",
    )


def check_world_claim_and_harvest(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    scan_seed = int(time.time_ns() % 1_000_000_000)
    scan = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/discovery/scan",
            {
                "player_id": SMOKE_PLAYER_ID,
                "body_class": "asteroid",
                "count": 1,
                "seed": scan_seed,
                "scan_power": 130,
            },
        ),
        timeout=timeout,
    )
    discovered = scan.get("items")
    require(
        isinstance(discovered, list) and len(discovered) == 1,
        "Discovery scan did not return exactly one world candidate",
    )
    world = discovered[0]
    require(isinstance(world, dict), "Discovery scan world row was not an object")
    world_id = world.get("world_id")
    require(
        isinstance(world_id, str) and len(world_id.strip()) > 0,
        "Discovery scan world row missing world_id",
    )

    claimed = request_json(
        method="POST",
        url=build_url(base_url, "/api/worlds/claim"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "world_id": world_id,
        },
    )
    claimed_world = claimed.get("world")
    require(
        isinstance(claimed_world, dict) and claimed_world.get("world_id") == world_id,
        "World claim endpoint did not return the claimed world",
    )
    require(
        claimed_world.get("body_class") == "asteroid",
        "Claimed world body_class does not match scan filter",
    )
    require(
        bool(claimed_world.get("is_depletable")) is True,
        "Asteroid claims must be flagged as depletable",
    )

    owned = request_json(
        method="GET",
        url=build_url(base_url, "/api/worlds/owned", {"player_id": SMOKE_PLAYER_ID}),
        timeout=timeout,
    )
    owned_items = owned.get("items")
    require(isinstance(owned_items, list), "Owned worlds endpoint missing items list")
    require(
        any(isinstance(row, dict) and row.get("world_id") == world_id for row in owned_items),
        "Owned worlds endpoint did not include claimed world_id",
    )

    detail = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/worlds/detail",
            {"player_id": SMOKE_PLAYER_ID, "world_id": world_id},
        ),
        timeout=timeout,
    )
    detail_world = detail.get("world")
    require(
        isinstance(detail_world, dict) and detail_world.get("world_id") == world_id,
        "World detail endpoint returned wrong world",
    )
    require(isinstance(detail.get("projection"), dict), "World detail endpoint missing projection")

    harvest = request_json(
        method="POST",
        url=build_url(base_url, "/api/worlds/harvest"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "world_id": world_id,
            "hours": 1.0,
        },
    )
    require(harvest.get("world_id") == world_id, "World harvest endpoint returned wrong world_id")
    require(isinstance(harvest.get("harvested"), list), "World harvest endpoint missing harvested list")
    require(
        isinstance(harvest.get("inventory_changes"), dict),
        "World harvest endpoint missing inventory_changes payload",
    )
    require(
        isinstance(harvest.get("depletion"), dict),
        "World harvest endpoint missing depletion payload",
    )
    depletion = harvest.get("depletion", {})
    before_remaining = claimed_world.get("remaining_total_units")
    after_world = harvest.get("world")
    require(isinstance(after_world, dict), "World harvest endpoint missing world payload")
    after_remaining = after_world.get("remaining_total_units")
    require(
        isinstance(before_remaining, (int, float)) and isinstance(after_remaining, (int, float)),
        "Depletable world harvest did not return numeric remaining_total_units",
    )
    require(
        float(after_remaining) <= float(before_remaining) + 1e-9,
        "Depletable harvest should not increase remaining_total_units",
    )
    if float(after_remaining) > 0.0:
        harvest_two = request_json(
            method="POST",
            url=build_url(base_url, "/api/worlds/harvest"),
            timeout=timeout,
            payload={
                "player_id": SMOKE_PLAYER_ID,
                "world_id": world_id,
                "hours": 1.0,
            },
        )
        world_two = harvest_two.get("world")
        require(isinstance(world_two, dict), "Second world harvest missing world payload")
        remaining_two = world_two.get("remaining_total_units")
        require(
            isinstance(remaining_two, (int, float)),
            "Second depletable harvest missing remaining_total_units",
        )
        require(
            float(remaining_two) <= float(after_remaining) + 1e-9,
            "Second depletable harvest should continue reducing remaining_total_units",
        )


def check_catalogs_and_job_queues(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    consumables = request_json(
        method="GET",
        url=build_url(base_url, "/api/consumables", {"limit": 20}),
        timeout=timeout,
    )
    consume_items = consumables.get("items")
    require(
        isinstance(consume_items, list) and len(consume_items) > 0,
        "Consumables endpoint returned no consumables",
    )

    board = request_json(
        method="GET",
        url=build_url(base_url, "/api/contracts/board", {"limit": 20}),
        timeout=timeout,
    )
    board_items = board.get("items")
    require(
        isinstance(board_items, list) and len(board_items) > 0,
        "Contracts board endpoint returned no templates",
    )

    contract_jobs = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/contracts/jobs",
            {"player_id": SMOKE_PLAYER_ID, "status": "active", "limit": 20},
        ),
        timeout=timeout,
    )
    require(
        isinstance(contract_jobs.get("items"), list),
        "Contracts jobs endpoint missing items list",
    )

    manufacturing_jobs = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/manufacturing/jobs",
            {"player_id": SMOKE_PLAYER_ID, "status": "active", "limit": 20},
        ),
        timeout=timeout,
    )
    require(
        isinstance(manufacturing_jobs.get("items"), list),
        "Manufacturing jobs endpoint missing items list",
    )

    reverse_jobs = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/reverse-engineering/jobs",
            {"player_id": SMOKE_PLAYER_ID, "status": "active", "limit": 20},
        ),
        timeout=timeout,
    )
    require(
        isinstance(reverse_jobs.get("items"), list),
        "Reverse-engineering jobs endpoint missing items list",
    )


def check_advanced_post_flows(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    peer_id = f"{SMOKE_PLAYER_ID}.peer"
    peer_payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/profile/save"),
        timeout=timeout,
        payload={
            "player_id": peer_id,
            "captain_name": "Smoke Peer Captain",
            "auth_mode": "guest",
            "email": "",
        },
    )
    require(
        isinstance(peer_payload.get("profile"), dict)
        and peer_payload["profile"].get("player_id") == peer_id,
        "Failed to create peer profile for advanced post-flow checks",
    )

    fitting = request_json(
        method="POST",
        url=build_url(base_url, "/api/fitting/simulate"),
        timeout=timeout,
        payload={
            "hull_id": "hull.settler_scout",
            "modules": [{"id": "module.armor_titanium_plating_mk1", "quantity": 1}],
            "runs": 8,
        },
    )
    require(fitting.get("can_fit") is True, "Fitting simulate returned can_fit != true")
    require(
        isinstance(fitting.get("odds"), dict),
        "Fitting simulate response missing odds payload",
    )
    low_level_fit = request_json(
        method="POST",
        url=build_url(base_url, "/api/fitting/simulate"),
        timeout=timeout,
        payload={
            "hull_id": "hull.settler_scout",
            "hull_level": 1,
            "modules": [{"id": "module.armor_titanium_plating_mk1", "quantity": 1, "level": 1}],
            "runs": 20,
            "seed": 91,
        },
    )
    high_level_fit = request_json(
        method="POST",
        url=build_url(base_url, "/api/fitting/simulate"),
        timeout=timeout,
        payload={
            "hull_id": "hull.settler_scout",
            "hull_level": 8,
            "modules": [{"id": "module.armor_titanium_plating_mk1", "quantity": 1, "level": 8}],
            "runs": 20,
            "seed": 91,
        },
    )
    require(
        isinstance(low_level_fit.get("combat_score"), (int, float))
        and isinstance(high_level_fit.get("combat_score"), (int, float))
        and float(high_level_fit["combat_score"]) > float(low_level_fit["combat_score"]),
        "Higher level fitting did not produce higher combat_score",
    )

    listing_created = request_json(
        method="POST",
        url=build_url(base_url, "/api/market/listings/create"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "asset_type": "element",
            "asset_id": "Fe",
            "quantity": 12,
            "currency": "credits",
            "unit_price": 7.5,
            "region_id": "region.core_worlds",
            "ttl_hours": 12,
        },
    )
    listing = listing_created.get("listing")
    require(
        isinstance(listing, dict) and isinstance(listing.get("listing_id"), str),
        "Market listing create did not return listing_id",
    )
    listing_id = str(listing["listing_id"])

    listing_bought = request_json(
        method="POST",
        url=build_url(base_url, "/api/market/listings/buy"),
        timeout=timeout,
        payload={
            "player_id": peer_id,
            "listing_id": listing_id,
            "quantity": 5,
        },
    )
    require(
        listing_bought.get("buyer_player_id") == peer_id,
        "Market listing buy returned wrong buyer_player_id",
    )
    require(
        isinstance(listing_bought.get("listing_after"), dict),
        "Market listing buy response missing listing_after",
    )

    listing_cancelled = request_json(
        method="POST",
        url=build_url(base_url, "/api/market/listings/cancel"),
        timeout=timeout,
        payload={
            "player_id": SMOKE_PLAYER_ID,
            "listing_id": listing_id,
        },
    )
    require(
        isinstance(listing_cancelled.get("listing_after"), dict)
        and listing_cancelled["listing_after"].get("status") == "cancelled",
        "Market listing cancel did not set status=cancelled",
    )

    admin_enabled = False
    try:
        admin_login = request_json(
            method="POST",
            url=build_url(base_url, "/api/admin/login"),
            timeout=timeout,
            payload={"username": "admin", "password": "admin"},
        )
        admin_enabled = (
            admin_login.get("player_id") == "admin" and isinstance(admin_login.get("auth"), dict)
        )
    except SmokeTestError:
        admin_enabled = False

    if admin_enabled:
        built_module = request_json(
            method="POST",
            url=build_url(base_url, "/api/crafting/build"),
            timeout=timeout,
            payload={
                "player_id": "admin",
                "item_id": "module.armor_titanium_plating_mk1",
                "quantity": 1,
            },
        )
        quality_rows = built_module.get("quality_instances")
        require(
            isinstance(quality_rows, list)
            and len(quality_rows) > 0
            and isinstance(quality_rows[0], dict)
            and isinstance(quality_rows[0].get("instance_id"), str),
            "Admin module build did not return quality instance rows",
        )
        leveled = request_json(
            method="POST",
            url=build_url(base_url, "/api/assets/instances/level-up"),
            timeout=timeout,
            payload={
                "player_id": "admin",
                "instance_id": quality_rows[0]["instance_id"],
                "levels": 3,
            },
        )
        before_level = leveled.get("instance_before", {}).get("item_level")
        after_level = leveled.get("instance_after", {}).get("item_level")
        require(
            isinstance(before_level, int)
            and isinstance(after_level, int)
            and after_level > before_level,
            "Asset instance level-up did not increase item_level",
        )

        mfg_started = request_json(
            method="POST",
            url=build_url(base_url, "/api/manufacturing/start"),
            timeout=timeout,
            payload={
                "player_id": "admin",
                "item_id": "hull.settler_scout",
                "quantity": 1,
                "profile_id": "mfg_profile.baseline_fabricator",
            },
        )
        mfg_job = mfg_started.get("job")
        require(
            isinstance(mfg_job, dict) and isinstance(mfg_job.get("job_id"), str),
            "Manufacturing start did not return job_id",
        )
        mfg_cancelled = request_json(
            method="POST",
            url=build_url(base_url, "/api/manufacturing/cancel"),
            timeout=timeout,
            payload={
                "player_id": "admin",
                "job_id": mfg_job["job_id"],
            },
        )
        require(
            isinstance(mfg_cancelled.get("job_after"), dict)
            and mfg_cancelled["job_after"].get("status") == "cancelled",
            "Manufacturing cancel did not set status=cancelled",
        )

        reverse_started = request_json(
            method="POST",
            url=build_url(base_url, "/api/reverse-engineering/start"),
            timeout=timeout,
            payload={
                "player_id": "admin",
                "recipe_id": "re_recipe.neural_rail_lance",
            },
        )
        require(
            isinstance(reverse_started.get("job"), dict)
            and reverse_started["job"].get("status") == "active",
            "Reverse-engineering start did not return active job",
        )

    contract_accepted = request_json(
        method="POST",
        url=build_url(base_url, "/api/contracts/accept"),
        timeout=timeout,
        payload={
            "player_id": peer_id,
            "template_id": "contract.salvage_precursor_spindle",
        },
    )
    active_contract = contract_accepted.get("job")
    require(
        isinstance(active_contract, dict)
        and isinstance(active_contract.get("contract_job_id"), str),
        "Contract accept did not return contract_job_id",
    )
    _ = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/discovery/scan",
            {"player_id": peer_id, "count": 6, "seed": 4242},
        ),
        timeout=timeout,
    )
    contract_completed = request_json(
        method="POST",
        url=build_url(base_url, "/api/contracts/complete"),
        timeout=timeout,
        payload={
            "player_id": peer_id,
            "contract_job_id": active_contract["contract_job_id"],
        },
    )
    require(
        isinstance(contract_completed.get("job_claimed"), dict)
        and contract_completed["job_claimed"].get("status") == "claimed",
        "Contract complete did not transition to claimed state",
    )

    contract_accepted_2 = request_json(
        method="POST",
        url=build_url(base_url, "/api/contracts/accept"),
        timeout=timeout,
        payload={
            "player_id": peer_id,
            "template_id": "contract.hauling_fe_frontier_bulk",
        },
    )
    active_contract_2 = contract_accepted_2.get("job")
    require(
        isinstance(active_contract_2, dict)
        and isinstance(active_contract_2.get("contract_job_id"), str),
        "Second contract accept did not return contract_job_id",
    )
    contract_abandoned = request_json(
        method="POST",
        url=build_url(base_url, "/api/contracts/abandon"),
        timeout=timeout,
        payload={
            "player_id": peer_id,
            "contract_job_id": active_contract_2["contract_job_id"],
        },
    )
    require(
        isinstance(contract_abandoned.get("job_after"), dict)
        and contract_abandoned["job_after"].get("status") == "abandoned",
        "Contract abandon did not set status=abandoned",
    )


def check_admin_inventory_controls(base_url: str, timeout: float) -> None:
    admin_enabled = False
    try:
        admin_login = request_json(
            method="POST",
            url=build_url(base_url, "/api/admin/login"),
            timeout=timeout,
            payload={"username": "admin", "password": "admin"},
        )
        admin_enabled = (
            admin_login.get("player_id") == "admin" and isinstance(admin_login.get("auth"), dict)
        )
    except SmokeTestError:
        admin_enabled = False

    if not admin_enabled:
        return

    target_id = f"player.smoke.adminctrl.{time.time_ns()}"
    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=target_id,
        captain_name="AdminCtrl Target",
    )

    players_payload = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/admin/players",
            {"player_id": "admin", "search": target_id, "limit": 50},
        ),
        timeout=timeout,
    )
    players = players_payload.get("items")
    require(isinstance(players, list), "Admin players endpoint missing items list")
    require(
        any(isinstance(row, dict) and row.get("player_id") == target_id for row in players),
        "Admin players endpoint did not include expected target profile",
    )

    jackpot = request_json(
        method="POST",
        url=build_url(base_url, "/api/admin/crafting/jackpot"),
        timeout=timeout,
        payload={
            "admin_player_id": "admin",
            "target_player_id": target_id,
            "item_id": "module.weapon_laser_bank_mk1",
            "quantity": 2,
            "jackpot_tier": "mythic",
        },
    )
    require(
        jackpot.get("target_player_id") == target_id
        and jackpot.get("item_kind") == "module"
        and int(jackpot.get("instances_created", 0)) >= 1,
        "Admin jackpot endpoint did not create expected module instances",
    )

    storage_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/inventory/storage", {"player_id": target_id}),
        timeout=timeout,
    )
    personal_storage = storage_payload.get("personal")
    require(isinstance(personal_storage, dict), "Storage profile missing personal section")

    smuggle_in = request_json(
        method="POST",
        url=build_url(base_url, "/api/assets/smuggle/move"),
        timeout=timeout,
        payload={
            "player_id": target_id,
            "direction": "to_smuggle",
            "asset_type": "module",
            "asset_id": "module.weapon_laser_bank_mk1",
            "quantity": 1,
        },
    )
    require(
        isinstance(smuggle_in.get("storage"), dict),
        "Smuggle move-in response missing storage snapshot",
    )
    smuggle_out = request_json(
        method="POST",
        url=build_url(base_url, "/api/assets/smuggle/move"),
        timeout=timeout,
        payload={
            "player_id": target_id,
            "direction": "to_personal",
            "asset_type": "module",
            "asset_id": "module.weapon_laser_bank_mk1",
            "quantity": 1,
        },
    )
    require(
        isinstance(smuggle_out.get("storage"), dict),
        "Smuggle move-out response missing storage snapshot",
    )

    instances = jackpot.get("instances")
    require(isinstance(instances, list) and len(instances) > 0, "Jackpot response missing instances")
    instance_id = instances[0].get("instance_id") if isinstance(instances[0], dict) else None
    require(
        isinstance(instance_id, str) and len(instance_id) > 4,
        "Missing crafted instance_id for trash flow",
    )

    trash_preview = request_json(
        method="POST",
        url=build_url(base_url, "/api/inventory/trash"),
        timeout=timeout,
        payload={
            "player_id": target_id,
            "target_type": "crafted_instance",
            "instance_id": instance_id,
        },
    )
    require(
        trash_preview.get("confirm_required") is True,
        "Trash preview should require confirmation for high-value instance",
    )
    trash_final = request_json(
        method="POST",
        url=build_url(base_url, "/api/inventory/trash"),
        timeout=timeout,
        payload={
            "player_id": target_id,
            "target_type": "crafted_instance",
            "instance_id": instance_id,
            "confirm": True,
        },
    )
    result_payload = trash_final.get("result")
    require(
        isinstance(result_payload, dict) and result_payload.get("deleted") is True,
        "Confirmed trash did not delete crafted instance",
    )

    moderation = request_json(
        method="POST",
        url=build_url(base_url, "/api/admin/players/moderate"),
        timeout=timeout,
        payload={
            "admin_player_id": "admin",
            "target_player_id": target_id,
            "action": "kick",
            "reason": "Smoke test moderation flow",
            "duration_hours": 1,
        },
    )
    moderation_payload = moderation.get("moderation")
    require(
        isinstance(moderation_payload, dict)
        and moderation_payload.get("status") in {"kicked", "suspended", "banned"}
        and moderation_payload.get("is_active") is True,
        "Admin moderation flow did not activate player restriction",
    )
    try:
        request_json(
            method="GET",
            url=build_url(base_url, "/api/economy/wallet", {"player_id": target_id}),
            timeout=timeout,
        )
        raise SmokeTestError("Expected kicked player wallet request to fail")
    except SmokeTestError as exc:
        text = str(exc).casefold()
        require(
            ("invalid or expired authorization token" in text)
            or ("is kicked" in text)
            or ("is suspended" in text)
            or ("is banned" in text),
            f"Unexpected kicked-player failure message: {exc}",
        )
    cleared = request_json(
        method="POST",
        url=build_url(base_url, "/api/admin/players/moderate"),
        timeout=timeout,
        payload={
            "admin_player_id": "admin",
            "target_player_id": target_id,
            "action": "clear",
            "reason": "Smoke cleanup",
        },
    )
    require(
        isinstance(cleared.get("cleared"), dict)
        and cleared["cleared"].get("cleared") in {True, False},
        "Admin clear moderation response missing cleared payload",
    )


def check_covert_ops_runtime(base_url: str, timeout: float) -> None:
    actor_id = f"player.smoke.covert.actor.{time.time_ns()}"
    target_id = f"player.smoke.covert.target.{time.time_ns()}"
    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=actor_id,
        captain_name="Covert Actor",
    )
    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=target_id,
        captain_name="Covert Target",
    )

    policy = request_json(
        method="GET",
        url=build_url(base_url, "/api/covert/policy", {"player_id": actor_id}),
        timeout=timeout,
    )
    ops = policy.get("ops")
    require(isinstance(ops, dict), "Covert policy endpoint missing ops object")
    require(
        all(name in ops for name in ("steal", "sabotage", "hack")),
        "Covert policy missing one or more covert op definitions",
    )

    cooldowns_before = request_json(
        method="GET",
        url=build_url(base_url, "/api/covert/cooldowns", {"player_id": actor_id}),
        timeout=timeout,
    )
    before_items = cooldowns_before.get("items")
    require(
        isinstance(before_items, list) and len(before_items) == 3,
        "Covert cooldown endpoint did not return 3 op rows",
    )

    steal = request_json(
        method="POST",
        url=build_url(base_url, "/api/covert/steal"),
        timeout=timeout,
        payload={
            "player_id": actor_id,
            "target_player_id": target_id,
            "quantity": 1,
            "seed": 331,
        },
    )
    require(steal.get("op_type") == "steal", "Covert steal response op_type mismatch")
    require(
        steal.get("status") in {"success", "failed", "blocked"},
        "Covert steal response missing valid status",
    )
    require(
        isinstance(steal.get("cooldown_after"), dict),
        "Covert steal response missing cooldown_after",
    )
    require(
        isinstance(steal.get("log"), dict) and isinstance(steal["log"].get("op_event_id"), str),
        "Covert steal response missing log payload",
    )

    sabotage = request_json(
        method="POST",
        url=build_url(base_url, "/api/covert/sabotage"),
        timeout=timeout,
        payload={
            "player_id": actor_id,
            "target_player_id": target_id,
            "seed": 332,
        },
    )
    require(sabotage.get("op_type") == "sabotage", "Covert sabotage response op_type mismatch")
    require(
        sabotage.get("status") in {"success", "failed", "blocked"},
        "Covert sabotage response missing valid status",
    )

    hack = request_json(
        method="POST",
        url=build_url(base_url, "/api/covert/hack"),
        timeout=timeout,
        payload={
            "player_id": actor_id,
            "target_player_id": target_id,
            "seed": 333,
        },
    )
    require(hack.get("op_type") == "hack", "Covert hack response op_type mismatch")
    require(
        hack.get("status") in {"success", "failed", "blocked"},
        "Covert hack response missing valid status",
    )

    logs_actor = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/covert/logs",
            {"player_id": actor_id, "perspective": "actor", "limit": 10},
        ),
        timeout=timeout,
    )
    actor_items = logs_actor.get("items")
    require(isinstance(actor_items, list), "Covert logs endpoint missing items list")
    require(
        len(actor_items) >= 3,
        "Covert logs endpoint expected at least 3 actor log rows after operations",
    )

    cooldowns_after = request_json(
        method="GET",
        url=build_url(base_url, "/api/covert/cooldowns", {"player_id": actor_id}),
        timeout=timeout,
    )
    after_items = cooldowns_after.get("items")
    require(
        isinstance(after_items, list) and len(after_items) == 3,
        "Covert cooldown endpoint (after ops) did not return 3 rows",
    )
    require(
        any(
            isinstance(row, dict)
            and isinstance(row.get("seconds_remaining"), int)
            and int(row["seconds_remaining"]) >= 0
            for row in after_items
        ),
        "Covert cooldown rows missing seconds_remaining",
    )


def check_faction_legion_governance(base_url: str, timeout: float) -> None:
    ensure_smoke_profile(base_url=base_url, timeout=timeout)
    leader_id = SMOKE_PLAYER_ID
    recruit_id = f"{SMOKE_PLAYER_ID}.recruit"
    faction_id = "faction.aurelian_compact"

    ensure_profile(
        base_url=base_url,
        timeout=timeout,
        player_id=recruit_id,
        captain_name="Legion Recruit",
    )

    aligned_leader = request_json(
        method="POST",
        url=build_url(base_url, "/api/factions/align"),
        timeout=timeout,
        payload={"player_id": leader_id, "faction_id": faction_id},
    )
    require(
        isinstance(aligned_leader.get("faction_affiliation"), dict)
        and aligned_leader["faction_affiliation"].get("faction_id") == faction_id,
        "Leader faction align failed",
    )
    aligned_recruit = request_json(
        method="POST",
        url=build_url(base_url, "/api/factions/align"),
        timeout=timeout,
        payload={"player_id": recruit_id, "faction_id": faction_id},
    )
    require(
        isinstance(aligned_recruit.get("faction_affiliation"), dict)
        and aligned_recruit["faction_affiliation"].get("faction_id") == faction_id,
        "Recruit faction align failed",
    )

    factions_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/factions", {"player_id": leader_id}),
        timeout=timeout,
    )
    faction_rows = factions_payload.get("items")
    require(isinstance(faction_rows, list) and len(faction_rows) > 0, "Factions endpoint returned no rows")
    selected = next(
        (
            row
            for row in faction_rows
            if isinstance(row, dict) and row.get("id") == faction_id
        ),
        None,
    )
    require(isinstance(selected, dict), "Aligned faction row missing from factions endpoint")
    require(bool(selected.get("is_aligned")) is True, "Aligned faction row did not set is_aligned=true")

    create_payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/create"),
        timeout=timeout,
        payload={
            "player_id": leader_id,
            "name": f"Smoke Legion {int(time.time())}",
            "tagline": "Governance first, war second.",
            "description": "Smoke-test legion for social-governance runtime verification.",
            "faction_id": faction_id,
            "visibility": "invite_only",
            "min_combat_rank": 1,
            "charter": "All strategic choices require proposal + vote audit trail.",
            "tax_rate_pct": 4.5,
        },
    )
    legion = create_payload.get("legion")
    require(isinstance(legion, dict), "Legion create did not return legion payload")
    legion_id = legion.get("legion_id")
    require(isinstance(legion_id, str) and len(legion_id.strip()) > 0, "Legion create missing legion_id")
    membership = create_payload.get("membership")
    require(
        isinstance(membership, dict) and membership.get("role") == "leader",
        "Creator was not assigned leader role",
    )

    legion_list = request_json(
        method="GET",
        url=build_url(base_url, "/api/legions", {"player_id": leader_id, "faction_id": faction_id, "limit": 60}),
        timeout=timeout,
    )
    legion_rows = legion_list.get("items")
    require(isinstance(legion_rows, list), "Legions endpoint missing items list")
    require(
        any(isinstance(row, dict) and row.get("legion_id") == legion_id for row in legion_rows),
        "Created legion missing from legions listing",
    )

    join_attempt = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/join"),
        timeout=timeout,
        payload={
            "player_id": recruit_id,
            "legion_id": legion_id,
            "message": "Requesting entry for logistics duty.",
        },
    )
    join_result = join_attempt.get("result")
    require(
        isinstance(join_result, dict) and join_result.get("mode") == "requested",
        "Recruit join to invite_only legion did not create a pending request",
    )
    request_row = join_result.get("request")
    require(
        isinstance(request_row, dict) and isinstance(request_row.get("request_id"), str),
        "Join request payload missing request_id",
    )
    request_id = str(request_row["request_id"])

    leader_requests = request_json(
        method="GET",
        url=build_url(
            base_url,
            "/api/legions/requests",
            {"player_id": leader_id, "legion_id": legion_id, "status": "pending", "limit": 40},
        ),
        timeout=timeout,
    )
    request_rows = leader_requests.get("items")
    require(isinstance(request_rows, list), "Legion requests endpoint missing items")
    require(
        any(isinstance(row, dict) and row.get("request_id") == request_id for row in request_rows),
        "Pending recruit request not visible to legion leader",
    )

    approved = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/requests/respond"),
        timeout=timeout,
        payload={"player_id": leader_id, "request_id": request_id, "decision": "approve"},
    )
    approved_result = approved.get("result")
    require(
        isinstance(approved_result, dict) and approved_result.get("decision") == "approved",
        "Join-request approval did not set decision=approved",
    )
    approved_membership = approved_result.get("membership")
    require(
        isinstance(approved_membership, dict)
        and approved_membership.get("player_id") == recruit_id
        and approved_membership.get("status") == "active",
        "Approved recruit membership payload invalid",
    )

    proposal_created = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/governance/propose"),
        timeout=timeout,
        payload={
            "player_id": leader_id,
            "legion_id": legion_id,
            "title": "Open The Gate",
            "proposal_type": "set_visibility",
            "payload": {"visibility": "open"},
            "expires_hours": 24,
        },
    )
    proposal = proposal_created.get("proposal")
    require(
        isinstance(proposal, dict) and isinstance(proposal.get("proposal_id"), str),
        "Governance proposal creation did not return proposal_id",
    )
    proposal_id = str(proposal["proposal_id"])

    vote_payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/governance/vote"),
        timeout=timeout,
        payload={"player_id": recruit_id, "proposal_id": proposal_id, "vote": "abstain"},
    )
    voted_proposal = vote_payload.get("proposal")
    require(
        isinstance(voted_proposal, dict)
        and isinstance(voted_proposal.get("votes"), dict)
        and int(voted_proposal["votes"].get("abstain", 0)) >= 1,
        "Proposal vote did not register abstain tally",
    )

    finalized = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/governance/finalize"),
        timeout=timeout,
        payload={"player_id": leader_id, "proposal_id": proposal_id},
    )
    finalized_proposal = finalized.get("proposal")
    require(
        isinstance(finalized_proposal, dict) and finalized_proposal.get("status") == "enacted",
        "Proposal finalize did not enact approved proposal",
    )

    detail_after = request_json(
        method="GET",
        url=build_url(base_url, "/api/legions/detail", {"player_id": leader_id, "legion_id": legion_id}),
        timeout=timeout,
    )
    detail_legion = detail_after.get("legion")
    require(
        isinstance(detail_legion, dict) and detail_legion.get("visibility") == "open",
        "Legion visibility did not update after enacted governance proposal",
    )

    role_set = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/members/role"),
        timeout=timeout,
        payload={
            "player_id": leader_id,
            "legion_id": legion_id,
            "target_player_id": recruit_id,
            "role": "officer",
        },
    )
    updated_member = role_set.get("member")
    require(
        isinstance(updated_member, dict)
        and updated_member.get("player_id") == recruit_id
        and updated_member.get("role") == "officer",
        "Member role update did not set recruit to officer",
    )

    left = request_json(
        method="POST",
        url=build_url(base_url, "/api/legions/leave"),
        timeout=timeout,
        payload={
            "player_id": leader_id,
            "legion_id": legion_id,
            "successor_player_id": recruit_id,
        },
    )
    left_result = left.get("result")
    require(
        isinstance(left_result, dict)
        and left_result.get("status") == "left"
        and left_result.get("legion_id") == legion_id,
        "Leader leave did not complete with successor transfer",
    )

    recruit_me = request_json(
        method="GET",
        url=build_url(base_url, "/api/legions/me", {"player_id": recruit_id}),
        timeout=timeout,
    )
    recruit_membership = recruit_me.get("legion_membership")
    require(
        isinstance(recruit_membership, dict)
        and recruit_membership.get("legion_id") == legion_id
        and recruit_membership.get("role") == "leader",
        "Successor leadership transfer failed after leader leave",
    )


def check_jwt_mode_hs256_auth(base_url: str, timeout: float) -> None:
    now_epoch = int(time.time())
    valid_token = build_hs256_jwt(
        issuer=JWT_SMOKE_ISSUER,
        audience=JWT_SMOKE_AUDIENCE,
        subject=JWT_SMOKE_SUBJECT,
        secret=JWT_SMOKE_SECRET,
        issued_at=now_epoch,
        not_before=now_epoch - 5,
        expires_at=now_epoch + 600,
    )
    expected_player_id = jwt_player_key(JWT_SMOKE_ISSUER, JWT_SMOKE_SUBJECT)
    profile_payload = request_json(
        method="POST",
        url=build_url(base_url, "/api/profile/save"),
        timeout=timeout,
        payload={
            "player_id": "client.supplied.placeholder",
            "captain_name": "JWT Smoke Captain",
            "display_name": "JWT Smoke Captain",
            "auth_mode": "guest",
            "email": "",
        },
        bearer_token=valid_token,
    )
    profile = profile_payload.get("profile")
    require(isinstance(profile, dict), "JWT mode profile/save missing profile payload")
    require(
        profile.get("player_id") == expected_player_id,
        "JWT mode did not map identity to expected stable player key",
    )
    wallet_payload = request_json(
        method="GET",
        url=build_url(base_url, "/api/economy/wallet", {"player_id": expected_player_id}),
        timeout=timeout,
        bearer_token=valid_token,
    )
    require(
        wallet_payload.get("player_id") == expected_player_id,
        "JWT mode wallet request returned wrong player id",
    )

    expired_token = build_hs256_jwt(
        issuer=JWT_SMOKE_ISSUER,
        audience=JWT_SMOKE_AUDIENCE,
        subject=JWT_SMOKE_SUBJECT,
        secret=JWT_SMOKE_SECRET,
        issued_at=now_epoch - 120,
        not_before=now_epoch - 120,
        expires_at=now_epoch - 10,
    )
    try:
        request_json(
            method="GET",
            url=build_url(base_url, "/api/economy/wallet", {"player_id": expected_player_id}),
            timeout=timeout,
            bearer_token=expired_token,
        )
        raise SmokeTestError("Expected expired JWT request to fail with HTTP 401")
    except SmokeTestError as exc:
        text = str(exc).casefold()
        require("http 401" in text, f"Expected HTTP 401 for expired JWT, got: {exc}")
        require(
            "authorization token expired" in text,
            f"Expected clear expired-token error, got: {exc}",
        )

    try:
        request_json(
            method="GET",
            url=build_url(base_url, "/api/economy/wallet", {"player_id": "player.other"}),
            timeout=timeout,
            bearer_token=valid_token,
        )
        raise SmokeTestError("Expected mismatched JWT identity request to fail with HTTP 401")
    except SmokeTestError as exc:
        text = str(exc).casefold()
        require("http 401" in text, f"Expected HTTP 401 for mismatched JWT identity, got: {exc}")
        require(
            "does not match authenticated identity" in text,
            f"Expected clear identity mismatch error, got: {exc}",
        )


def run_smoke_suite(base_url: str, timeout: float) -> None:
    checks = [
        ("health endpoint", check_health),
        ("missions endpoint with AI missions", check_missions),
        ("elements endpoint with real/lore descriptions", check_elements_descriptions),
        ("materials endpoint and category filter", check_materials),
        ("crafting substitutions endpoints", check_substitutions),
        ("crafting quote with substitution", check_crafting_quote_with_substitution),
        ("research compute and jobs endpoints", check_research_compute),
        ("research tracks endpoint", check_research_tracks),
        ("profile identity + memory endpoints", check_profile_identity_and_memory),
        ("dev player login endpoint", check_dev_player_login),
        ("combat contacts + flee flow", check_combat_contacts_and_flee),
        ("combat authoritative persisted-loadout flow", check_combat_authoritative_persisted_loadout),
        ("fair-play policy endpoint", check_fairplay_policy),
        ("economy, fleet, and unlock endpoints", check_economy_fleet_and_unlocks),
        ("market snapshot and trading endpoints", check_market_core),
        ("crafting build + asset inventory endpoints", check_crafting_build_and_assets),
        ("world claim/detail/harvest endpoints", check_world_claim_and_harvest),
        ("consumables, contracts, and job queue endpoints", check_catalogs_and_job_queues),
        ("faction/legion governance runtime endpoints", check_faction_legion_governance),
        ("advanced post-flow endpoints", check_advanced_post_flows),
        ("covert steal/sabotage/hack runtime endpoints", check_covert_ops_runtime),
        ("admin moderation + inventory controls endpoints", check_admin_inventory_controls),
    ]
    passed = 0
    for label, fn in checks:
        fn(base_url, timeout)
        passed += 1
        print(f"[PASS] {label}")
    print(f"[OK] Completed {passed} smoke checks.")


def pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def read_process_output(process: subprocess.Popen[str]) -> str:
    if process.stdout is None:
        return ""
    output = process.stdout.read().strip()
    if not output:
        return ""
    if len(output) > 4000:
        return output[-4000:]
    return output


def wait_for_server_health(
    process: subprocess.Popen[str],
    base_url: str,
    startup_timeout: float,
    request_timeout: float,
) -> None:
    deadline = time.monotonic() + startup_timeout
    last_error: str | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = read_process_output(process)
            detail = f"\nServer output:\n{output}" if output else ""
            raise SmokeTestError(
                f"Mock server exited early with code {process.returncode}.{detail}"
            )
        try:
            check_health(base_url=base_url, timeout=request_timeout)
            return
        except SmokeTestError as exc:
            last_error = str(exc)
            time.sleep(0.2)
    if last_error:
        raise SmokeTestError(
            f"Timed out waiting for server health after {startup_timeout:.1f}s: {last_error}"
        )
    raise SmokeTestError(f"Timed out waiting for server health after {startup_timeout:.1f}s")


def run_against_managed_server(args: argparse.Namespace) -> None:
    if args.port != 0 and not (1 <= args.port <= 65535):
        raise SmokeTestError("--port must be 0 or in range 1-65535")

    server_script = Path(__file__).resolve().parent / "mock_server.py"
    require(server_script.exists(), f"Unable to find server script at {server_script}")
    port = args.port or pick_free_port(args.host)
    base_url = f"http://{args.host}:{port}"

    with tempfile.TemporaryDirectory(prefix="spaceshift_smoke_") as temp_dir:
        state_db = Path(temp_dir) / "smoke_state.sqlite3"
        command = [
            sys.executable,
            str(server_script),
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
            print(f"[INFO] Running smoke checks against {base_url}")
            run_smoke_suite(base_url=base_url, timeout=args.request_timeout)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5.0)
            read_process_output(process)

    run_jwt_auth_smoke(args=args)


def run_jwt_auth_smoke(args: argparse.Namespace) -> None:
    server_script = Path(__file__).resolve().parent / "mock_server.py"
    require(server_script.exists(), f"Unable to find server script at {server_script}")
    port = pick_free_port(args.host)
    base_url = f"http://{args.host}:{port}"

    with tempfile.TemporaryDirectory(prefix="spaceshift_smoke_jwt_") as temp_dir:
        state_db = Path(temp_dir) / "smoke_jwt_state.sqlite3"
        command = [
            sys.executable,
            str(server_script),
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
                "SPACESHIFT_AUTH_MODE": "jwt",
                "SPACESHIFT_JWT_ISSUER": JWT_SMOKE_ISSUER,
                "SPACESHIFT_JWT_AUDIENCE": JWT_SMOKE_AUDIENCE,
                "SPACESHIFT_JWT_ALGORITHMS": "HS256",
                "SPACESHIFT_JWT_HS256_SECRET": JWT_SMOKE_SECRET,
                "SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN": "0",
                "SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN": "0",
                "SPACESHIFT_ENABLE_ADMIN_GOD_MODE": "0",
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
            print(f"[INFO] Running JWT auth smoke checks against {base_url}")
            check_jwt_mode_hs256_auth(base_url=base_url, timeout=args.request_timeout)
            print("[PASS] JWT auth mode HS256 checks")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5.0)
            read_process_output(process)


def run_against_existing_server(args: argparse.Namespace) -> None:
    require(isinstance(args.base_url, str) and args.base_url.strip(), "--base-url is required")
    base_url = args.base_url.strip().rstrip("/")
    print(f"[INFO] Running smoke checks against existing server {base_url}")
    run_smoke_suite(base_url=base_url, timeout=args.request_timeout)


def main() -> int:
    args = parse_args()
    try:
        if args.base_url:
            run_against_existing_server(args)
        else:
            run_against_managed_server(args)
    except SmokeTestError as exc:
        print(f"[FAIL] {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
