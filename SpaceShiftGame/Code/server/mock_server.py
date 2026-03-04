#!/usr/bin/env python3
"""Minimal stdlib mock backend for SpaceShift seed data."""

from __future__ import annotations

import argparse
import base64
import binascii
import calendar
import hashlib
import hmac
import json
import logging
import math
import os
import random
import secrets
import sqlite3
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    psycopg = None
    dict_row = None

DEFAULT_LIMIT = 20
MAX_LIMIT = 200
DEFAULT_COMBAT_ROUNDS = 6
MAX_COMBAT_ROUNDS = 30
DEFAULT_SCAN_COUNT = 6
MAX_SCAN_COUNT = 24
DEFAULT_SCAN_POWER = 100.0
MAX_SCAN_POWER = 500.0
DEFAULT_MARKET_LIMIT = 24
MAX_MARKET_LIMIT = 118
STARTING_CREDITS = 250_000.0
STARTING_VOIDCOIN = 120.0
DEFAULT_ACTION_ENERGY_MAX = 240.0
DEFAULT_ACTION_ENERGY_REGEN_PER_HOUR = 15.0
ENERGY_COST_DISCOVERY_SCAN = 8.0
ENERGY_COST_WORLD_HARVEST = 4.0
ENERGY_COST_COMBAT_ENGAGE = 12.0
ENERGY_COST_COMBAT_AUTO_RESOLVE = 9.0
ENERGY_COST_COVERT_STEAL = 7.0
ENERGY_COST_COVERT_SABOTAGE = 11.0
ENERGY_COST_COVERT_HACK = 9.0
COVERT_STEAL_COOLDOWN_SECONDS = 15 * 60
COVERT_SABOTAGE_COOLDOWN_SECONDS = 25 * 60
COVERT_HACK_COOLDOWN_SECONDS = 20 * 60
EQUIPMENT_M3_PER_DECK_POINT = 4.0
HABITABLE_M3_PER_CREW = 14.0
HABITABLE_M3_PER_PASSENGER = 8.0
CARGO_M3_PER_TON = 1.0
BASE_PERSONAL_STORAGE_SLOTS = 48
BASE_SMUGGLE_STORAGE_SLOTS = 6
ADMIN_GOD_CREDITS = 10_000_000_000_000.0
ADMIN_GOD_VOIDCOIN = 100_000_000.0
ADMIN_GOD_ELEMENT_FLOOR = 10_000_000.0
DEFAULT_HIGH_RISK_VISIBILITY_THRESHOLD = 0.75
MAX_COMBAT_REWARD_SCALE = 3.75
DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 12_000
DEFAULT_SQLITE_WAL_AUTOCHECKPOINT_PAGES = 1000
DEFAULT_SQLITE_SYNCHRONOUS = "NORMAL"
SQLITE_SYNCHRONOUS_VALUES = {"OFF", "NORMAL", "FULL", "EXTRA"}
SQLITE_JOURNAL_MODE_VALUES = {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
DEFAULT_DB_BACKEND = "sqlite"
DB_BACKEND_VALUES = {"sqlite", "postgres"}
DEFAULT_POSTGRES_CONNECT_RETRIES = 3
DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS = 2
DEFAULT_POSTGRES_RETRY_DELAY_SECONDS = 0.35
DEFAULT_AUTH_MODE = "local"
AUTH_MODE_VALUES = {"local", "jwt"}
JWT_SUPPORTED_ALGORITHMS = {"HS256", "RS256"}
DEFAULT_JWT_ALGORITHMS = ("RS256",)
DEFAULT_JWKS_CACHE_SECONDS = 300
JWT_RS256_SHA256_DIGESTINFO_PREFIX = bytes.fromhex(
    "3031300d060960864801650304020105000420"
)

STAT_KEYS = ("attack", "defense", "hull", "shield", "energy", "scan", "cloak")
DAMAGE_TYPES = ("kinetic", "thermal", "explosive", "plasma", "ion")
LEGION_VISIBILITY_VALUES = {"open", "invite_only", "closed"}
LEGION_MEMBER_ROLE_VALUES = {"leader", "officer", "member"}
LEGION_MEMBER_STATUS_VALUES = {"active", "left", "kicked"}
LEGION_REQUEST_STATUS_VALUES = {"pending", "approved", "rejected", "cancelled"}
LEGION_PROPOSAL_STATUS_VALUES = {"open", "enacted", "rejected", "cancelled", "expired"}
LEGION_PROPOSAL_TYPE_VALUES = {
    "set_visibility",
    "set_min_combat_rank",
    "set_faction_alignment",
    "update_charter",
    "set_tax_rate_pct",
}
LEGION_VOTE_VALUES = {"yes", "no", "abstain"}


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().casefold() in {"1", "true", "yes", "on"}


def env_nonnegative_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    value = raw.strip()
    if not value:
        return int(default)
    try:
        parsed = int(value, 10)
    except (TypeError, ValueError):
        return int(default)
    return parsed if parsed >= 0 else int(default)


def env_choice(name: str, default: str, allowed_values: set[str]) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().upper()
    if not value:
        return default
    return value if value in allowed_values else default


def env_casefold_choice(name: str, default: str, allowed_values: set[str]) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().casefold()
    if not value:
        return default
    return value if value in allowed_values else default


def env_upper_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None:
        return tuple(default)
    tokens = [token.strip().upper() for token in raw.split(",") if token.strip()]
    if not tokens:
        return tuple(default)
    return tuple(tokens)


def base64url_decode(raw: str, *, label: str) -> bytes:
    if not isinstance(raw, str) or not raw.strip():
        raise AuthError(f"Authorization token {label} segment is missing")
    text = raw.strip()
    padded = text + ("=" * ((4 - (len(text) % 4)) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
        raise AuthError(f"Authorization token {label} segment is invalid") from exc


def stable_hash_int(*parts: Any) -> int:
    digest = hashlib.sha256(
        "|".join(str(part) for part in parts).encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


DETERMINISTIC_MODE = env_flag("SPACESHIFT_DETERMINISTIC", default=False)

STARTER_INVENTORY = {
    "Fe": 650.0,
    "Al": 500.0,
    "Si": 480.0,
    "C": 420.0,
    "H": 700.0,
    "O": 620.0,
    "Cu": 220.0,
    "Ni": 160.0,
    "Ti": 90.0,
    "Mg": 180.0,
    "S": 150.0,
    "AIR": 18000.0,
    "H2O": 16000.0,
    "FOOD": 13000.0,
}

LIFE_SUPPORT_SYMBOLS = ("AIR", "H2O", "FOOD")
LIFE_SUPPORT_MAX_TICK_HOURS = 24.0 * 45.0

RARE_ELEMENT_SYMBOLS = {
    "Pt",
    "Pd",
    "Ir",
    "Au",
    "U",
    "Th",
    "Re",
    "W",
    "Rh",
    "Ru",
    "Os",
    "Hf",
    "Ta",
}

COMMON_ELEMENT_SYMBOLS = {
    "H",
    "He",
    "C",
    "N",
    "O",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "K",
    "Ca",
    "Fe",
}

ASSET_STACK_SIZE_BY_TYPE = {
    "module": 6,
    "hull": 1,
    "blueprint": 10,
    "artifact": 2,
    "consumable": 12,
    "structure": 2,
    "tech": 20,
}

WARNING_INSTANCE_QUALITY_TIERS = {"elite", "prototype", "legendary", "mythic", "rare"}
WARNING_ELEMENT_SYMBOLS = {
    "Au", "Pt", "Pd", "Rh", "Ir", "Os", "Ru", "Re", "W", "U", "Th", "Np", "Pu", "Am"
}


class SeedDataError(RuntimeError):
    """Raised when seed files are missing or malformed."""


class StateStoreError(RuntimeError):
    """Raised when persistent state operations fail."""


class AuthError(RuntimeError):
    """Raised when request authentication fails."""


class SeedStore:
    """In-memory representation of seed files used by the mock API."""

    def __init__(self, seeds_dir: Path) -> None:
        self.seeds_dir = seeds_dir
        self.missions: list[dict[str, Any]] = []
        self.modules: list[dict[str, Any]] = []
        self.ship_hulls: list[dict[str, Any]] = []
        self.starter_ships: list[dict[str, Any]] = []
        self.tech_tree: list[dict[str, Any]] = []
        self.research_tracks: list[dict[str, Any]] = []
        self.market_regions: list[dict[str, Any]] = []
        self.contract_templates: list[dict[str, Any]] = []
        self.ai_opponents: list[dict[str, Any]] = []
        self.consumables: list[dict[str, Any]] = []
        self.reverse_engineering_recipes: list[dict[str, Any]] = []
        self.manufacturing_profiles: list[dict[str, Any]] = []
        self.p2p_listing_policy: dict[str, Any] = {}
        self.races: list[dict[str, Any]] = []
        self.factions: list[dict[str, Any]] = []
        self.professions: list[dict[str, Any]] = []
        self.abilities: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []
        self.blueprints: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []
        self.planet_types: list[dict[str, Any]] = []
        self.elements: list[dict[str, Any]] = []
        self.celestial_templates: list[dict[str, Any]] = []
        self.structures: list[dict[str, Any]] = []
        self.lore_codex: list[dict[str, Any]] = []
        self.materials: list[dict[str, Any]] = []
        self.crafting_substitutions: list[dict[str, Any]] = []
        self.manifest: dict[str, Any] = {}

    @classmethod
    def load(cls, seeds_dir: Path) -> "SeedStore":
        store = cls(seeds_dir)
        store.missions = store._load_array("missions.json")
        store.modules = store._load_array("modules.json")
        store.ship_hulls = store._load_array("ship_hulls.json")
        store.starter_ships = store._load_array_optional("starter_ships.json")
        store.tech_tree = store._load_array("tech_tree_core.json")
        store.research_tracks = store._load_array("research_tracks.json")
        store.market_regions = store._load_array("market_regions.json")
        store.contract_templates = store._load_array("contract_templates.json")
        store.ai_opponents = store._load_array("ai_opponents.json")
        store.consumables = store._load_array("consumables.json")
        store.reverse_engineering_recipes = store._load_array("reverse_engineering_recipes.json")
        store.manufacturing_profiles = store._load_array("manufacturing_profiles.json")
        store.p2p_listing_policy = store._load_object("p2p_listing_policy.json")
        store.races = store._load_array("races.json")
        store.factions = store._load_array("factions.json")
        store.professions = store._load_array("professions.json")
        store.abilities = store._load_array("abilities.json")
        store.artifacts = store._load_array("artifacts.json")
        store.blueprints = store._load_array("blueprints.json")
        store.events = store._load_array("events.json")
        store.planet_types = store._load_array("planet_types.json")
        store.elements = store._load_array("elements_full.json")
        store.celestial_templates = store._load_array("celestial_templates.json")
        store.structures = store._load_array("structures.json")
        store.lore_codex = store._load_array("lore_codex.json")
        store.materials = store._load_array("materials.json")
        store.crafting_substitutions = store._load_array("crafting_substitutions.json")
        store.manifest = store._load_object("content_manifest.json")
        return store

    def _load_array(self, file_name: str) -> list[dict[str, Any]]:
        payload = self._load_json(file_name)
        if not isinstance(payload, list):
            raise SeedDataError(f"Expected list in {file_name}, got {type(payload).__name__}")
        return payload

    def _load_array_optional(self, file_name: str) -> list[dict[str, Any]]:
        file_path = self.seeds_dir / file_name
        if not file_path.exists():
            return []
        return self._load_array(file_name)

    def _load_object(self, file_name: str) -> dict[str, Any]:
        payload = self._load_json(file_name)
        if not isinstance(payload, dict):
            raise SeedDataError(f"Expected object in {file_name}, got {type(payload).__name__}")
        return payload

    def _load_json(self, file_name: str) -> Any:
        file_path = self.seeds_dir / file_name
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError as exc:
            raise SeedDataError(f"Seed file not found: {file_path}") from exc
        except json.JSONDecodeError as exc:
            raise SeedDataError(f"Invalid JSON in {file_path}: {exc}") from exc
        except OSError as exc:
            raise SeedDataError(f"Unable to read {file_path}: {exc}") from exc

    def module_families(self) -> list[str]:
        families = {
            module.get("family")
            for module in self.modules
            if isinstance(module, dict) and isinstance(module.get("family"), str)
        }
        return sorted(families)

    def elements_by_symbol(self) -> dict[str, dict[str, Any]]:
        return {
            element["symbol"]: element
            for element in self.elements
            if isinstance(element, dict) and isinstance(element.get("symbol"), str)
        }

    def module_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.modules
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def hull_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.ship_hulls
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def structure_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.structures
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def tech_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.tech_tree
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def substitution_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.crafting_substitutions
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def consumable_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.consumables
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def reverse_recipe_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.reverse_engineering_recipes
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def contract_template_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.contract_templates
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def faction_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.factions
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def region_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.market_regions
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def race_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.races
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def profession_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.professions
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def substitutions_for_item(self, item_id: str) -> list[dict[str, Any]]:
        return [
            row
            for row in self.crafting_substitutions
            if isinstance(row, dict)
            and isinstance(row.get("item_id"), str)
            and row["item_id"] == item_id
        ]


def _rewrite_qmark_placeholders(sql: str) -> str:
    output: list[str] = []
    in_single_quote = False
    in_double_quote = False
    index = 0
    while index < len(sql):
        char = sql[index]
        if char == "'" and not in_double_quote:
            output.append(char)
            if in_single_quote and index + 1 < len(sql) and sql[index + 1] == "'":
                output.append("'")
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue
        if char == '"' and not in_single_quote:
            output.append(char)
            if in_double_quote and index + 1 < len(sql) and sql[index + 1] == '"':
                output.append('"')
                index += 2
                continue
            in_double_quote = not in_double_quote
            index += 1
            continue
        if char == "?" and not in_single_quote and not in_double_quote:
            output.append("%s")
        else:
            output.append(char)
        index += 1
    return "".join(output)


def _append_on_conflict_do_nothing(sql: str) -> str:
    stripped = sql.rstrip()
    suffix = sql[len(stripped) :]
    if stripped.endswith(";"):
        body = stripped[:-1].rstrip()
        return f"{body} ON CONFLICT DO NOTHING;{suffix}"
    return f"{stripped} ON CONFLICT DO NOTHING{suffix}"


def _rewrite_sql_for_postgres(sql: str) -> str:
    rewritten = _rewrite_qmark_placeholders(sql)
    upper = rewritten.upper()
    marker = "INSERT OR IGNORE INTO"
    if marker in upper:
        marker_index = upper.index(marker)
        rewritten = (
            f"{rewritten[:marker_index]}INSERT INTO{rewritten[marker_index + len(marker):]}"
        )
        if "ON CONFLICT" not in rewritten.upper():
            rewritten = _append_on_conflict_do_nothing(rewritten)
    return rewritten


def _normalize_sql_params(params: Any | None) -> tuple[Any, ...] | None:
    if params is None:
        return None
    if isinstance(params, tuple):
        return params
    if isinstance(params, list):
        return tuple(params)
    return (params,)


class PostgresCompatConnection:
    """Small adapter so sqlite-style query calls keep working on psycopg."""

    def __init__(self, raw_conn: Any) -> None:
        self._raw_conn = raw_conn

    def __enter__(self) -> "PostgresCompatConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        try:
            if exc_type is None:
                self._raw_conn.commit()
            else:
                self._raw_conn.rollback()
        finally:
            self._raw_conn.close()
        return False

    def __getattr__(self, attr_name: str) -> Any:
        return getattr(self._raw_conn, attr_name)

    def execute(self, sql: str, params: Any | None = None) -> Any:
        statement = _rewrite_sql_for_postgres(sql)
        normalized_params = _normalize_sql_params(params)
        try:
            if normalized_params is None:
                return self._raw_conn.execute(statement)
            return self._raw_conn.execute(statement, normalized_params)
        except Exception as exc:
            if psycopg is not None and isinstance(exc, psycopg.IntegrityError):
                raise sqlite3.IntegrityError(str(exc)) from exc
            raise

    def executescript(self, script: str) -> None:
        for statement in script.split(";"):
            cleaned = statement.strip()
            if not cleaned:
                continue
            self.execute(cleaned)

    def commit(self) -> None:
        self._raw_conn.commit()

    def rollback(self) -> None:
        self._raw_conn.rollback()


class PersistentState:
    """Runtime state for profiles, worlds, economy, and player progression."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_backend = env_casefold_choice(
            "SPACESHIFT_DB_BACKEND",
            default=DEFAULT_DB_BACKEND,
            allowed_values=DB_BACKEND_VALUES,
        )
        self._postgres_dsn = os.getenv("SPACESHIFT_POSTGRES_DSN", "").strip()
        self._sqlite_busy_timeout_ms = max(
            1_000,
            env_nonnegative_int(
                "SPACESHIFT_SQLITE_BUSY_TIMEOUT_MS",
                default=DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
            ),
        )
        self._sqlite_wal_autocheckpoint_pages = max(
            200,
            env_nonnegative_int(
                "SPACESHIFT_SQLITE_WAL_AUTOCHECKPOINT_PAGES",
                default=DEFAULT_SQLITE_WAL_AUTOCHECKPOINT_PAGES,
            ),
        )
        self._sqlite_synchronous = env_choice(
            "SPACESHIFT_SQLITE_SYNCHRONOUS",
            default=DEFAULT_SQLITE_SYNCHRONOUS,
            allowed_values=SQLITE_SYNCHRONOUS_VALUES,
        )
        self._sqlite_journal_mode = env_choice(
            "SPACESHIFT_SQLITE_JOURNAL_MODE",
            default="WAL",
            allowed_values=SQLITE_JOURNAL_MODE_VALUES,
        )
        self._lock = Lock()
        self._init_schema()

    def _connect(self) -> Any:
        if self.db_backend == "postgres":
            return self._connect_postgres()
        return self._connect_sqlite()

    def _connect_sqlite(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=self._sqlite_busy_timeout_ms / 1000.0,
            check_same_thread=False,
            isolation_level="IMMEDIATE",
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {self._sqlite_busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA journal_mode = {self._sqlite_journal_mode}")
        conn.execute(f"PRAGMA synchronous = {self._sqlite_synchronous}")
        conn.execute(
            f"PRAGMA wal_autocheckpoint = {self._sqlite_wal_autocheckpoint_pages}"
        )
        conn.execute("PRAGMA temp_store = MEMORY")
        return conn

    def _connect_postgres(self) -> PostgresCompatConnection:
        if psycopg is None or dict_row is None:
            raise OSError(
                "SPACESHIFT_DB_BACKEND=postgres requires psycopg. "
                "Install dependencies from Code/server/requirements.txt."
            )
        if not self._postgres_dsn:
            raise OSError(
                "SPACESHIFT_POSTGRES_DSN is required when SPACESHIFT_DB_BACKEND=postgres."
            )

        last_error: Exception | None = None
        for attempt in range(1, DEFAULT_POSTGRES_CONNECT_RETRIES + 1):
            try:
                conn = psycopg.connect(
                    self._postgres_dsn,
                    connect_timeout=DEFAULT_POSTGRES_CONNECT_TIMEOUT_SECONDS,
                    autocommit=False,
                    row_factory=dict_row,
                )
                conn.execute("SET TIME ZONE 'UTC'")
                return PostgresCompatConnection(conn)
            except Exception as exc:
                last_error = exc
                if attempt >= DEFAULT_POSTGRES_CONNECT_RETRIES:
                    break
                delay_seconds = DEFAULT_POSTGRES_RETRY_DELAY_SECONDS * attempt
                logging.warning(
                    "Postgres connect attempt %d/%d failed (%s). Retrying in %.2fs",
                    attempt,
                    DEFAULT_POSTGRES_CONNECT_RETRIES,
                    exc,
                    delay_seconds,
                )
                time.sleep(delay_seconds)

        raise OSError(
            "Failed to connect to Postgres using SPACESHIFT_POSTGRES_DSN after "
            f"{DEFAULT_POSTGRES_CONNECT_RETRIES} attempts: {last_error}"
        )

    def _init_schema(self) -> None:
        if self.db_backend == "sqlite":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with self._connect() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS profiles (
                      player_id TEXT PRIMARY KEY,
                      captain_name TEXT NOT NULL,
                      display_name TEXT NOT NULL,
                      auth_mode TEXT NOT NULL,
                      email TEXT NOT NULL,
                      race_id TEXT,
                      profession_id TEXT,
                      starting_ship_id TEXT,
                      tutorial_mode TEXT,
                      planet_type_id TEXT,
                      player_memory_json TEXT NOT NULL,
                      updated_utc TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS player_pvp_settings (
                      player_id TEXT PRIMARY KEY,
                      allow_high_risk_visibility INTEGER NOT NULL DEFAULT 0,
                      high_risk_loss_threshold REAL NOT NULL DEFAULT 0.75,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS player_combat_progress (
                      player_id TEXT PRIMARY KEY,
                      combat_xp REAL NOT NULL DEFAULT 0,
                      combat_rank INTEGER NOT NULL DEFAULT 1,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS player_action_energy (
                      player_id TEXT PRIMARY KEY,
                      current_energy REAL NOT NULL DEFAULT 240,
                      max_energy REAL NOT NULL DEFAULT 240,
                      regen_per_hour REAL NOT NULL DEFAULT 15,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS player_life_support_state (
                      player_id TEXT PRIMARY KEY,
                      last_tick_utc TEXT NOT NULL,
                      deficit_air REAL NOT NULL DEFAULT 0,
                      deficit_water REAL NOT NULL DEFAULT 0,
                      deficit_food REAL NOT NULL DEFAULT 0,
                      shortage_stress REAL NOT NULL DEFAULT 0,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS claimed_worlds (
                      world_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      body_class TEXT NOT NULL,
                      world_name TEXT NOT NULL,
                      payload_json TEXT NOT NULL,
                      discovered_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS claimed_worlds_player_idx
                      ON claimed_worlds(player_id);

                    CREATE TABLE IF NOT EXISTS discovered_bodies (
                      world_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      payload_json TEXT NOT NULL,
                      discovered_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS discovered_bodies_player_idx
                      ON discovered_bodies(player_id, updated_utc DESC);

                    CREATE TABLE IF NOT EXISTS world_structures (
                      world_id TEXT NOT NULL,
                      structure_id TEXT NOT NULL,
                      built_utc TEXT NOT NULL,
                      PRIMARY KEY (world_id, structure_id),
                      FOREIGN KEY(world_id) REFERENCES claimed_worlds(world_id)
                    );

                    CREATE TABLE IF NOT EXISTS wallets (
                      player_id TEXT PRIMARY KEY,
                      credits REAL NOT NULL,
                      voidcoin REAL NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS element_inventory (
                      player_id TEXT NOT NULL,
                      symbol TEXT NOT NULL,
                      amount REAL NOT NULL,
                      updated_utc TEXT NOT NULL,
                      PRIMARY KEY (player_id, symbol),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS element_inventory_player_idx
                      ON element_inventory(player_id);

                    CREATE TABLE IF NOT EXISTS player_assets (
                      player_id TEXT NOT NULL,
                      asset_type TEXT NOT NULL,
                      asset_id TEXT NOT NULL,
                      quantity INTEGER NOT NULL,
                      updated_utc TEXT NOT NULL,
                      PRIMARY KEY (player_id, asset_type, asset_id),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS player_assets_player_idx
                      ON player_assets(player_id);

                    CREATE TABLE IF NOT EXISTS crafted_instances (
                      instance_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      asset_type TEXT NOT NULL,
                      asset_id TEXT NOT NULL,
                      quality_tier TEXT NOT NULL,
                      quality_score REAL NOT NULL,
                      stat_multiplier REAL NOT NULL,
                      payload_json TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS crafted_instances_player_idx
                      ON crafted_instances(player_id, asset_type, asset_id);

                    CREATE TABLE IF NOT EXISTS smuggled_assets (
                      player_id TEXT NOT NULL,
                      asset_type TEXT NOT NULL,
                      asset_id TEXT NOT NULL,
                      quantity INTEGER NOT NULL,
                      updated_utc TEXT NOT NULL,
                      PRIMARY KEY (player_id, asset_type, asset_id),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS smuggled_assets_player_idx
                      ON smuggled_assets(player_id);

                    CREATE TABLE IF NOT EXISTS player_storage_upgrades (
                      player_id TEXT PRIMARY KEY,
                      personal_slots_bonus REAL NOT NULL DEFAULT 0,
                      smuggle_slots_bonus REAL NOT NULL DEFAULT 0,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS player_moderation (
                      player_id TEXT PRIMARY KEY,
                      status TEXT NOT NULL,
                      reason TEXT NOT NULL,
                      imposed_by_player_id TEXT NOT NULL,
                      imposed_utc TEXT NOT NULL,
                      expires_utc TEXT,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id),
                      FOREIGN KEY(imposed_by_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS player_moderation_status_idx
                      ON player_moderation(status, updated_utc DESC);

                    CREATE TABLE IF NOT EXISTS admin_action_log (
                      action_id TEXT PRIMARY KEY,
                      admin_player_id TEXT NOT NULL,
                      action_type TEXT NOT NULL,
                      target_player_id TEXT,
                      payload_json TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      FOREIGN KEY(admin_player_id) REFERENCES profiles(player_id),
                      FOREIGN KEY(target_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS admin_action_log_admin_idx
                      ON admin_action_log(admin_player_id, created_utc DESC);

                    CREATE INDEX IF NOT EXISTS admin_action_log_target_idx
                      ON admin_action_log(target_player_id, created_utc DESC);

                    CREATE TABLE IF NOT EXISTS covert_op_cooldowns (
                      player_id TEXT NOT NULL,
                      op_type TEXT NOT NULL,
                      next_ready_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      PRIMARY KEY (player_id, op_type),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS covert_op_cooldowns_next_idx
                      ON covert_op_cooldowns(op_type, next_ready_utc);

                    CREATE TABLE IF NOT EXISTS covert_op_log (
                      op_event_id TEXT PRIMARY KEY,
                      actor_player_id TEXT NOT NULL,
                      target_player_id TEXT NOT NULL,
                      op_type TEXT NOT NULL,
                      status TEXT NOT NULL,
                      success_probability REAL NOT NULL,
                      detection_probability REAL NOT NULL,
                      outcome_json TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      FOREIGN KEY(actor_player_id) REFERENCES profiles(player_id),
                      FOREIGN KEY(target_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS covert_op_log_actor_idx
                      ON covert_op_log(actor_player_id, created_utc DESC);

                    CREATE INDEX IF NOT EXISTS covert_op_log_target_idx
                      ON covert_op_log(target_player_id, created_utc DESC);

                    CREATE TABLE IF NOT EXISTS research_unlocks (
                      player_id TEXT NOT NULL,
                      tech_id TEXT NOT NULL,
                      unlocked_utc TEXT NOT NULL,
                      PRIMARY KEY (player_id, tech_id),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS research_unlocks_player_idx
                      ON research_unlocks(player_id);

                    CREATE TABLE IF NOT EXISTS research_jobs (
                      job_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      tech_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      required_compute REAL NOT NULL,
                      compute_power_per_hour REAL NOT NULL,
                      duration_seconds INTEGER NOT NULL,
                      started_utc TEXT NOT NULL,
                      completes_utc TEXT NOT NULL,
                      substitution_id TEXT,
                      cost_json TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS research_jobs_player_idx
                      ON research_jobs(player_id);

                    CREATE TABLE IF NOT EXISTS fleet_state (
                      player_id TEXT PRIMARY KEY,
                      active_hull_id TEXT,
                      hull_durability REAL NOT NULL,
                      ship_level INTEGER NOT NULL DEFAULT 1,
                      ship_xp REAL NOT NULL DEFAULT 0,
                      crew_total REAL NOT NULL,
                      crew_elite REAL NOT NULL,
                      cargo_json TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS manufacturing_jobs (
                      job_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      item_id TEXT NOT NULL,
                      quantity INTEGER NOT NULL,
                      status TEXT NOT NULL,
                      profile_id TEXT NOT NULL,
                      workload REAL NOT NULL,
                      throughput_per_hour REAL NOT NULL,
                      duration_seconds INTEGER NOT NULL,
                      started_utc TEXT NOT NULL,
                      completes_utc TEXT NOT NULL,
                      world_id TEXT,
                      substitution_id TEXT,
                      cost_json TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS manufacturing_jobs_player_idx
                      ON manufacturing_jobs(player_id);

                    CREATE TABLE IF NOT EXISTS reverse_jobs (
                      job_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      recipe_id TEXT NOT NULL,
                      target_item_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      compute_cost REAL NOT NULL,
                      duration_seconds INTEGER NOT NULL,
                      started_utc TEXT NOT NULL,
                      completes_utc TEXT NOT NULL,
                      consumable_id TEXT NOT NULL,
                      unlock_blueprint_id TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS reverse_jobs_player_idx
                      ON reverse_jobs(player_id);

                    CREATE TABLE IF NOT EXISTS market_listings (
                      listing_id TEXT PRIMARY KEY,
                      seller_player_id TEXT NOT NULL,
                      asset_type TEXT NOT NULL,
                      asset_id TEXT NOT NULL,
                      quantity REAL NOT NULL,
                      quantity_remaining REAL NOT NULL,
                      currency TEXT NOT NULL,
                      unit_price REAL NOT NULL,
                      region_id TEXT,
                      status TEXT NOT NULL,
                      expires_utc TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      metadata_json TEXT NOT NULL DEFAULT '{}',
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(seller_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS market_listings_lookup_idx
                      ON market_listings(asset_type, asset_id, status, region_id);

                    CREATE INDEX IF NOT EXISTS market_listings_seller_idx
                      ON market_listings(seller_player_id, status);

                    CREATE TABLE IF NOT EXISTS market_trade_log (
                      trade_id TEXT PRIMARY KEY,
                      trade_source TEXT NOT NULL,
                      buyer_player_id TEXT,
                      seller_player_id TEXT,
                      asset_type TEXT NOT NULL,
                      asset_id TEXT NOT NULL,
                      quantity REAL NOT NULL,
                      currency TEXT NOT NULL,
                      unit_price REAL NOT NULL,
                      gross_total REAL NOT NULL,
                      maker_fee REAL NOT NULL,
                      taker_fee REAL NOT NULL,
                      region_id TEXT,
                      listing_id TEXT,
                      metadata_json TEXT NOT NULL DEFAULT '{}',
                      created_utc TEXT NOT NULL,
                      FOREIGN KEY(buyer_player_id) REFERENCES profiles(player_id),
                      FOREIGN KEY(seller_player_id) REFERENCES profiles(player_id),
                      FOREIGN KEY(listing_id) REFERENCES market_listings(listing_id)
                    );

                    CREATE INDEX IF NOT EXISTS market_trade_log_asset_idx
                      ON market_trade_log(asset_type, asset_id, created_utc DESC);

                    CREATE INDEX IF NOT EXISTS market_trade_log_time_idx
                      ON market_trade_log(created_utc DESC);

                    CREATE TABLE IF NOT EXISTS player_contracts (
                      contract_job_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      template_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      assigned_utc TEXT NOT NULL,
                      expires_utc TEXT NOT NULL,
                      progress_value REAL NOT NULL,
                      objective_target REAL NOT NULL,
                      payload_json TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS player_contracts_player_idx
                      ON player_contracts(player_id, status);

                    CREATE TABLE IF NOT EXISTS player_missions (
                      mission_job_id TEXT PRIMARY KEY,
                      player_id TEXT NOT NULL,
                      mission_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      accepted_utc TEXT NOT NULL,
                      progress_value REAL NOT NULL,
                      objective_target REAL NOT NULL,
                      payload_json TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS player_missions_player_idx
                      ON player_missions(player_id, status);

                    CREATE TABLE IF NOT EXISTS player_battle_metrics (
                      player_id TEXT PRIMARY KEY,
                      battles_won INTEGER NOT NULL DEFAULT 0,
                      battles_lost INTEGER NOT NULL DEFAULT 0,
                      battles_fled INTEGER NOT NULL DEFAULT 0,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE TABLE IF NOT EXISTS player_faction_affiliations (
                      player_id TEXT PRIMARY KEY,
                      faction_id TEXT NOT NULL,
                      standing REAL NOT NULL DEFAULT 0,
                      role TEXT NOT NULL DEFAULT 'member',
                      joined_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS player_faction_affiliations_faction_idx
                      ON player_faction_affiliations(faction_id);

                    CREATE TABLE IF NOT EXISTS legions (
                      legion_id TEXT PRIMARY KEY,
                      name TEXT NOT NULL UNIQUE,
                      tagline TEXT NOT NULL,
                      description TEXT NOT NULL,
                      faction_id TEXT,
                      visibility TEXT NOT NULL,
                      min_combat_rank INTEGER NOT NULL,
                      owner_player_id TEXT NOT NULL,
                      policy_json TEXT NOT NULL,
                      treasury_credits REAL NOT NULL DEFAULT 0,
                      created_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(owner_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS legions_faction_idx
                      ON legions(faction_id, visibility, updated_utc DESC);

                    CREATE TABLE IF NOT EXISTS legion_members (
                      legion_id TEXT NOT NULL,
                      player_id TEXT NOT NULL,
                      role TEXT NOT NULL,
                      status TEXT NOT NULL,
                      joined_utc TEXT NOT NULL,
                      contribution_score REAL NOT NULL DEFAULT 0,
                      updated_utc TEXT NOT NULL,
                      PRIMARY KEY (legion_id, player_id),
                      FOREIGN KEY(legion_id) REFERENCES legions(legion_id),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS legion_members_player_idx
                      ON legion_members(player_id, status, updated_utc DESC);

                    CREATE TABLE IF NOT EXISTS legion_join_requests (
                      request_id TEXT PRIMARY KEY,
                      legion_id TEXT NOT NULL,
                      player_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      message TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(legion_id) REFERENCES legions(legion_id),
                      FOREIGN KEY(player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS legion_join_requests_legion_idx
                      ON legion_join_requests(legion_id, status, updated_utc DESC);

                    CREATE INDEX IF NOT EXISTS legion_join_requests_player_idx
                      ON legion_join_requests(player_id, status, updated_utc DESC);

                    CREATE TABLE IF NOT EXISTS legion_governance_proposals (
                      proposal_id TEXT PRIMARY KEY,
                      legion_id TEXT NOT NULL,
                      proposer_player_id TEXT NOT NULL,
                      title TEXT NOT NULL,
                      proposal_type TEXT NOT NULL,
                      payload_json TEXT NOT NULL,
                      status TEXT NOT NULL,
                      required_yes_votes INTEGER NOT NULL,
                      expires_utc TEXT NOT NULL,
                      resolution_json TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      updated_utc TEXT NOT NULL,
                      FOREIGN KEY(legion_id) REFERENCES legions(legion_id),
                      FOREIGN KEY(proposer_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS legion_governance_proposals_legion_idx
                      ON legion_governance_proposals(legion_id, status, updated_utc DESC);

                    CREATE TABLE IF NOT EXISTS legion_governance_votes (
                      proposal_id TEXT NOT NULL,
                      voter_player_id TEXT NOT NULL,
                      vote TEXT NOT NULL,
                      weight REAL NOT NULL DEFAULT 1,
                      cast_utc TEXT NOT NULL,
                      PRIMARY KEY (proposal_id, voter_player_id),
                      FOREIGN KEY(proposal_id) REFERENCES legion_governance_proposals(proposal_id),
                      FOREIGN KEY(voter_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS legion_governance_votes_proposal_idx
                      ON legion_governance_votes(proposal_id, vote);

                    CREATE TABLE IF NOT EXISTS legion_event_log (
                      event_id TEXT PRIMARY KEY,
                      legion_id TEXT NOT NULL,
                      actor_player_id TEXT,
                      event_type TEXT NOT NULL,
                      payload_json TEXT NOT NULL,
                      created_utc TEXT NOT NULL,
                      FOREIGN KEY(legion_id) REFERENCES legions(legion_id),
                      FOREIGN KEY(actor_player_id) REFERENCES profiles(player_id)
                    );

                    CREATE INDEX IF NOT EXISTS legion_event_log_legion_idx
                      ON legion_event_log(legion_id, created_utc DESC);
                    """
                )
                self._ensure_profiles_schema_columns(conn)
                self._ensure_fleet_schema_columns(conn)
                self._ensure_market_listing_schema_columns(conn)
                conn.commit()

    def _load_existing_column_names(self, conn: Any, table_name: str) -> set[str]:
        if self.db_backend == "sqlite":
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        else:
            rows = conn.execute(
                """
                SELECT column_name AS name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ?
                """,
                (table_name,),
            ).fetchall()
        known_columns = {
            str(row["name"]).strip()
            for row in rows
            if isinstance(row["name"], str) and row["name"].strip()
        }
        return known_columns

    def _ensure_profiles_schema_columns(self, conn: sqlite3.Connection) -> None:
        known_columns = self._load_existing_column_names(conn, "profiles")
        expected_columns: dict[str, str] = {
            "display_name": "TEXT NOT NULL DEFAULT ''",
            "starting_ship_id": "TEXT",
            "tutorial_mode": "TEXT",
            "player_memory_json": "TEXT NOT NULL DEFAULT '{}'",
        }
        for column_name, column_definition in expected_columns.items():
            if column_name in known_columns:
                continue
            conn.execute(
                f"ALTER TABLE profiles ADD COLUMN {column_name} {column_definition}"
            )

    def _ensure_fleet_schema_columns(self, conn: sqlite3.Connection) -> None:
        known_columns = self._load_existing_column_names(conn, "fleet_state")
        expected_columns: dict[str, str] = {
            "ship_level": "INTEGER NOT NULL DEFAULT 1",
            "ship_xp": "REAL NOT NULL DEFAULT 0",
        }
        for column_name, column_definition in expected_columns.items():
            if column_name in known_columns:
                continue
            conn.execute(
                f"ALTER TABLE fleet_state ADD COLUMN {column_name} {column_definition}"
            )

    def _ensure_market_listing_schema_columns(self, conn: sqlite3.Connection) -> None:
        known_columns = self._load_existing_column_names(conn, "market_listings")
        expected_columns: dict[str, str] = {
            "metadata_json": "TEXT NOT NULL DEFAULT '{}'",
        }
        for column_name, column_definition in expected_columns.items():
            if column_name in known_columns:
                continue
            conn.execute(
                f"ALTER TABLE market_listings ADD COLUMN {column_name} {column_definition}"
            )

    def upsert_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        display_name = payload.get("display_name")
        captain_name = payload.get("captain_name")
        auth_mode = payload.get("auth_mode", "guest")
        email = payload.get("email", "")
        race_id = payload.get("race_id")
        profession_id = payload.get("profession_id")
        starting_ship_id = payload.get("starting_ship_id")
        tutorial_mode = payload.get("tutorial_mode")
        planet_type_id = payload.get("planet_type_id")
        player_memory = payload.get("player_memory", {})

        if not isinstance(player_id, str) or not player_id.strip():
            raise StateStoreError("player_id is required")
        if not isinstance(display_name, str):
            if isinstance(captain_name, str):
                display_name = captain_name
            else:
                raise StateStoreError("display_name must be at least 3 characters")
        if not isinstance(captain_name, str):
            captain_name = display_name
        if len(display_name.strip()) < 3:
            raise StateStoreError("display_name must be at least 3 characters")
        if auth_mode not in {"guest", "email"}:
            raise StateStoreError("auth_mode must be one of: guest, email")
        if not isinstance(email, str):
            raise StateStoreError("email must be a string")
        if starting_ship_id is not None and not isinstance(starting_ship_id, str):
            raise StateStoreError("starting_ship_id must be a string when provided")
        if tutorial_mode is not None and not isinstance(tutorial_mode, str):
            raise StateStoreError("tutorial_mode must be a string when provided")
        if isinstance(tutorial_mode, str) and tutorial_mode not in {"guided", "quick", "skip"}:
            raise StateStoreError("tutorial_mode must be one of: guided, quick, skip")
        if player_memory is None:
            player_memory = {}
        if not isinstance(player_memory, dict):
            raise StateStoreError("player_memory must be an object when provided")
        try:
            player_memory_json = json.dumps(player_memory, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise StateStoreError("player_memory must be JSON-serializable") from exc
        if len(player_memory_json) > 32000:
            raise StateStoreError("player_memory exceeds 32000 serialized bytes")

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO profiles (
                      player_id, captain_name, display_name, auth_mode, email,
                      race_id, profession_id, starting_ship_id, tutorial_mode,
                      planet_type_id, player_memory_json, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      captain_name=excluded.captain_name,
                      display_name=excluded.display_name,
                      auth_mode=excluded.auth_mode,
                      email=excluded.email,
                      race_id=excluded.race_id,
                      profession_id=excluded.profession_id,
                      starting_ship_id=excluded.starting_ship_id,
                      tutorial_mode=excluded.tutorial_mode,
                      planet_type_id=excluded.planet_type_id,
                      player_memory_json=excluded.player_memory_json,
                      updated_utc=excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        captain_name.strip(),
                        display_name.strip(),
                        auth_mode,
                        email.strip(),
                        race_id if isinstance(race_id, str) else None,
                        profession_id if isinstance(profession_id, str) else None,
                        starting_ship_id.strip()
                        if isinstance(starting_ship_id, str) and starting_ship_id.strip()
                        else None,
                        tutorial_mode if isinstance(tutorial_mode, str) else None,
                        planet_type_id if isinstance(planet_type_id, str) else None,
                        player_memory_json,
                        now,
                    ),
                )
                conn.commit()
        return self.get_profile(player_id)

    def get_profile(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT player_id, captain_name, display_name, auth_mode, email,
                           race_id, profession_id, starting_ship_id, tutorial_mode,
                           planet_type_id, player_memory_json, updated_utc
                    FROM profiles
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
        if row is None:
            raise StateStoreError(f"Profile not found for player_id={player_id}")
        profile = dict(row)
        display_name = profile.get("display_name")
        if not isinstance(display_name, str) or not display_name.strip():
            fallback_name = profile.get("captain_name")
            if isinstance(fallback_name, str) and fallback_name.strip():
                display_name = fallback_name.strip()
            else:
                display_name = "Captain"
        profile["display_name"] = display_name
        profile["captain_name"] = display_name

        memory_payload = profile.pop("player_memory_json", "{}")
        player_memory: dict[str, Any] = {}
        if isinstance(memory_payload, str) and memory_payload.strip():
            try:
                parsed_memory = json.loads(memory_payload)
                if isinstance(parsed_memory, dict):
                    player_memory = parsed_memory
            except json.JSONDecodeError:
                player_memory = {}
        profile["player_memory"] = player_memory
        profile["pvp_visibility"] = self.get_pvp_visibility_setting(player_id=player_id)
        profile["combat_progress"] = self.get_combat_progress(player_id=player_id)
        profile["action_energy"] = self.get_action_energy(player_id=player_id)
        faction_affiliation = self.get_player_faction_affiliation(player_id=player_id)
        profile["faction_affiliation"] = faction_affiliation
        profile["faction_id"] = (
            faction_affiliation.get("faction_id")
            if isinstance(faction_affiliation, dict)
            and isinstance(faction_affiliation.get("faction_id"), str)
            else None
        )
        profile["legion_membership"] = self.get_player_active_legion_membership(
            player_id=player_id
        )
        return profile

    def update_profile_memory(
        self, player_id: str, player_memory: dict[str, Any], merge: bool = True
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not isinstance(player_memory, dict):
            raise StateStoreError("player_memory must be an object")
        current_profile = self.get_profile(player_id=player_id)
        current_memory = current_profile.get("player_memory", {})
        if not isinstance(current_memory, dict):
            current_memory = {}
        next_memory = dict(current_memory) if merge else {}
        next_memory.update(player_memory)
        try:
            memory_json = json.dumps(next_memory, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError) as exc:
            raise StateStoreError("player_memory must be JSON-serializable") from exc
        if len(memory_json) > 32000:
            raise StateStoreError("player_memory exceeds 32000 serialized bytes")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE profiles
                    SET player_memory_json = ?, updated_utc = ?
                    WHERE player_id = ?
                    """,
                    (memory_json, now, player_id.strip()),
                )
                if conn.total_changes <= 0:
                    raise StateStoreError(f"Profile not found for player_id={player_id}")
                conn.commit()
        return self.get_profile(player_id=player_id)

    def profile_exists(self, player_id: str) -> bool:
        if not player_id.strip():
            return False
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 AS ok FROM profiles WHERE player_id = ?",
                    (player_id.strip(),),
                ).fetchone()
        return row is not None

    def _moderation_is_active(
        self,
        status: str | None,
        expires_utc: str | None,
    ) -> bool:
        if not isinstance(status, str) or not status.strip():
            return False
        status_value = status.strip().casefold()
        if status_value not in {"kicked", "banned", "suspended"}:
            return False
        if not isinstance(expires_utc, str) or not expires_utc.strip():
            return True
        try:
            return self._utc_to_epoch(expires_utc.strip()) > int(time.time())
        except ValueError:
            return True

    def get_player_moderation(self, player_id: str) -> dict[str, Any] | None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT player_id, status, reason, imposed_by_player_id, imposed_utc, expires_utc, updated_utc
                    FROM player_moderation
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
        if row is None:
            return None
        is_active = self._moderation_is_active(
            status=str(row["status"]),
            expires_utc=str(row["expires_utc"]) if row["expires_utc"] is not None else None,
        )
        return {
            "player_id": row["player_id"],
            "status": row["status"],
            "reason": row["reason"],
            "imposed_by_player_id": row["imposed_by_player_id"],
            "imposed_utc": row["imposed_utc"],
            "expires_utc": row["expires_utc"],
            "updated_utc": row["updated_utc"],
            "is_active": is_active,
        }

    def set_player_moderation(
        self,
        *,
        player_id: str,
        status: str,
        reason: str,
        imposed_by_player_id: str,
        duration_hours: float | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not imposed_by_player_id.strip():
            raise StateStoreError("imposed_by_player_id is required")
        status_value = str(status or "").strip().casefold()
        if status_value not in {"kicked", "banned", "suspended"}:
            raise StateStoreError("status must be one of: kicked, banned, suspended")
        reason_value = str(reason or "").strip()
        if len(reason_value) < 4 or len(reason_value) > 240:
            raise StateStoreError("reason must be 4-240 characters")
        expires_utc: str | None = None
        if duration_hours is not None:
            if isinstance(duration_hours, bool) or not isinstance(duration_hours, (int, float)):
                raise StateStoreError("duration_hours must be numeric when provided")
            duration = float(duration_hours)
            if not math.isfinite(duration) or duration <= 0:
                raise StateStoreError("duration_hours must be > 0 when provided")
            if duration > 24 * 365 * 3:
                raise StateStoreError("duration_hours is too large")
            expires_epoch = int(time.time() + (duration * 3600.0))
            expires_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_epoch))
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._assert_profile_exists(conn, player_id=imposed_by_player_id)
                conn.execute(
                    """
                    INSERT INTO player_moderation (
                      player_id, status, reason, imposed_by_player_id, imposed_utc, expires_utc, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      status = excluded.status,
                      reason = excluded.reason,
                      imposed_by_player_id = excluded.imposed_by_player_id,
                      imposed_utc = excluded.imposed_utc,
                      expires_utc = excluded.expires_utc,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        status_value,
                        reason_value,
                        imposed_by_player_id.strip(),
                        now,
                        expires_utc,
                        now,
                    ),
                )
                conn.commit()
        row = self.get_player_moderation(player_id=player_id)
        if not isinstance(row, dict):
            raise StateStoreError("Unable to load moderation row after update")
        return row

    def clear_player_moderation(
        self,
        *,
        player_id: str,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                cursor = conn.execute(
                    "DELETE FROM player_moderation WHERE player_id = ?",
                    (player_id.strip(),),
                )
                conn.commit()
        return {"player_id": player_id.strip(), "cleared": int(cursor.rowcount or 0) > 0}

    def list_player_profiles_admin(
        self,
        *,
        limit: int = 60,
        search: str | None = None,
        include_admin: bool = False,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        status_value = str(status or "all").strip().casefold()
        if status_value not in {"all", "active", "restricted"}:
            raise StateStoreError("status must be one of: all, active, restricted")

        where_clauses: list[str] = []
        params: list[Any] = []
        if not include_admin:
            where_clauses.append("p.player_id <> 'admin'")
        if isinstance(search, str) and search.strip():
            like = f"%{search.strip().casefold()}%"
            where_clauses.append(
                "(LOWER(p.player_id) LIKE ? OR LOWER(p.display_name) LIKE ? OR LOWER(p.captain_name) LIKE ?)"
            )
            params.extend([like, like, like])

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = (
            "SELECT p.player_id, p.display_name, p.captain_name, p.race_id, p.profession_id, "
            "p.updated_utc, pm.status AS moderation_status, pm.reason AS moderation_reason, "
            "pm.expires_utc AS moderation_expires_utc, "
            "COALESCE(cp.combat_rank, 1) AS combat_rank, "
            "COALESCE(fs.ship_level, 1) AS ship_level, "
            "COALESCE(w.credits, 0) AS credits "
            "FROM profiles p "
            "LEFT JOIN player_moderation pm ON pm.player_id = p.player_id "
            "LEFT JOIN player_combat_progress cp ON cp.player_id = p.player_id "
            "LEFT JOIN fleet_state fs ON fs.player_id = p.player_id "
            "LEFT JOIN wallets w ON w.player_id = p.player_id "
            f"{where_sql} "
            "ORDER BY p.updated_utc DESC "
            "LIMIT ?"
        )
        params.append(int(limit))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(query, tuple(params)).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            is_restricted = self._moderation_is_active(
                status=str(row["moderation_status"]) if row["moderation_status"] is not None else None,
                expires_utc=(
                    str(row["moderation_expires_utc"])
                    if row["moderation_expires_utc"] is not None
                    else None
                ),
            )
            if status_value == "active" and is_restricted:
                continue
            if status_value == "restricted" and (not is_restricted):
                continue
            out.append(
                {
                    "player_id": row["player_id"],
                    "display_name": row["display_name"] or row["captain_name"] or row["player_id"],
                    "race_id": row["race_id"],
                    "profession_id": row["profession_id"],
                    "combat_rank": int(row["combat_rank"]),
                    "ship_level": int(row["ship_level"]),
                    "credits": round(float(row["credits"]), 2),
                    "updated_utc": row["updated_utc"],
                    "moderation": {
                        "status": row["moderation_status"],
                        "reason": row["moderation_reason"],
                        "expires_utc": row["moderation_expires_utc"],
                        "is_active": is_restricted,
                    }
                    if row["moderation_status"] is not None
                    else None,
                }
            )
        return out

    def log_admin_action(
        self,
        *,
        admin_player_id: str,
        action_type: str,
        target_player_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not admin_player_id.strip():
            raise StateStoreError("admin_player_id is required")
        action_type_value = str(action_type or "").strip().casefold()
        if len(action_type_value) < 3 or len(action_type_value) > 80:
            raise StateStoreError("action_type must be 3-80 characters")
        action_id = f"adminact.{uuid.uuid4().hex[:16]}"
        created_utc = self._utc_now()
        payload_json = json.dumps(
            payload if isinstance(payload, dict) else {},
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=admin_player_id)
                if isinstance(target_player_id, str) and target_player_id.strip():
                    self._assert_profile_exists(conn, player_id=target_player_id.strip())
                conn.execute(
                    """
                    INSERT INTO admin_action_log (
                      action_id, admin_player_id, action_type, target_player_id, payload_json, created_utc
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        action_id,
                        admin_player_id.strip(),
                        action_type_value,
                        target_player_id.strip() if isinstance(target_player_id, str) and target_player_id.strip() else None,
                        payload_json,
                        created_utc,
                    ),
                )
                conn.commit()
        return {
            "action_id": action_id,
            "admin_player_id": admin_player_id.strip(),
            "action_type": action_type_value,
            "target_player_id": (
                target_player_id.strip()
                if isinstance(target_player_id, str) and target_player_id.strip()
                else None
            ),
            "created_utc": created_utc,
        }

    def list_admin_actions(
        self,
        *,
        limit: int = 80,
        admin_player_id: str | None = None,
        target_player_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        clauses: list[str] = []
        params: list[Any] = []
        if isinstance(admin_player_id, str) and admin_player_id.strip():
            clauses.append("admin_player_id = ?")
            params.append(admin_player_id.strip())
        if isinstance(target_player_id, str) and target_player_id.strip():
            clauses.append("target_player_id = ?")
            params.append(target_player_id.strip())
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            "SELECT action_id, admin_player_id, action_type, target_player_id, payload_json, created_utc "
            "FROM admin_action_log "
            f"{where_sql} "
            "ORDER BY created_utc DESC "
            "LIMIT ?"
        )
        params.append(int(limit))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(query, tuple(params)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                parsed = json.loads(str(row["payload_json"]))
                if isinstance(parsed, dict):
                    payload = parsed
            except json.JSONDecodeError:
                payload = {}
            out.append(
                {
                    "action_id": row["action_id"],
                    "admin_player_id": row["admin_player_id"],
                    "action_type": row["action_type"],
                    "target_player_id": row["target_player_id"],
                    "payload": payload,
                    "created_utc": row["created_utc"],
                }
            )
        return out

    @staticmethod
    def _covert_op_type_value(op_type: str) -> str:
        value = str(op_type or "").strip().casefold()
        if value not in {"steal", "sabotage", "hack"}:
            raise StateStoreError("op_type must be one of: steal, sabotage, hack")
        return value

    def get_covert_cooldown(self, player_id: str, op_type: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        op_type_value = self._covert_op_type_value(op_type)
        now_epoch = int(time.time())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT next_ready_utc, updated_utc
                    FROM covert_op_cooldowns
                    WHERE player_id = ? AND op_type = ?
                    """,
                    (player_id.strip(), op_type_value),
                ).fetchone()
        if row is None:
            return {
                "player_id": player_id.strip(),
                "op_type": op_type_value,
                "ready": True,
                "seconds_remaining": 0,
                "next_ready_utc": None,
                "updated_utc": None,
            }
        next_ready_utc = str(row["next_ready_utc"])
        try:
            next_epoch = self._utc_to_epoch(next_ready_utc)
        except Exception:
            next_epoch = now_epoch
        remaining = max(0, next_epoch - now_epoch)
        return {
            "player_id": player_id.strip(),
            "op_type": op_type_value,
            "ready": remaining <= 0,
            "seconds_remaining": int(remaining),
            "next_ready_utc": next_ready_utc,
            "updated_utc": row["updated_utc"],
        }

    def list_covert_cooldowns(self, player_id: str) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        return [
            self.get_covert_cooldown(player_id=player_id, op_type=op_type)
            for op_type in ("steal", "sabotage", "hack")
        ]

    def set_covert_cooldown(
        self,
        *,
        player_id: str,
        op_type: str,
        cooldown_seconds: float,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        op_type_value = self._covert_op_type_value(op_type)
        if isinstance(cooldown_seconds, bool) or not isinstance(cooldown_seconds, (int, float)):
            raise StateStoreError("cooldown_seconds must be numeric")
        cooldown_value = float(cooldown_seconds)
        if not math.isfinite(cooldown_value):
            raise StateStoreError("cooldown_seconds must be finite")
        cooldown_value = max(0.0, min(7.0 * 24.0 * 3600.0, cooldown_value))
        now = self._utc_now()
        next_ready_epoch = int(time.time() + cooldown_value)
        next_ready_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(next_ready_epoch))
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO covert_op_cooldowns (
                      player_id, op_type, next_ready_utc, updated_utc
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(player_id, op_type) DO UPDATE SET
                      next_ready_utc = excluded.next_ready_utc,
                      updated_utc = excluded.updated_utc
                    """,
                    (player_id.strip(), op_type_value, next_ready_utc, now),
                )
                conn.commit()
        return self.get_covert_cooldown(player_id=player_id, op_type=op_type_value)

    def log_covert_op(
        self,
        *,
        actor_player_id: str,
        target_player_id: str,
        op_type: str,
        status: str,
        success_probability: float,
        detection_probability: float,
        outcome: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not actor_player_id.strip():
            raise StateStoreError("actor_player_id is required")
        if not target_player_id.strip():
            raise StateStoreError("target_player_id is required")
        op_type_value = self._covert_op_type_value(op_type)
        status_value = str(status or "").strip().casefold()
        if status_value not in {"success", "failed", "blocked"}:
            raise StateStoreError("status must be one of: success, failed, blocked")
        if isinstance(success_probability, bool) or not isinstance(success_probability, (int, float)):
            raise StateStoreError("success_probability must be numeric")
        if isinstance(detection_probability, bool) or not isinstance(detection_probability, (int, float)):
            raise StateStoreError("detection_probability must be numeric")
        success_prob = max(0.0, min(1.0, float(success_probability)))
        detection_prob = max(0.0, min(1.0, float(detection_probability)))
        created_utc = self._utc_now()
        op_event_id = f"covert.{uuid.uuid4().hex[:16]}"
        outcome_json = json.dumps(
            outcome if isinstance(outcome, dict) else {},
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=actor_player_id)
                self._assert_profile_exists(conn, player_id=target_player_id)
                conn.execute(
                    """
                    INSERT INTO covert_op_log (
                      op_event_id, actor_player_id, target_player_id, op_type, status,
                      success_probability, detection_probability, outcome_json, created_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        op_event_id,
                        actor_player_id.strip(),
                        target_player_id.strip(),
                        op_type_value,
                        status_value,
                        success_prob,
                        detection_prob,
                        outcome_json,
                        created_utc,
                    ),
                )
                conn.commit()
        return {
            "op_event_id": op_event_id,
            "actor_player_id": actor_player_id.strip(),
            "target_player_id": target_player_id.strip(),
            "op_type": op_type_value,
            "status": status_value,
            "success_probability": round(success_prob, 4),
            "detection_probability": round(detection_prob, 4),
            "outcome": outcome if isinstance(outcome, dict) else {},
            "created_utc": created_utc,
        }

    def list_covert_logs(
        self,
        *,
        player_id: str,
        perspective: str = "both",
        op_type: str | None = None,
        limit: int = 80,
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        perspective_value = str(perspective or "both").strip().casefold()
        if perspective_value not in {"actor", "target", "both"}:
            raise StateStoreError("perspective must be one of: actor, target, both")
        op_type_value = None
        if isinstance(op_type, str) and op_type.strip():
            op_type_value = self._covert_op_type_value(op_type)

        clauses: list[str] = []
        params: list[Any] = []
        if perspective_value == "actor":
            clauses.append("actor_player_id = ?")
            params.append(player_id.strip())
        elif perspective_value == "target":
            clauses.append("target_player_id = ?")
            params.append(player_id.strip())
        else:
            clauses.append("(actor_player_id = ? OR target_player_id = ?)")
            params.extend([player_id.strip(), player_id.strip()])
        if op_type_value is not None:
            clauses.append("op_type = ?")
            params.append(op_type_value)
        where_sql = f"WHERE {' AND '.join(clauses)}"
        query = (
            "SELECT op_event_id, actor_player_id, target_player_id, op_type, status, "
            "success_probability, detection_probability, outcome_json, created_utc "
            "FROM covert_op_log "
            f"{where_sql} "
            "ORDER BY created_utc DESC "
            "LIMIT ?"
        )
        params.append(int(limit))
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                rows = conn.execute(query, tuple(params)).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            outcome = {}
            try:
                parsed = json.loads(str(row["outcome_json"]))
                if isinstance(parsed, dict):
                    outcome = parsed
            except json.JSONDecodeError:
                outcome = {}
            out.append(
                {
                    "op_event_id": row["op_event_id"],
                    "actor_player_id": row["actor_player_id"],
                    "target_player_id": row["target_player_id"],
                    "op_type": row["op_type"],
                    "status": row["status"],
                    "success_probability": round(float(row["success_probability"]), 4),
                    "detection_probability": round(float(row["detection_probability"]), 4),
                    "outcome": outcome,
                    "created_utc": row["created_utc"],
                }
            )
        return out

    def get_pvp_visibility_setting(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT allow_high_risk_visibility, high_risk_loss_threshold, updated_utc
                    FROM player_pvp_settings
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_pvp_settings (
                          player_id, allow_high_risk_visibility, high_risk_loss_threshold, updated_utc
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            player_id.strip(),
                            0,
                            DEFAULT_HIGH_RISK_VISIBILITY_THRESHOLD,
                            now,
                        ),
                    )
                    conn.commit()
                    row = conn.execute(
                        """
                        SELECT allow_high_risk_visibility, high_risk_loss_threshold, updated_utc
                        FROM player_pvp_settings
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
        if row is None:
            raise StateStoreError("Unable to load pvp visibility settings")
        return {
            "allow_high_risk_visibility": bool(int(row["allow_high_risk_visibility"])),
            "high_risk_loss_threshold": round(float(row["high_risk_loss_threshold"]), 4),
            "updated_utc": row["updated_utc"],
        }

    def set_pvp_visibility_setting(
        self,
        player_id: str,
        allow_high_risk_visibility: bool,
        high_risk_loss_threshold: float | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        threshold = (
            DEFAULT_HIGH_RISK_VISIBILITY_THRESHOLD
            if high_risk_loss_threshold is None
            else float(high_risk_loss_threshold)
        )
        if not math.isfinite(threshold):
            raise StateStoreError("high_risk_loss_threshold must be finite")
        threshold = max(0.55, min(0.99, threshold))
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO player_pvp_settings (
                      player_id, allow_high_risk_visibility, high_risk_loss_threshold, updated_utc
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      allow_high_risk_visibility = excluded.allow_high_risk_visibility,
                      high_risk_loss_threshold = excluded.high_risk_loss_threshold,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        1 if allow_high_risk_visibility else 0,
                        threshold,
                        now,
                    ),
                )
                conn.commit()
        return self.get_pvp_visibility_setting(player_id=player_id)

    def get_combat_progress(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT combat_xp, combat_rank, updated_utc
                    FROM player_combat_progress
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_combat_progress (player_id, combat_xp, combat_rank, updated_utc)
                        VALUES (?, ?, ?, ?)
                        """,
                        (player_id.strip(), 0.0, 1, now),
                    )
                    conn.commit()
                    row = conn.execute(
                        """
                        SELECT combat_xp, combat_rank, updated_utc
                        FROM player_combat_progress
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
        if row is None:
            raise StateStoreError("Unable to load combat progress")
        combat_xp = max(0.0, float(row["combat_xp"]))
        combat_rank, rank_xp, xp_to_next = self._combat_rank_from_total_xp(combat_xp)
        return {
            "combat_xp": round(combat_xp, 3),
            "combat_rank": int(combat_rank),
            "combat_rank_xp": round(rank_xp, 3),
            "combat_xp_to_next_rank": round(xp_to_next, 3),
            "combat_rank_progress_ratio": round(min(1.0, rank_xp / max(1.0, xp_to_next)), 5),
            "combat_xp_formula": "xp_to_next_rank = 90 * rank^1.24 * (1 + 0.0085 * max(rank-10,0)^0.74)",
            "updated_utc": row["updated_utc"],
        }

    def _combat_xp_to_next_rank(self, rank: int) -> float:
        rank_value = max(1, int(rank))
        # Near-forever progression: each rank costs progressively more XP.
        base = 90.0 * (float(rank_value) ** 1.24)
        high_rank_tail = 1.0 + (0.0085 * (max(0.0, float(rank_value - 10)) ** 0.74))
        return max(65.0, base * high_rank_tail)

    def _combat_rank_from_total_xp(self, total_xp: float) -> tuple[int, float, float]:
        pool = max(0.0, float(total_xp))
        rank = 1
        while rank < 250_000:
            needed = self._combat_xp_to_next_rank(rank)
            if pool + 1e-9 < needed:
                return rank, pool, needed
            pool -= needed
            rank += 1
        return 250_000, 0.0, self._combat_xp_to_next_rank(250_000)

    def grant_combat_xp(self, player_id: str, xp_delta: float) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not math.isfinite(float(xp_delta)):
            raise StateStoreError("xp_delta must be finite")
        xp_delta = max(0.0, float(xp_delta))
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT combat_xp, combat_rank
                    FROM player_combat_progress
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    current_xp = 0.0
                    conn.execute(
                        """
                        INSERT INTO player_combat_progress (player_id, combat_xp, combat_rank, updated_utc)
                        VALUES (?, ?, ?, ?)
                        """,
                        (player_id.strip(), current_xp, 1, now),
                    )
                else:
                    current_xp = float(row["combat_xp"])

                next_xp = max(0.0, current_xp + xp_delta)
                next_rank, _, _ = self._combat_rank_from_total_xp(next_xp)
                conn.execute(
                    """
                    UPDATE player_combat_progress
                    SET combat_xp = ?, combat_rank = ?, updated_utc = ?
                    WHERE player_id = ?
                    """,
                    (next_xp, next_rank, now, player_id.strip()),
                )
                conn.commit()
        return self.get_combat_progress(player_id=player_id)

    def get_action_energy(
        self,
        player_id: str,
        max_energy_bonus: float = 0.0,
        regen_bonus_per_hour: float = 0.0,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now_utc = self._utc_now()
        now_epoch = int(time.time())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT current_energy, max_energy, regen_per_hour, updated_utc
                    FROM player_action_energy
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_action_energy (
                          player_id, current_energy, max_energy, regen_per_hour, updated_utc
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            player_id.strip(),
                            DEFAULT_ACTION_ENERGY_MAX,
                            DEFAULT_ACTION_ENERGY_MAX,
                            DEFAULT_ACTION_ENERGY_REGEN_PER_HOUR,
                            now_utc,
                        ),
                    )
                    conn.commit()
                    row = conn.execute(
                        """
                        SELECT current_energy, max_energy, regen_per_hour, updated_utc
                        FROM player_action_energy
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
                if row is None:
                    raise StateStoreError("Unable to load action energy")
                current_energy = float(row["current_energy"])
                base_max_energy = max(1.0, float(row["max_energy"]))
                base_regen_per_hour = max(0.0, float(row["regen_per_hour"]))
                max_energy = max(1.0, base_max_energy + max(0.0, float(max_energy_bonus)))
                regen_per_hour = max(
                    0.0,
                    base_regen_per_hour + max(0.0, float(regen_bonus_per_hour)),
                )
                updated_utc = str(row["updated_utc"])
                try:
                    last_epoch = self._utc_to_epoch(updated_utc)
                except Exception:
                    last_epoch = now_epoch
                elapsed_hours = max(0.0, (now_epoch - last_epoch) / 3600.0)
                regenerated = min(max_energy, current_energy + (elapsed_hours * regen_per_hour))
                if abs(regenerated - current_energy) > 1e-9 or updated_utc != now_utc:
                    conn.execute(
                        """
                        UPDATE player_action_energy
                        SET current_energy = ?, updated_utc = ?
                        WHERE player_id = ?
                        """,
                        (
                            regenerated,
                            now_utc,
                            player_id.strip(),
                        ),
                    )
                    conn.commit()
                else:
                    regenerated = current_energy
        remaining = max(0.0, max_energy - regenerated)
        seconds_to_full = 0
        if regen_per_hour > 1e-9 and remaining > 1e-9:
            seconds_to_full = int(math.ceil((remaining / regen_per_hour) * 3600.0))
        return {
            "current_energy": round(regenerated, 4),
            "max_energy": round(max_energy, 4),
            "regen_per_hour": round(regen_per_hour, 4),
            "fill_ratio": round(min(1.0, regenerated / max(1e-9, max_energy)), 4),
            "seconds_to_full": max(0, seconds_to_full),
            "updated_utc": now_utc,
        }

    def consume_action_energy(
        self,
        player_id: str,
        amount: float,
        reason: str = "action",
        max_energy_bonus: float = 0.0,
        regen_bonus_per_hour: float = 0.0,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise StateStoreError("amount must be numeric")
        amount = float(amount)
        if not math.isfinite(amount) or amount <= 0:
            raise StateStoreError("amount must be > 0 and finite")
        now_utc = self._utc_now()
        now_epoch = int(time.time())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT current_energy, max_energy, regen_per_hour, updated_utc
                    FROM player_action_energy
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_action_energy (
                          player_id, current_energy, max_energy, regen_per_hour, updated_utc
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            player_id.strip(),
                            DEFAULT_ACTION_ENERGY_MAX,
                            DEFAULT_ACTION_ENERGY_MAX,
                            DEFAULT_ACTION_ENERGY_REGEN_PER_HOUR,
                            now_utc,
                        ),
                    )
                    row = conn.execute(
                        """
                        SELECT current_energy, max_energy, regen_per_hour, updated_utc
                        FROM player_action_energy
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
                if row is None:
                    raise StateStoreError("Unable to load action energy")
                current_energy = float(row["current_energy"])
                base_max_energy = max(1.0, float(row["max_energy"]))
                base_regen_per_hour = max(0.0, float(row["regen_per_hour"]))
                max_energy = max(1.0, base_max_energy + max(0.0, float(max_energy_bonus)))
                regen_per_hour = max(
                    0.0,
                    base_regen_per_hour + max(0.0, float(regen_bonus_per_hour)),
                )
                updated_utc = str(row["updated_utc"])
                try:
                    last_epoch = self._utc_to_epoch(updated_utc)
                except Exception:
                    last_epoch = now_epoch
                elapsed_hours = max(0.0, (now_epoch - last_epoch) / 3600.0)
                regenerated = min(max_energy, current_energy + (elapsed_hours * regen_per_hour))
                if regenerated + 1e-9 < amount:
                    raise StateStoreError(
                        "Insufficient action energy: need {:.2f}, have {:.2f}".format(
                            amount,
                            regenerated,
                        )
                    )
                remaining = max(0.0, regenerated - amount)
                conn.execute(
                    """
                    UPDATE player_action_energy
                    SET current_energy = ?, updated_utc = ?
                    WHERE player_id = ?
                    """,
                    (
                        remaining,
                        now_utc,
                        player_id.strip(),
                    ),
                )
                conn.commit()
        next_payload = self.get_action_energy(
            player_id=player_id,
            max_energy_bonus=max_energy_bonus,
            regen_bonus_per_hour=regen_bonus_per_hour,
        )
        next_payload["spent"] = round(amount, 4)
        next_payload["reason"] = reason
        return next_payload

    def set_action_energy(
        self,
        player_id: str,
        current_energy: float,
        max_energy: float,
        regen_per_hour: float,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if isinstance(current_energy, bool) or not isinstance(current_energy, (int, float)):
            raise StateStoreError("current_energy must be numeric")
        if isinstance(max_energy, bool) or not isinstance(max_energy, (int, float)):
            raise StateStoreError("max_energy must be numeric")
        if isinstance(regen_per_hour, bool) or not isinstance(regen_per_hour, (int, float)):
            raise StateStoreError("regen_per_hour must be numeric")
        max_energy = max(1.0, float(max_energy))
        current_energy = max(0.0, min(max_energy, float(current_energy)))
        regen_per_hour = max(0.0, float(regen_per_hour))
        now_utc = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO player_action_energy (
                      player_id, current_energy, max_energy, regen_per_hour, updated_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      current_energy = excluded.current_energy,
                      max_energy = excluded.max_energy,
                      regen_per_hour = excluded.regen_per_hour,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        current_energy,
                        max_energy,
                        regen_per_hour,
                        now_utc,
                    ),
                )
                conn.commit()
        return self.get_action_energy(player_id=player_id)

    def get_life_support_state(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now_utc = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT player_id, last_tick_utc, deficit_air, deficit_water, deficit_food,
                           shortage_stress, updated_utc
                    FROM player_life_support_state
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_life_support_state (
                          player_id, last_tick_utc, deficit_air, deficit_water, deficit_food,
                          shortage_stress, updated_utc
                        ) VALUES (?, ?, 0, 0, 0, 0, ?)
                        """,
                        (player_id.strip(), now_utc, now_utc),
                    )
                    conn.commit()
                    row = conn.execute(
                        """
                        SELECT player_id, last_tick_utc, deficit_air, deficit_water, deficit_food,
                               shortage_stress, updated_utc
                        FROM player_life_support_state
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
        if row is None:
            raise StateStoreError("Unable to load life support state")
        return {
            "player_id": str(row["player_id"]),
            "last_tick_utc": str(row["last_tick_utc"]),
            "deficit_air": round(float(row["deficit_air"]), 6),
            "deficit_water": round(float(row["deficit_water"]), 6),
            "deficit_food": round(float(row["deficit_food"]), 6),
            "shortage_stress": round(float(row["shortage_stress"]), 6),
            "updated_utc": str(row["updated_utc"]),
        }

    def set_life_support_state(
        self,
        player_id: str,
        *,
        last_tick_utc: str,
        deficit_air: float,
        deficit_water: float,
        deficit_food: float,
        shortage_stress: float,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not isinstance(last_tick_utc, str) or not last_tick_utc.strip():
            raise StateStoreError("last_tick_utc is required")
        for label, value in (
            ("deficit_air", deficit_air),
            ("deficit_water", deficit_water),
            ("deficit_food", deficit_food),
            ("shortage_stress", shortage_stress),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise StateStoreError(f"{label} must be numeric")
            if not math.isfinite(float(value)):
                raise StateStoreError(f"{label} must be finite")
        now_utc = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO player_life_support_state (
                      player_id, last_tick_utc, deficit_air, deficit_water, deficit_food,
                      shortage_stress, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      last_tick_utc = excluded.last_tick_utc,
                      deficit_air = excluded.deficit_air,
                      deficit_water = excluded.deficit_water,
                      deficit_food = excluded.deficit_food,
                      shortage_stress = excluded.shortage_stress,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        last_tick_utc.strip(),
                        max(0.0, float(deficit_air)),
                        max(0.0, float(deficit_water)),
                        max(0.0, float(deficit_food)),
                        max(0.0, min(100.0, float(shortage_stress))),
                        now_utc,
                    ),
                )
                conn.commit()
        return self.get_life_support_state(player_id=player_id)

    def _assert_profile_exists(self, conn: sqlite3.Connection, player_id: str) -> None:
        row = conn.execute(
            "SELECT 1 FROM profiles WHERE player_id = ?",
            (player_id.strip(),),
        ).fetchone()
        if row is None:
            raise StateStoreError(
                f"Profile not found for player_id='{player_id}'. Save profile first."
            )

    def bootstrap_player(
        self,
        player_id: str,
        starter_inventory: dict[str, float],
        starter_tech_ids: list[str],
    ) -> None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO wallets (player_id, credits, voidcoin, updated_utc)
                    VALUES (?, ?, ?, ?)
                    """,
                    (player_id, STARTING_CREDITS, STARTING_VOIDCOIN, now),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO player_action_energy (
                      player_id, current_energy, max_energy, regen_per_hour, updated_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        player_id,
                        DEFAULT_ACTION_ENERGY_MAX,
                        DEFAULT_ACTION_ENERGY_MAX,
                        DEFAULT_ACTION_ENERGY_REGEN_PER_HOUR,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO player_life_support_state (
                      player_id, last_tick_utc, deficit_air, deficit_water, deficit_food,
                      shortage_stress, updated_utc
                    ) VALUES (?, ?, 0, 0, 0, 0, ?)
                    """,
                    (
                        player_id,
                        now,
                        now,
                    ),
                )

                for symbol, amount in starter_inventory.items():
                    if amount <= 0:
                        continue
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO element_inventory (player_id, symbol, amount, updated_utc)
                        VALUES (?, ?, ?, ?)
                        """,
                        (player_id, symbol, float(amount), now),
                    )

                for tech_id in starter_tech_ids:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO research_unlocks (player_id, tech_id, unlocked_utc)
                        VALUES (?, ?, ?)
                        """,
                        (player_id, tech_id, now),
                    )
                conn.commit()

    def get_wallet(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT player_id, credits, voidcoin, updated_utc
                    FROM wallets
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
        if row is None:
            raise StateStoreError("Wallet not found; save profile first")
        wallet = dict(row)
        wallet["credits"] = round(float(wallet["credits"]), 2)
        wallet["voidcoin"] = round(float(wallet["voidcoin"]), 8)
        return wallet

    def list_inventory(self, player_id: str, limit: int = 40) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT symbol, amount, updated_utc
                    FROM element_inventory
                    WHERE player_id = ? AND amount > 0
                    ORDER BY amount DESC, symbol ASC
                    LIMIT ?
                    """,
                    (player_id.strip(), limit),
                ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "symbol": row["symbol"],
                    "amount": round(float(row["amount"]), 3),
                    "updated_utc": row["updated_utc"],
                }
            )
        return items

    def get_inventory_amounts(
        self, player_id: str, symbols: list[str] | None = None
    ) -> dict[str, float]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                if symbols:
                    placeholders = ",".join("?" for _ in symbols)
                    args: list[Any] = [player_id.strip(), *symbols]
                    rows = conn.execute(
                        f"""
                        SELECT symbol, amount
                        FROM element_inventory
                        WHERE player_id = ? AND symbol IN ({placeholders})
                        """,
                        args,
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT symbol, amount
                        FROM element_inventory
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchall()
        return {row["symbol"]: float(row["amount"]) for row in rows}

    def adjust_wallet(
        self, player_id: str, credits_delta: float = 0.0, voidcoin_delta: float = 0.0
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if math.isnan(credits_delta) or math.isnan(voidcoin_delta):
            raise StateStoreError("wallet deltas must be finite")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    "SELECT credits, voidcoin FROM wallets WHERE player_id = ?",
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO wallets (player_id, credits, voidcoin, updated_utc)
                        VALUES (?, ?, ?, ?)
                        """,
                        (player_id.strip(), STARTING_CREDITS, STARTING_VOIDCOIN, now),
                    )
                    row = conn.execute(
                        "SELECT credits, voidcoin FROM wallets WHERE player_id = ?",
                        (player_id.strip(),),
                    ).fetchone()

                next_credits = float(row["credits"]) + float(credits_delta)
                next_voidcoin = float(row["voidcoin"]) + float(voidcoin_delta)
                if next_credits < -1e-9:
                    raise StateStoreError("Insufficient credits")
                if next_voidcoin < -1e-9:
                    raise StateStoreError("Insufficient voidcoin")

                conn.execute(
                    """
                    UPDATE wallets
                    SET credits = ?, voidcoin = ?, updated_utc = ?
                    WHERE player_id = ?
                    """,
                    (max(0.0, next_credits), max(0.0, next_voidcoin), now, player_id.strip()),
                )
                conn.commit()
        return self.get_wallet(player_id)

    def adjust_inventory(self, player_id: str, symbol_deltas: dict[str, float]) -> dict[str, float]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not symbol_deltas:
            return self.get_inventory_amounts(player_id=player_id, symbols=[])
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                for symbol, delta in symbol_deltas.items():
                    if not isinstance(symbol, str) or not symbol.strip():
                        raise StateStoreError("Inventory symbol keys must be non-empty strings")
                    if isinstance(delta, bool) or not isinstance(delta, (int, float)):
                        raise StateStoreError("Inventory delta values must be numeric")
                    if math.isnan(float(delta)) or math.isinf(float(delta)):
                        raise StateStoreError("Inventory delta values must be finite")

                    normalized_symbol = symbol.strip()
                    row = conn.execute(
                        """
                        SELECT amount
                        FROM element_inventory
                        WHERE player_id = ? AND symbol = ?
                        """,
                        (player_id.strip(), normalized_symbol),
                    ).fetchone()
                    current = float(row["amount"]) if row is not None else 0.0
                    next_amount = current + float(delta)
                    if next_amount < -1e-9:
                        raise StateStoreError(
                            f"Insufficient inventory for symbol '{normalized_symbol}'"
                        )

                    if row is None:
                        conn.execute(
                            """
                            INSERT INTO element_inventory (player_id, symbol, amount, updated_utc)
                            VALUES (?, ?, ?, ?)
                            """,
                            (player_id.strip(), normalized_symbol, max(0.0, next_amount), now),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE element_inventory
                            SET amount = ?, updated_utc = ?
                            WHERE player_id = ? AND symbol = ?
                            """,
                            (max(0.0, next_amount), now, player_id.strip(), normalized_symbol),
                        )
                conn.commit()

        return self.get_inventory_amounts(player_id=player_id, symbols=list(symbol_deltas.keys()))

    def apply_resource_delta(
        self,
        player_id: str,
        credits_delta: float = 0.0,
        voidcoin_delta: float = 0.0,
        element_deltas: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if math.isnan(float(credits_delta)) or math.isinf(float(credits_delta)):
            raise StateStoreError("credits_delta must be finite")
        if math.isnan(float(voidcoin_delta)) or math.isinf(float(voidcoin_delta)):
            raise StateStoreError("voidcoin_delta must be finite")
        element_deltas = element_deltas or {}
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                wallet_row = conn.execute(
                    "SELECT credits, voidcoin FROM wallets WHERE player_id = ?",
                    (player_id.strip(),),
                ).fetchone()
                if wallet_row is None:
                    conn.execute(
                        """
                        INSERT INTO wallets (player_id, credits, voidcoin, updated_utc)
                        VALUES (?, ?, ?, ?)
                        """,
                        (player_id.strip(), STARTING_CREDITS, STARTING_VOIDCOIN, now),
                    )
                    wallet_row = conn.execute(
                        "SELECT credits, voidcoin FROM wallets WHERE player_id = ?",
                        (player_id.strip(),),
                    ).fetchone()

                next_credits = float(wallet_row["credits"]) + float(credits_delta)
                next_voidcoin = float(wallet_row["voidcoin"]) + float(voidcoin_delta)
                if next_credits < -1e-9:
                    raise StateStoreError("Insufficient credits")
                if next_voidcoin < -1e-9:
                    raise StateStoreError("Insufficient voidcoin")

                for symbol, delta in element_deltas.items():
                    if not isinstance(symbol, str) or not symbol.strip():
                        raise StateStoreError("Inventory symbol keys must be non-empty strings")
                    if isinstance(delta, bool) or not isinstance(delta, (int, float)):
                        raise StateStoreError("Inventory delta values must be numeric")
                    delta_value = float(delta)
                    if math.isnan(delta_value) or math.isinf(delta_value):
                        raise StateStoreError("Inventory delta values must be finite")
                    row = conn.execute(
                        """
                        SELECT amount
                        FROM element_inventory
                        WHERE player_id = ? AND symbol = ?
                        """,
                        (player_id.strip(), symbol.strip()),
                    ).fetchone()
                    current = float(row["amount"]) if row is not None else 0.0
                    next_amount = current + delta_value
                    if next_amount < -1e-9:
                        raise StateStoreError(f"Insufficient inventory for symbol '{symbol.strip()}'")

                conn.execute(
                    """
                    UPDATE wallets
                    SET credits = ?, voidcoin = ?, updated_utc = ?
                    WHERE player_id = ?
                    """,
                    (max(0.0, next_credits), max(0.0, next_voidcoin), now, player_id.strip()),
                )

                for symbol, delta in element_deltas.items():
                    delta_value = float(delta)
                    normalized_symbol = symbol.strip()
                    row = conn.execute(
                        """
                        SELECT amount
                        FROM element_inventory
                        WHERE player_id = ? AND symbol = ?
                        """,
                        (player_id.strip(), normalized_symbol),
                    ).fetchone()
                    current = float(row["amount"]) if row is not None else 0.0
                    next_amount = max(0.0, current + delta_value)
                    if row is None:
                        conn.execute(
                            """
                            INSERT INTO element_inventory (player_id, symbol, amount, updated_utc)
                            VALUES (?, ?, ?, ?)
                            """,
                            (player_id.strip(), normalized_symbol, next_amount, now),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE element_inventory
                            SET amount = ?, updated_utc = ?
                            WHERE player_id = ? AND symbol = ?
                            """,
                            (next_amount, now, player_id.strip(), normalized_symbol),
                        )
                conn.commit()

        wallet = self.get_wallet(player_id)
        inventory = self.get_inventory_amounts(
            player_id=player_id,
            symbols=list(element_deltas.keys()),
        )
        return {"wallet": wallet, "inventory": inventory}

    def list_assets(
        self, player_id: str, asset_type: str | None = None, limit: int = 60
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                if asset_type is None:
                    rows = conn.execute(
                        """
                        SELECT asset_type, asset_id, quantity, updated_utc
                        FROM player_assets
                        WHERE player_id = ?
                        ORDER BY updated_utc DESC, asset_type ASC, asset_id ASC
                        LIMIT ?
                        """,
                        (player_id.strip(), limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT asset_type, asset_id, quantity, updated_utc
                        FROM player_assets
                        WHERE player_id = ? AND asset_type = ?
                        ORDER BY updated_utc DESC, asset_id ASC
                        LIMIT ?
                        """,
                        (player_id.strip(), asset_type, limit),
                    ).fetchall()

        return [
            {
                "asset_type": row["asset_type"],
                "asset_id": row["asset_id"],
                "quantity": int(row["quantity"]),
                "updated_utc": row["updated_utc"],
            }
            for row in rows
        ]

    def add_asset(self, player_id: str, asset_type: str, asset_id: str, quantity: int = 1) -> None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not asset_type.strip():
            raise StateStoreError("asset_type is required")
        if not asset_id.strip():
            raise StateStoreError("asset_id is required")
        if quantity <= 0:
            raise StateStoreError("quantity must be > 0")

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT quantity
                    FROM player_assets
                    WHERE player_id = ? AND asset_type = ? AND asset_id = ?
                    """,
                    (player_id.strip(), asset_type, asset_id),
                ).fetchone()
                current = int(row["quantity"]) if row is not None else 0
                next_quantity = current + quantity
                conn.execute(
                    """
                    INSERT INTO player_assets (player_id, asset_type, asset_id, quantity, updated_utc)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(player_id, asset_type, asset_id) DO UPDATE SET
                      quantity = excluded.quantity,
                      updated_utc = excluded.updated_utc
                    """,
                    (player_id.strip(), asset_type, asset_id, next_quantity, now),
                )
                conn.commit()

    def adjust_asset_quantity(
        self, player_id: str, asset_type: str, asset_id: str, quantity_delta: int
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not asset_type.strip() or not asset_id.strip():
            raise StateStoreError("asset_type and asset_id are required")
        if quantity_delta == 0:
            rows = self.list_assets(player_id=player_id, asset_type=asset_type, limit=200)
            for row in rows:
                if row.get("asset_id") == asset_id:
                    return row
            return {
                "asset_type": asset_type.strip(),
                "asset_id": asset_id.strip(),
                "quantity": 0,
            }

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT quantity
                    FROM player_assets
                    WHERE player_id = ? AND asset_type = ? AND asset_id = ?
                    """,
                    (player_id.strip(), asset_type.strip(), asset_id.strip()),
                ).fetchone()
                current = int(row["quantity"]) if row is not None else 0
                next_quantity = current + int(quantity_delta)
                if next_quantity < 0:
                    raise StateStoreError(
                        f"Insufficient asset quantity for {asset_type}.{asset_id}"
                    )
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_assets (player_id, asset_type, asset_id, quantity, updated_utc)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            player_id.strip(),
                            asset_type.strip(),
                            asset_id.strip(),
                            next_quantity,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE player_assets
                        SET quantity = ?, updated_utc = ?
                        WHERE player_id = ? AND asset_type = ? AND asset_id = ?
                        """,
                        (
                            next_quantity,
                            now,
                            player_id.strip(),
                            asset_type.strip(),
                            asset_id.strip(),
                        ),
                    )
                if next_quantity == 0:
                    conn.execute(
                        """
                        DELETE FROM player_assets
                        WHERE player_id = ? AND asset_type = ? AND asset_id = ? AND quantity <= 0
                        """,
                        (player_id.strip(), asset_type.strip(), asset_id.strip()),
                    )
                conn.commit()
        rows = self.list_assets(player_id=player_id, asset_type=asset_type, limit=200)
        for row in rows:
            if row.get("asset_id") == asset_id:
                return row
        return {
            "asset_type": asset_type.strip(),
            "asset_id": asset_id.strip(),
            "quantity": 0,
        }

    def list_smuggled_assets(
        self, player_id: str, asset_type: str | None = None, limit: int = 60
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                if asset_type is None:
                    rows = conn.execute(
                        """
                        SELECT asset_type, asset_id, quantity, updated_utc
                        FROM smuggled_assets
                        WHERE player_id = ?
                        ORDER BY updated_utc DESC, asset_type ASC, asset_id ASC
                        LIMIT ?
                        """,
                        (player_id.strip(), int(limit)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT asset_type, asset_id, quantity, updated_utc
                        FROM smuggled_assets
                        WHERE player_id = ? AND asset_type = ?
                        ORDER BY updated_utc DESC, asset_id ASC
                        LIMIT ?
                        """,
                        (player_id.strip(), asset_type.strip(), int(limit)),
                    ).fetchall()
        return [
            {
                "asset_type": row["asset_type"],
                "asset_id": row["asset_id"],
                "quantity": int(row["quantity"]),
                "updated_utc": row["updated_utc"],
            }
            for row in rows
        ]

    def adjust_smuggled_asset_quantity(
        self, player_id: str, asset_type: str, asset_id: str, quantity_delta: int
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not asset_type.strip() or not asset_id.strip():
            raise StateStoreError("asset_type and asset_id are required")
        if quantity_delta == 0:
            rows = self.list_smuggled_assets(player_id=player_id, asset_type=asset_type, limit=200)
            for row in rows:
                if row.get("asset_id") == asset_id:
                    return row
            return {
                "asset_type": asset_type.strip(),
                "asset_id": asset_id.strip(),
                "quantity": 0,
            }

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT quantity
                    FROM smuggled_assets
                    WHERE player_id = ? AND asset_type = ? AND asset_id = ?
                    """,
                    (player_id.strip(), asset_type.strip(), asset_id.strip()),
                ).fetchone()
                current = int(row["quantity"]) if row is not None else 0
                next_quantity = current + int(quantity_delta)
                if next_quantity < 0:
                    raise StateStoreError(
                        f"Insufficient smuggled quantity for {asset_type}.{asset_id}"
                    )
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO smuggled_assets (player_id, asset_type, asset_id, quantity, updated_utc)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            player_id.strip(),
                            asset_type.strip(),
                            asset_id.strip(),
                            next_quantity,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE smuggled_assets
                        SET quantity = ?, updated_utc = ?
                        WHERE player_id = ? AND asset_type = ? AND asset_id = ?
                        """,
                        (
                            next_quantity,
                            now,
                            player_id.strip(),
                            asset_type.strip(),
                            asset_id.strip(),
                        ),
                    )
                if next_quantity == 0:
                    conn.execute(
                        """
                        DELETE FROM smuggled_assets
                        WHERE player_id = ? AND asset_type = ? AND asset_id = ? AND quantity <= 0
                        """,
                        (player_id.strip(), asset_type.strip(), asset_id.strip()),
                    )
                conn.commit()
        rows = self.list_smuggled_assets(player_id=player_id, asset_type=asset_type, limit=200)
        for row in rows:
            if row.get("asset_id") == asset_id:
                return row
        return {
            "asset_type": asset_type.strip(),
            "asset_id": asset_id.strip(),
            "quantity": 0,
        }

    def get_storage_upgrades(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT personal_slots_bonus, smuggle_slots_bonus, updated_utc
                    FROM player_storage_upgrades
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_storage_upgrades (
                          player_id, personal_slots_bonus, smuggle_slots_bonus, updated_utc
                        ) VALUES (?, 0, 0, ?)
                        """,
                        (player_id.strip(), now),
                    )
                    conn.commit()
                    row = conn.execute(
                        """
                        SELECT personal_slots_bonus, smuggle_slots_bonus, updated_utc
                        FROM player_storage_upgrades
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
        if row is None:
            raise StateStoreError("Unable to load storage upgrades")
        return {
            "personal_slots_bonus": round(float(row["personal_slots_bonus"]), 4),
            "smuggle_slots_bonus": round(float(row["smuggle_slots_bonus"]), 4),
            "updated_utc": row["updated_utc"],
        }

    def add_storage_upgrade(
        self,
        *,
        player_id: str,
        personal_slots_delta: float = 0.0,
        smuggle_slots_delta: float = 0.0,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not math.isfinite(float(personal_slots_delta)):
            raise StateStoreError("personal_slots_delta must be finite")
        if not math.isfinite(float(smuggle_slots_delta)):
            raise StateStoreError("smuggle_slots_delta must be finite")
        now = self._utc_now()
        current = self.get_storage_upgrades(player_id=player_id)
        next_personal = max(0.0, float(current["personal_slots_bonus"]) + float(personal_slots_delta))
        next_smuggle = max(0.0, float(current["smuggle_slots_bonus"]) + float(smuggle_slots_delta))
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO player_storage_upgrades (
                      player_id, personal_slots_bonus, smuggle_slots_bonus, updated_utc
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      personal_slots_bonus = excluded.personal_slots_bonus,
                      smuggle_slots_bonus = excluded.smuggle_slots_bonus,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        next_personal,
                        next_smuggle,
                        now,
                    ),
                )
                conn.commit()
        return self.get_storage_upgrades(player_id=player_id)

    def add_crafted_instance(
        self,
        player_id: str,
        asset_type: str,
        asset_id: str,
        quality_payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not asset_type.strip():
            raise StateStoreError("asset_type is required")
        if not asset_id.strip():
            raise StateStoreError("asset_id is required")
        quality_tier = str(quality_payload.get("quality_tier", "standard"))
        quality_score = float(quality_payload.get("quality_score", 1.0))
        stat_multiplier = float(quality_payload.get("stat_multiplier", quality_score))
        instance_id = f"inst.{uuid.uuid4().hex[:16]}"
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload_json = json.dumps(quality_payload, ensure_ascii=True, separators=(",", ":"))

        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO crafted_instances (
                      instance_id, player_id, asset_type, asset_id, quality_tier,
                      quality_score, stat_multiplier, payload_json, created_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        instance_id,
                        player_id.strip(),
                        asset_type.strip(),
                        asset_id.strip(),
                        quality_tier,
                        quality_score,
                        stat_multiplier,
                        payload_json,
                        now,
                    ),
                )
                conn.commit()
        out = dict(quality_payload)
        out["instance_id"] = instance_id
        out["asset_type"] = asset_type.strip()
        out["asset_id"] = asset_id.strip()
        out["created_utc"] = now
        return out

    def list_crafted_instances(
        self,
        player_id: str,
        asset_type: str | None = None,
        asset_id: str | None = None,
        limit: int = 80,
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                clauses = ["player_id = ?"]
                args: list[Any] = [player_id.strip()]
                if asset_type is not None:
                    clauses.append("asset_type = ?")
                    args.append(asset_type.strip())
                if asset_id is not None:
                    clauses.append("asset_id = ?")
                    args.append(asset_id.strip())
                where_clause = " AND ".join(clauses)
                query = (
                    "SELECT instance_id, asset_type, asset_id, quality_tier, quality_score, "
                    "stat_multiplier, payload_json, created_utc "
                    "FROM crafted_instances "
                    f"WHERE {where_clause} "
                    "ORDER BY created_utc DESC "
                    "LIMIT ?"
                )
                args.append(limit)
                rows = conn.execute(query, args).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                payload = {}
            payload.update(
                {
                    "instance_id": row["instance_id"],
                    "asset_type": row["asset_type"],
                    "asset_id": row["asset_id"],
                    "quality_tier": row["quality_tier"],
                    "quality_score": round(float(row["quality_score"]), 4),
                    "stat_multiplier": round(float(row["stat_multiplier"]), 4),
                    "created_utc": row["created_utc"],
                }
            )
            out.append(payload)
        return out

    def get_crafted_instance(self, player_id: str, instance_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not instance_id.strip():
            raise StateStoreError("instance_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT instance_id, asset_type, asset_id, quality_tier, quality_score,
                           stat_multiplier, payload_json, created_utc
                    FROM crafted_instances
                    WHERE player_id = ? AND instance_id = ?
                    """,
                    (player_id.strip(), instance_id.strip()),
                ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown crafted instance '{instance_id}'")
        payload = {}
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            payload = {}
        payload.update(
            {
                "instance_id": row["instance_id"],
                "asset_type": row["asset_type"],
                "asset_id": row["asset_id"],
                "quality_tier": row["quality_tier"],
                "quality_score": round(float(row["quality_score"]), 4),
                "stat_multiplier": round(float(row["stat_multiplier"]), 4),
                "created_utc": row["created_utc"],
            }
        )
        return payload

    def get_crafted_instance_any(self, instance_id: str) -> dict[str, Any]:
        if not instance_id.strip():
            raise StateStoreError("instance_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT instance_id, player_id, asset_type, asset_id, quality_tier, quality_score,
                           stat_multiplier, payload_json, created_utc
                    FROM crafted_instances
                    WHERE instance_id = ?
                    """,
                    (instance_id.strip(),),
                ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown crafted instance '{instance_id}'")
        payload = {}
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            payload = {}
        payload.update(
            {
                "instance_id": row["instance_id"],
                "player_id": row["player_id"],
                "asset_type": row["asset_type"],
                "asset_id": row["asset_id"],
                "quality_tier": row["quality_tier"],
                "quality_score": round(float(row["quality_score"]), 4),
                "stat_multiplier": round(float(row["stat_multiplier"]), 4),
                "created_utc": row["created_utc"],
            }
        )
        return payload

    def transfer_crafted_instance_owner(
        self,
        instance_id: str,
        from_player_id: str,
        to_player_id: str,
    ) -> dict[str, Any]:
        if not instance_id.strip():
            raise StateStoreError("instance_id is required")
        if not from_player_id.strip() or not to_player_id.strip():
            raise StateStoreError("from_player_id and to_player_id are required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=from_player_id.strip())
                self._assert_profile_exists(conn, player_id=to_player_id.strip())
                row = conn.execute(
                    """
                    SELECT player_id
                    FROM crafted_instances
                    WHERE instance_id = ?
                    """,
                    (instance_id.strip(),),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown crafted instance '{instance_id}'")
                owner = row["player_id"]
                if owner != from_player_id.strip():
                    raise StateStoreError(
                        f"crafted instance '{instance_id}' is not owned by '{from_player_id}'"
                    )
                conn.execute(
                    """
                    UPDATE crafted_instances
                    SET player_id = ?
                    WHERE instance_id = ?
                    """,
                    (to_player_id.strip(), instance_id.strip()),
                )
                conn.commit()
        return self.get_crafted_instance(player_id=to_player_id.strip(), instance_id=instance_id.strip())

    def update_crafted_instance(
        self,
        player_id: str,
        instance_id: str,
        payload: dict[str, Any],
        quality_tier: str | None = None,
        quality_score: float | None = None,
        stat_multiplier: float | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not instance_id.strip():
            raise StateStoreError("instance_id is required")
        if not isinstance(payload, dict):
            raise StateStoreError("payload must be an object")
        current = self.get_crafted_instance(player_id=player_id, instance_id=instance_id)
        next_quality_tier = (
            quality_tier
            if isinstance(quality_tier, str) and quality_tier.strip()
            else str(current.get("quality_tier", "standard"))
        )
        if quality_score is None:
            next_quality_score = float(current.get("quality_score", 1.0))
        else:
            next_quality_score = float(quality_score)
        if stat_multiplier is None:
            next_stat_multiplier = float(current.get("stat_multiplier", next_quality_score))
        else:
            next_stat_multiplier = float(stat_multiplier)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE crafted_instances
                    SET quality_tier = ?, quality_score = ?, stat_multiplier = ?, payload_json = ?
                    WHERE player_id = ? AND instance_id = ?
                    """,
                    (
                        next_quality_tier,
                        next_quality_score,
                        next_stat_multiplier,
                        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                        player_id.strip(),
                        instance_id.strip(),
                    ),
                )
                conn.commit()
        return self.get_crafted_instance(player_id=player_id, instance_id=instance_id)

    def delete_crafted_instance(self, player_id: str, instance_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not instance_id.strip():
            raise StateStoreError("instance_id is required")
        instance = self.get_crafted_instance(player_id=player_id, instance_id=instance_id)
        with self._lock:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    DELETE FROM crafted_instances
                    WHERE player_id = ? AND instance_id = ?
                    """,
                    (player_id.strip(), instance_id.strip()),
                )
                conn.commit()
        deleted = int(cursor.rowcount or 0) > 0
        return {
            "player_id": player_id.strip(),
            "instance_id": instance_id.strip(),
            "deleted": deleted,
            "instance": instance,
        }

    def count_crafted_instances(self, player_id: str) -> int:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM crafted_instances
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
        return int(row["c"]) if row is not None else 0

    def set_wallet_balances(self, player_id: str, credits: float, voidcoin: float) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not math.isfinite(float(credits)) or not math.isfinite(float(voidcoin)):
            raise StateStoreError("wallet balances must be finite")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO wallets (player_id, credits, voidcoin, updated_utc)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      credits = excluded.credits,
                      voidcoin = excluded.voidcoin,
                      updated_utc = excluded.updated_utc
                    """,
                    (player_id.strip(), max(0.0, float(credits)), max(0.0, float(voidcoin)), now),
                )
                conn.commit()
        return self.get_wallet(player_id=player_id)

    def set_inventory_floor(self, player_id: str, symbol_floors: dict[str, float]) -> dict[str, float]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                for symbol, floor_value in symbol_floors.items():
                    if not isinstance(symbol, str) or not symbol.strip():
                        raise StateStoreError("symbol keys must be non-empty strings")
                    if isinstance(floor_value, bool) or not isinstance(floor_value, (int, float)):
                        raise StateStoreError("floor values must be numeric")
                    floor = max(0.0, float(floor_value))
                    row = conn.execute(
                        """
                        SELECT amount
                        FROM element_inventory
                        WHERE player_id = ? AND symbol = ?
                        """,
                        (player_id.strip(), symbol.strip()),
                    ).fetchone()
                    current = float(row["amount"]) if row is not None else 0.0
                    if current >= floor:
                        continue
                    if row is None:
                        conn.execute(
                            """
                            INSERT INTO element_inventory (player_id, symbol, amount, updated_utc)
                            VALUES (?, ?, ?, ?)
                            """,
                            (player_id.strip(), symbol.strip(), floor, now),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE element_inventory
                            SET amount = ?, updated_utc = ?
                            WHERE player_id = ? AND symbol = ?
                            """,
                            (floor, now, player_id.strip(), symbol.strip()),
                        )
                conn.commit()
        return self.get_inventory_amounts(player_id=player_id)

    def list_unlocked_tech(self, player_id: str, limit: int | None = None) -> list[str]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        resolved_limit = None
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise StateStoreError("limit must be an integer when provided")
            if limit <= 0:
                raise StateStoreError("limit must be > 0 when provided")
            resolved_limit = limit
        with self._lock:
            with self._connect() as conn:
                if resolved_limit is None:
                    rows = conn.execute(
                        """
                        SELECT tech_id
                        FROM research_unlocks
                        WHERE player_id = ?
                        ORDER BY tech_id
                        """,
                        (player_id.strip(),),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT tech_id
                        FROM research_unlocks
                        WHERE player_id = ?
                        ORDER BY tech_id
                        LIMIT ?
                        """,
                        (player_id.strip(), resolved_limit),
                    ).fetchall()
        return [row["tech_id"] for row in rows]

    def unlock_tech(self, player_id: str, tech_id: str) -> None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not tech_id.strip():
            raise StateStoreError("tech_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO research_unlocks (player_id, tech_id, unlocked_utc)
                    VALUES (?, ?, ?)
                    """,
                    (player_id.strip(), tech_id.strip(), now),
                )
                conn.commit()

    def is_tech_unlocked(self, player_id: str, tech_id: str) -> bool:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not tech_id.strip():
            raise StateStoreError("tech_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM research_unlocks
                    WHERE player_id = ? AND tech_id = ?
                    """,
                    (player_id.strip(), tech_id.strip()),
                ).fetchone()
        return row is not None

    @staticmethod
    def _utc_now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    @staticmethod
    def _utc_to_epoch(value: str) -> int:
        return int(calendar.timegm(time.strptime(value, "%Y-%m-%dT%H:%M:%SZ")))

    def start_research_job(
        self,
        player_id: str,
        tech_id: str,
        required_compute: float,
        compute_power_per_hour: float,
        duration_seconds: int,
        cost_payload: dict[str, Any],
        substitution_id: str | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not tech_id.strip():
            raise StateStoreError("tech_id is required")
        if required_compute <= 0:
            raise StateStoreError("required_compute must be > 0")
        if compute_power_per_hour <= 0:
            raise StateStoreError("compute_power_per_hour must be > 0")
        if duration_seconds <= 0:
            raise StateStoreError("duration_seconds must be > 0")

        job_id = f"rjob.{uuid.uuid4().hex[:16]}"
        started_utc = self._utc_now()
        completes_epoch = int(time.time()) + duration_seconds
        completes_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(completes_epoch))
        now = started_utc

        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                unlocked_row = conn.execute(
                    """
                    SELECT 1
                    FROM research_unlocks
                    WHERE player_id = ? AND tech_id = ?
                    """,
                    (player_id.strip(), tech_id.strip()),
                ).fetchone()
                if unlocked_row is not None:
                    raise StateStoreError(f"Tech '{tech_id}' is already unlocked")
                existing = conn.execute(
                    """
                    SELECT 1
                    FROM research_jobs
                    WHERE player_id = ? AND tech_id = ? AND status IN ('active', 'completed')
                    """,
                    (player_id.strip(), tech_id.strip()),
                ).fetchone()
                if existing is not None:
                    raise StateStoreError(
                        f"Research job already exists for tech '{tech_id}'"
                    )
                conn.execute(
                    """
                    INSERT INTO research_jobs (
                      job_id, player_id, tech_id, status, required_compute,
                      compute_power_per_hour, duration_seconds, started_utc, completes_utc,
                      substitution_id, cost_json, updated_utc
                    ) VALUES (?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        player_id.strip(),
                        tech_id.strip(),
                        float(required_compute),
                        float(compute_power_per_hour),
                        int(duration_seconds),
                        started_utc,
                        completes_utc,
                        substitution_id.strip() if isinstance(substitution_id, str) else None,
                        json.dumps(cost_payload, ensure_ascii=True, separators=(",", ":")),
                        now,
                    ),
                )
                conn.commit()
        return self.get_research_job(player_id=player_id, job_id=job_id)

    def _sync_research_job_statuses(self, conn: sqlite3.Connection, player_id: str) -> None:
        now = self._utc_now()
        conn.execute(
            """
            UPDATE research_jobs
            SET status = 'completed',
                updated_utc = ?
            WHERE player_id = ?
              AND status = 'active'
              AND completes_utc <= ?
            """,
            (now, player_id.strip(), now),
        )

    def list_research_jobs(
        self, player_id: str, status: str | None = None, limit: int = 40
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        if status is not None and status not in {"active", "completed", "claimed"}:
            raise StateStoreError("status must be one of: active, completed, claimed")

        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_research_job_statuses(conn, player_id=player_id)
                if status is None:
                    rows = conn.execute(
                        """
                        SELECT job_id, tech_id, status, required_compute, compute_power_per_hour,
                               duration_seconds, started_utc, completes_utc, substitution_id, cost_json,
                               updated_utc
                        FROM research_jobs
                        WHERE player_id = ?
                        ORDER BY
                          CASE status WHEN 'active' THEN 0 WHEN 'completed' THEN 1 ELSE 2 END ASC,
                          updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT job_id, tech_id, status, required_compute, compute_power_per_hour,
                               duration_seconds, started_utc, completes_utc, substitution_id, cost_json,
                               updated_utc
                        FROM research_jobs
                        WHERE player_id = ? AND status = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), status, limit),
                    ).fetchall()
                conn.commit()

        return [self._research_row_to_payload(dict(row)) for row in rows]

    def get_research_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not job_id.strip():
            raise StateStoreError("job_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_research_job_statuses(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT job_id, tech_id, status, required_compute, compute_power_per_hour,
                           duration_seconds, started_utc, completes_utc, substitution_id, cost_json,
                           updated_utc
                    FROM research_jobs
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (player_id.strip(), job_id.strip()),
                ).fetchone()
                conn.commit()
        if row is None:
            raise StateStoreError(f"Unknown research job '{job_id}'")
        return self._research_row_to_payload(dict(row))

    def _research_row_to_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        now_epoch = int(time.time())
        completes_epoch = self._utc_to_epoch(str(row["completes_utc"]))
        remaining = max(0, completes_epoch - now_epoch)
        try:
            cost_payload = json.loads(str(row.get("cost_json", "{}")))
        except json.JSONDecodeError:
            cost_payload = {}
        return {
            "job_id": row["job_id"],
            "tech_id": row["tech_id"],
            "status": row["status"],
            "required_compute": round(float(row["required_compute"]), 4),
            "compute_power_per_hour": round(float(row["compute_power_per_hour"]), 4),
            "duration_seconds": int(row["duration_seconds"]),
            "started_utc": row["started_utc"],
            "completes_utc": row["completes_utc"],
            "remaining_seconds": int(remaining),
            "substitution_id": row["substitution_id"],
            "cost": cost_payload,
            "updated_utc": row["updated_utc"],
        }

    def claim_research_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not job_id.strip():
            raise StateStoreError("job_id is required")

        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_research_job_statuses(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT job_id, tech_id, status, required_compute, compute_power_per_hour,
                           duration_seconds, started_utc, completes_utc, substitution_id, cost_json,
                           updated_utc
                    FROM research_jobs
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (player_id.strip(), job_id.strip()),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown research job '{job_id}'")

                payload = self._research_row_to_payload(dict(row))
                status = str(row["status"])
                if status == "claimed":
                    return payload
                if status != "completed" and int(payload["remaining_seconds"]) > 0:
                    raise StateStoreError(
                        f"Research job '{job_id}' is not complete yet "
                        f"({payload['remaining_seconds']}s remaining)"
                    )

                now = self._utc_now()
                conn.execute(
                    """
                    UPDATE research_jobs
                    SET status = 'claimed', updated_utc = ?
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (now, player_id.strip(), job_id.strip()),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO research_unlocks (player_id, tech_id, unlocked_utc)
                    VALUES (?, ?, ?)
                    """,
                    (player_id.strip(), str(row["tech_id"]), now),
                )
                conn.commit()
        return self.get_research_job(player_id=player_id, job_id=job_id)

    def _sync_generic_job_statuses(
        self,
        conn: sqlite3.Connection,
        table: str,
        player_id: str,
    ) -> None:
        now = self._utc_now()
        conn.execute(
            f"""
            UPDATE {table}
            SET status = 'completed',
                updated_utc = ?
            WHERE player_id = ?
              AND status = 'active'
              AND completes_utc <= ?
            """,
            (now, player_id.strip(), now),
        )

    def start_manufacturing_job(
        self,
        player_id: str,
        item_id: str,
        quantity: int,
        profile_id: str,
        workload: float,
        throughput_per_hour: float,
        duration_seconds: int,
        cost_payload: dict[str, Any],
        world_id: str | None = None,
        substitution_id: str | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if quantity <= 0:
            raise StateStoreError("quantity must be > 0")
        if workload <= 0 or throughput_per_hour <= 0 or duration_seconds <= 0:
            raise StateStoreError("manufacturing runtime values must be > 0")
        job_id = f"mjob.{uuid.uuid4().hex[:16]}"
        started_utc = self._utc_now()
        completes_epoch = int(time.time()) + duration_seconds
        completes_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(completes_epoch))
        now = started_utc
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO manufacturing_jobs (
                      job_id, player_id, item_id, quantity, status, profile_id, workload,
                      throughput_per_hour, duration_seconds, started_utc, completes_utc,
                      world_id, substitution_id, cost_json, updated_utc
                    ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        player_id.strip(),
                        item_id.strip(),
                        int(quantity),
                        profile_id.strip(),
                        float(workload),
                        float(throughput_per_hour),
                        int(duration_seconds),
                        started_utc,
                        completes_utc,
                        world_id.strip() if isinstance(world_id, str) else None,
                        substitution_id.strip() if isinstance(substitution_id, str) else None,
                        json.dumps(cost_payload, ensure_ascii=True, separators=(",", ":")),
                        now,
                    ),
                )
                conn.commit()
        return self.get_manufacturing_job(player_id=player_id, job_id=job_id)

    def _manufacturing_row_to_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        now_epoch = int(time.time())
        completes_epoch = self._utc_to_epoch(str(row["completes_utc"]))
        remaining = max(0, completes_epoch - now_epoch)
        try:
            cost_payload = json.loads(str(row.get("cost_json", "{}")))
        except json.JSONDecodeError:
            cost_payload = {}
        return {
            "job_id": row["job_id"],
            "item_id": row["item_id"],
            "quantity": int(row["quantity"]),
            "status": row["status"],
            "profile_id": row["profile_id"],
            "workload": round(float(row["workload"]), 4),
            "throughput_per_hour": round(float(row["throughput_per_hour"]), 4),
            "duration_seconds": int(row["duration_seconds"]),
            "started_utc": row["started_utc"],
            "completes_utc": row["completes_utc"],
            "remaining_seconds": int(remaining),
            "world_id": row.get("world_id"),
            "substitution_id": row.get("substitution_id"),
            "cost": cost_payload,
            "updated_utc": row["updated_utc"],
        }

    def list_manufacturing_jobs(
        self, player_id: str, status: str | None = None, limit: int = 40
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_generic_job_statuses(conn, "manufacturing_jobs", player_id=player_id)
                if status is None:
                    rows = conn.execute(
                        """
                        SELECT job_id, item_id, quantity, status, profile_id, workload,
                               throughput_per_hour, duration_seconds, started_utc, completes_utc,
                               world_id, substitution_id, cost_json, updated_utc
                        FROM manufacturing_jobs
                        WHERE player_id = ?
                        ORDER BY
                          CASE status WHEN 'active' THEN 0 WHEN 'completed' THEN 1 ELSE 2 END ASC,
                          updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT job_id, item_id, quantity, status, profile_id, workload,
                               throughput_per_hour, duration_seconds, started_utc, completes_utc,
                               world_id, substitution_id, cost_json, updated_utc
                        FROM manufacturing_jobs
                        WHERE player_id = ? AND status = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), status, limit),
                    ).fetchall()
                conn.commit()
        return [self._manufacturing_row_to_payload(dict(row)) for row in rows]

    def get_manufacturing_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        if not player_id.strip() or not job_id.strip():
            raise StateStoreError("player_id and job_id are required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_generic_job_statuses(conn, "manufacturing_jobs", player_id=player_id)
                row = conn.execute(
                    """
                    SELECT job_id, item_id, quantity, status, profile_id, workload,
                           throughput_per_hour, duration_seconds, started_utc, completes_utc,
                           world_id, substitution_id, cost_json, updated_utc
                    FROM manufacturing_jobs
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (player_id.strip(), job_id.strip()),
                ).fetchone()
                conn.commit()
        if row is None:
            raise StateStoreError(f"Unknown manufacturing job '{job_id}'")
        return self._manufacturing_row_to_payload(dict(row))

    def claim_manufacturing_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        payload = self.get_manufacturing_job(player_id=player_id, job_id=job_id)
        if payload["status"] == "claimed":
            return payload
        if payload["status"] != "completed" and payload["remaining_seconds"] > 0:
            raise StateStoreError(
                f"Manufacturing job '{job_id}' is not complete yet ({payload['remaining_seconds']}s remaining)"
            )
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE manufacturing_jobs
                    SET status = 'claimed', updated_utc = ?
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (now, player_id.strip(), job_id.strip()),
                )
                conn.commit()
        return self.get_manufacturing_job(player_id=player_id, job_id=job_id)

    def cancel_manufacturing_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        payload = self.get_manufacturing_job(player_id=player_id, job_id=job_id)
        if payload["status"] in {"claimed", "cancelled"}:
            return payload
        if payload["status"] == "completed":
            raise StateStoreError("Completed manufacturing jobs cannot be cancelled")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE manufacturing_jobs
                    SET status = 'cancelled', updated_utc = ?
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (now, player_id.strip(), job_id.strip()),
                )
                conn.commit()
        return self.get_manufacturing_job(player_id=player_id, job_id=job_id)

    def start_reverse_job(
        self,
        player_id: str,
        recipe_id: str,
        target_item_id: str,
        consumable_id: str,
        unlock_blueprint_id: str,
        compute_cost: float,
        duration_seconds: int,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if duration_seconds <= 0:
            raise StateStoreError("duration_seconds must be > 0")
        job_id = f"rjobx.{uuid.uuid4().hex[:16]}"
        started_utc = self._utc_now()
        completes_epoch = int(time.time()) + duration_seconds
        completes_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(completes_epoch))
        now = started_utc
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO reverse_jobs (
                      job_id, player_id, recipe_id, target_item_id, status, compute_cost,
                      duration_seconds, started_utc, completes_utc, consumable_id,
                      unlock_blueprint_id, updated_utc
                    ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        player_id.strip(),
                        recipe_id.strip(),
                        target_item_id.strip(),
                        float(compute_cost),
                        int(duration_seconds),
                        started_utc,
                        completes_utc,
                        consumable_id.strip(),
                        unlock_blueprint_id.strip(),
                        now,
                    ),
                )
                conn.commit()
        return self.get_reverse_job(player_id=player_id, job_id=job_id)

    def _reverse_row_to_payload(self, row: dict[str, Any]) -> dict[str, Any]:
        now_epoch = int(time.time())
        completes_epoch = self._utc_to_epoch(str(row["completes_utc"]))
        remaining = max(0, completes_epoch - now_epoch)
        return {
            "job_id": row["job_id"],
            "recipe_id": row["recipe_id"],
            "target_item_id": row["target_item_id"],
            "status": row["status"],
            "compute_cost": round(float(row["compute_cost"]), 4),
            "duration_seconds": int(row["duration_seconds"]),
            "started_utc": row["started_utc"],
            "completes_utc": row["completes_utc"],
            "remaining_seconds": int(remaining),
            "consumable_id": row["consumable_id"],
            "unlock_blueprint_id": row["unlock_blueprint_id"],
            "updated_utc": row["updated_utc"],
        }

    def list_reverse_jobs(
        self, player_id: str, status: str | None = None, limit: int = 40
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_generic_job_statuses(conn, "reverse_jobs", player_id=player_id)
                if status is None:
                    rows = conn.execute(
                        """
                        SELECT job_id, recipe_id, target_item_id, status, compute_cost,
                               duration_seconds, started_utc, completes_utc, consumable_id,
                               unlock_blueprint_id, updated_utc
                        FROM reverse_jobs
                        WHERE player_id = ?
                        ORDER BY
                          CASE status WHEN 'active' THEN 0 WHEN 'completed' THEN 1 ELSE 2 END ASC,
                          updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT job_id, recipe_id, target_item_id, status, compute_cost,
                               duration_seconds, started_utc, completes_utc, consumable_id,
                               unlock_blueprint_id, updated_utc
                        FROM reverse_jobs
                        WHERE player_id = ? AND status = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), status, limit),
                    ).fetchall()
                conn.commit()
        return [self._reverse_row_to_payload(dict(row)) for row in rows]

    def get_reverse_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        if not player_id.strip() or not job_id.strip():
            raise StateStoreError("player_id and job_id are required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_generic_job_statuses(conn, "reverse_jobs", player_id=player_id)
                row = conn.execute(
                    """
                    SELECT job_id, recipe_id, target_item_id, status, compute_cost,
                           duration_seconds, started_utc, completes_utc, consumable_id,
                           unlock_blueprint_id, updated_utc
                    FROM reverse_jobs
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (player_id.strip(), job_id.strip()),
                ).fetchone()
                conn.commit()
        if row is None:
            raise StateStoreError(f"Unknown reverse-engineering job '{job_id}'")
        return self._reverse_row_to_payload(dict(row))

    def claim_reverse_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        payload = self.get_reverse_job(player_id=player_id, job_id=job_id)
        if payload["status"] == "claimed":
            return payload
        if payload["status"] != "completed" and payload["remaining_seconds"] > 0:
            raise StateStoreError(
                f"Reverse-engineering job '{job_id}' is not complete yet ({payload['remaining_seconds']}s remaining)"
            )
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE reverse_jobs
                    SET status = 'claimed', updated_utc = ?
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (now, player_id.strip(), job_id.strip()),
                )
                conn.commit()
        return self.get_reverse_job(player_id=player_id, job_id=job_id)

    def cancel_reverse_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        payload = self.get_reverse_job(player_id=player_id, job_id=job_id)
        if payload["status"] in {"claimed", "cancelled"}:
            return payload
        if payload["status"] == "completed":
            raise StateStoreError("Completed reverse-engineering jobs cannot be cancelled")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE reverse_jobs
                    SET status = 'cancelled', updated_utc = ?
                    WHERE player_id = ? AND job_id = ?
                    """,
                    (now, player_id.strip(), job_id.strip()),
                )
                conn.commit()
        return self.get_reverse_job(player_id=player_id, job_id=job_id)

    def ensure_fleet_state(
        self,
        player_id: str,
        active_hull_id: str,
        crew_total: float,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    "SELECT player_id FROM fleet_state WHERE player_id = ?",
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO fleet_state (
                          player_id, active_hull_id, hull_durability, ship_level, ship_xp,
                          crew_total, crew_elite, cargo_json, updated_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            player_id.strip(),
                            active_hull_id.strip(),
                            100.0,
                            1,
                            0.0,
                            max(0.0, float(crew_total)),
                            max(0.0, float(crew_total) * 0.08),
                            json.dumps({}, ensure_ascii=True),
                            now,
                        ),
                    )
                    conn.commit()
        return self.get_fleet_state(player_id=player_id)

    def get_fleet_state(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT player_id, active_hull_id, hull_durability, ship_level, ship_xp,
                           crew_total, crew_elite, cargo_json, updated_utc
                    FROM fleet_state
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
        if row is None:
            raise StateStoreError("Fleet state not found")
        cargo = {}
        try:
            cargo = json.loads(str(row["cargo_json"]))
        except json.JSONDecodeError:
            cargo = {}
        level_raw = row["ship_level"]
        ship_level = int(level_raw) if isinstance(level_raw, int) else 1
        ship_level = max(1, ship_level)
        ship_xp_raw = row["ship_xp"]
        ship_xp = (
            float(ship_xp_raw)
            if isinstance(ship_xp_raw, (int, float)) and not isinstance(ship_xp_raw, bool)
            else 0.0
        )
        ship_xp = max(0.0, ship_xp)
        ship_xp_to_next = self._ship_xp_to_next_level(ship_level)
        return {
            "player_id": row["player_id"],
            "active_hull_id": row["active_hull_id"],
            "hull_durability": round(float(row["hull_durability"]), 4),
            "ship_level": ship_level,
            "ship_xp": round(ship_xp, 4),
            "ship_xp_to_next_level": round(ship_xp_to_next, 4),
            "ship_xp_progress_ratio": round(
                min(1.0, ship_xp / max(1.0, ship_xp_to_next)),
                5,
            ),
            "crew_total": round(float(row["crew_total"]), 4),
            "crew_elite": round(float(row["crew_elite"]), 4),
            "cargo": cargo if isinstance(cargo, dict) else {},
            "updated_utc": row["updated_utc"],
        }

    def update_fleet_state(
        self,
        player_id: str,
        active_hull_id: str | None = None,
        hull_durability: float | None = None,
        ship_level: int | None = None,
        ship_xp: float | None = None,
        crew_total: float | None = None,
        crew_elite: float | None = None,
        cargo: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        current = self.get_fleet_state(player_id=player_id)
        next_payload = {
            "active_hull_id": current["active_hull_id"],
            "hull_durability": float(current["hull_durability"]),
            "ship_level": int(current.get("ship_level", 1)),
            "ship_xp": float(current.get("ship_xp", 0.0)),
            "crew_total": float(current["crew_total"]),
            "crew_elite": float(current["crew_elite"]),
            "cargo": dict(current["cargo"]),
        }
        if isinstance(active_hull_id, str) and active_hull_id.strip():
            next_payload["active_hull_id"] = active_hull_id.strip()
        if hull_durability is not None:
            next_payload["hull_durability"] = max(0.0, min(100.0, float(hull_durability)))
        if ship_level is not None:
            next_payload["ship_level"] = max(1, int(ship_level))
        if ship_xp is not None:
            next_payload["ship_xp"] = max(0.0, float(ship_xp))
        if crew_total is not None:
            next_payload["crew_total"] = max(0.0, float(crew_total))
        if crew_elite is not None:
            next_payload["crew_elite"] = max(0.0, float(crew_elite))
        if isinstance(cargo, dict):
            clean: dict[str, float] = {}
            for symbol, amount in cargo.items():
                if not isinstance(symbol, str):
                    continue
                if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                    continue
                clean[symbol] = max(0.0, float(amount))
            next_payload["cargo"] = clean
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE fleet_state
                    SET active_hull_id = ?, hull_durability = ?, ship_level = ?, ship_xp = ?,
                        crew_total = ?, crew_elite = ?, cargo_json = ?, updated_utc = ?
                    WHERE player_id = ?
                    """,
                    (
                        next_payload["active_hull_id"],
                        next_payload["hull_durability"],
                        next_payload["ship_level"],
                        next_payload["ship_xp"],
                        next_payload["crew_total"],
                        next_payload["crew_elite"],
                        json.dumps(next_payload["cargo"], ensure_ascii=True),
                        now,
                        player_id.strip(),
                    ),
                )
                conn.commit()
        return self.get_fleet_state(player_id=player_id)

    def _ship_xp_to_next_level(self, level: int) -> float:
        level_value = max(1, int(level))
        # Near-forever progression: super-linear base with a late-game tail increase.
        base = 120.0 * (float(level_value) ** 1.35)
        high_level_tail = 1.0 + (0.011 * (max(0.0, float(level_value - 18)) ** 0.72))
        return max(60.0, base * high_level_tail)

    def grant_fleet_xp(self, player_id: str, xp_delta: float) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not math.isfinite(float(xp_delta)):
            raise StateStoreError("xp_delta must be finite")
        xp_delta = max(0.0, float(xp_delta))
        fleet = self.get_fleet_state(player_id=player_id)
        current_level = max(1, int(fleet.get("ship_level", 1)))
        current_xp = max(0.0, float(fleet.get("ship_xp", 0.0)))
        pool = current_xp + xp_delta
        levels_gained = 0
        while True:
            needed = self._ship_xp_to_next_level(current_level)
            if pool + 1e-9 < needed:
                break
            pool -= needed
            current_level += 1
            levels_gained += 1
            if current_level >= 250_000:
                pool = 0.0
                break
        updated = self.update_fleet_state(
            player_id=player_id,
            ship_level=current_level,
            ship_xp=pool,
        )
        updated["ship_progress"] = {
            "xp_awarded": round(xp_delta, 4),
            "levels_gained": int(levels_gained),
            "level_before": int(fleet.get("ship_level", 1)),
            "level_after": int(updated.get("ship_level", current_level)),
        }
        return updated

    def apply_fleet_combat_losses(
        self,
        player_id: str,
        hull_durability_loss: float,
        crew_casualties: float,
        cargo_loss_ratio: float,
    ) -> dict[str, Any]:
        current = self.get_fleet_state(player_id=player_id)
        crew_before = float(current["crew_total"])
        elite_before = float(current["crew_elite"])
        crew_after = max(0.0, crew_before - max(0.0, float(crew_casualties)))
        elite_after = 0.0
        if crew_before > 1e-9:
            elite_after = min(crew_after, max(0.0, elite_before * (crew_after / crew_before)))
        ratio = max(0.0, min(0.95, float(cargo_loss_ratio)))
        cargo_after: dict[str, float] = {}
        for symbol, amount in current["cargo"].items():
            if isinstance(symbol, str) and isinstance(amount, (int, float)) and not isinstance(amount, bool):
                cargo_after[symbol] = max(0.0, float(amount) * (1.0 - ratio))
        return self.update_fleet_state(
            player_id=player_id,
            hull_durability=max(
                0.0,
                float(current["hull_durability"]) - max(0.0, float(hull_durability_loss)),
            ),
            crew_total=crew_after,
            crew_elite=elite_after,
            cargo=cargo_after,
        )

    def create_listing(
        self,
        seller_player_id: str,
        asset_type: str,
        asset_id: str,
        quantity: float,
        currency: str,
        unit_price: float,
        region_id: str | None,
        ttl_hours: float,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        listing_id = f"listing.{uuid.uuid4().hex[:16]}"
        now = self._utc_now()
        expires_epoch = int(time.time() + max(1.0, ttl_hours) * 3600.0)
        expires_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_epoch))
        metadata_payload = metadata if isinstance(metadata, dict) else {}
        metadata_json = json.dumps(metadata_payload, ensure_ascii=True, separators=(",", ":"))
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=seller_player_id)
                conn.execute(
                    """
                    INSERT INTO market_listings (
                      listing_id, seller_player_id, asset_type, asset_id, quantity, quantity_remaining,
                      currency, unit_price, region_id, status, expires_utc, created_utc, metadata_json, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                    """,
                    (
                        listing_id,
                        seller_player_id.strip(),
                        asset_type.strip(),
                        asset_id.strip(),
                        float(quantity),
                        float(quantity),
                        currency.strip(),
                        float(unit_price),
                        region_id.strip() if isinstance(region_id, str) else None,
                        expires_utc,
                        now,
                        metadata_json,
                        now,
                    ),
                )
                conn.commit()
        return self.get_listing(listing_id=listing_id)

    def _decode_listing_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        metadata_json = payload.get("metadata_json")
        metadata: dict[str, Any] = {}
        if isinstance(metadata_json, str) and metadata_json.strip():
            try:
                parsed = json.loads(metadata_json)
                if isinstance(parsed, dict):
                    metadata = parsed
            except json.JSONDecodeError:
                metadata = {}
        payload["metadata"] = metadata
        payload.pop("metadata_json", None)
        return payload

    def _sync_listing_statuses(self, conn: sqlite3.Connection) -> None:
        now = self._utc_now()
        conn.execute(
            """
            UPDATE market_listings
            SET status = 'expired', updated_utc = ?
            WHERE status = 'active'
              AND expires_utc <= ?
            """,
            (now, now),
        )
        conn.execute(
            """
            UPDATE market_listings
            SET status = 'filled', updated_utc = ?
            WHERE status = 'active' AND quantity_remaining <= 0.0000001
            """,
            (now,),
        )

    def list_listings(
        self,
        limit: int = 60,
        asset_type: str | None = None,
        asset_id: str | None = None,
        region_id: str | None = None,
        seller_player_id: str | None = None,
        status: str = "active",
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._sync_listing_statuses(conn)
                clauses = ["status = ?"]
                args: list[Any] = [status]
                if asset_type is not None:
                    clauses.append("asset_type = ?")
                    args.append(asset_type.strip())
                if asset_id is not None:
                    clauses.append("asset_id = ?")
                    args.append(asset_id.strip())
                if region_id is not None:
                    clauses.append("region_id = ?")
                    args.append(region_id.strip())
                if seller_player_id is not None:
                    clauses.append("seller_player_id = ?")
                    args.append(seller_player_id.strip())
                where_clause = " AND ".join(clauses)
                query = (
                    "SELECT listing_id, seller_player_id, asset_type, asset_id, quantity, quantity_remaining, "
                    "currency, unit_price, region_id, status, expires_utc, created_utc, metadata_json, updated_utc "
                    "FROM market_listings "
                    f"WHERE {where_clause} "
                    "ORDER BY created_utc DESC "
                    "LIMIT ?"
                )
                args.append(limit)
                rows = conn.execute(query, args).fetchall()
                conn.commit()
        return [self._decode_listing_row(row) for row in rows]

    def get_listing(self, listing_id: str) -> dict[str, Any]:
        if not listing_id.strip():
            raise StateStoreError("listing_id is required")
        with self._lock:
            with self._connect() as conn:
                self._sync_listing_statuses(conn)
                row = conn.execute(
                    """
                    SELECT listing_id, seller_player_id, asset_type, asset_id, quantity, quantity_remaining,
                           currency, unit_price, region_id, status, expires_utc, created_utc, metadata_json, updated_utc
                    FROM market_listings
                    WHERE listing_id = ?
                    """,
                    (listing_id.strip(),),
                ).fetchone()
                conn.commit()
        if row is None:
            raise StateStoreError(f"Unknown listing '{listing_id}'")
        return self._decode_listing_row(row)

    def update_listing_remaining(self, listing_id: str, remaining: float, status: str | None = None) -> dict[str, Any]:
        now = self._utc_now()
        next_status = status
        if next_status is None:
            next_status = "filled" if remaining <= 0.0000001 else "active"
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE market_listings
                    SET quantity_remaining = ?, status = ?, updated_utc = ?
                    WHERE listing_id = ?
                    """,
                    (max(0.0, float(remaining)), next_status, now, listing_id.strip()),
                )
                conn.commit()
        return self.get_listing(listing_id=listing_id)

    def cancel_listing(self, seller_player_id: str, listing_id: str) -> dict[str, Any]:
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT seller_player_id FROM market_listings WHERE listing_id = ?",
                    (listing_id.strip(),),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown listing '{listing_id}'")
                if row["seller_player_id"] != seller_player_id.strip():
                    raise StateStoreError("Cannot cancel listing owned by another player")
                conn.execute(
                    """
                    UPDATE market_listings
                    SET status = 'cancelled', updated_utc = ?
                    WHERE listing_id = ?
                    """,
                    (now, listing_id.strip()),
                )
                conn.commit()
        return self.get_listing(listing_id=listing_id)

    def record_market_trade(
        self,
        trade_source: str,
        asset_type: str,
        asset_id: str,
        quantity: float,
        currency: str,
        unit_price: float,
        gross_total: float,
        maker_fee: float = 0.0,
        taker_fee: float = 0.0,
        buyer_player_id: str | None = None,
        seller_player_id: str | None = None,
        region_id: str | None = None,
        listing_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not trade_source.strip():
            raise StateStoreError("trade_source is required")
        if not asset_type.strip():
            raise StateStoreError("asset_type is required")
        if not asset_id.strip():
            raise StateStoreError("asset_id is required")
        trade_id = f"trade.{uuid.uuid4().hex[:16]}"
        now = self._utc_now()
        metadata_json = json.dumps(
            metadata if isinstance(metadata, dict) else {},
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with self._lock:
            with self._connect() as conn:
                if isinstance(buyer_player_id, str) and buyer_player_id.strip():
                    self._assert_profile_exists(conn, player_id=buyer_player_id.strip())
                if isinstance(seller_player_id, str) and seller_player_id.strip():
                    self._assert_profile_exists(conn, player_id=seller_player_id.strip())
                conn.execute(
                    """
                    INSERT INTO market_trade_log (
                      trade_id, trade_source, buyer_player_id, seller_player_id, asset_type, asset_id,
                      quantity, currency, unit_price, gross_total, maker_fee, taker_fee, region_id,
                      listing_id, metadata_json, created_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade_id,
                        trade_source.strip(),
                        buyer_player_id.strip() if isinstance(buyer_player_id, str) and buyer_player_id.strip() else None,
                        seller_player_id.strip() if isinstance(seller_player_id, str) and seller_player_id.strip() else None,
                        asset_type.strip(),
                        asset_id.strip(),
                        float(quantity),
                        currency.strip(),
                        float(unit_price),
                        float(gross_total),
                        float(maker_fee),
                        float(taker_fee),
                        region_id.strip() if isinstance(region_id, str) and region_id.strip() else None,
                        listing_id.strip() if isinstance(listing_id, str) and listing_id.strip() else None,
                        metadata_json,
                        now,
                    ),
                )
                conn.commit()
        return {
            "trade_id": trade_id,
            "trade_source": trade_source.strip(),
            "asset_type": asset_type.strip(),
            "asset_id": asset_id.strip(),
            "quantity": round(float(quantity), 6),
            "currency": currency.strip(),
            "unit_price": round(float(unit_price), 8 if currency.strip() == "voidcoin" else 4),
            "gross_total": round(float(gross_total), 8 if currency.strip() == "voidcoin" else 4),
            "maker_fee": round(float(maker_fee), 8 if currency.strip() == "voidcoin" else 4),
            "taker_fee": round(float(taker_fee), 8 if currency.strip() == "voidcoin" else 4),
            "region_id": region_id if isinstance(region_id, str) else None,
            "listing_id": listing_id if isinstance(listing_id, str) else None,
            "created_utc": now,
        }

    def list_market_trade_history(
        self,
        limit: int = 80,
        asset_type: str | None = None,
        asset_id: str | None = None,
        currency: str | None = None,
        trade_source: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                clauses: list[str] = []
                args: list[Any] = []
                if isinstance(asset_type, str) and asset_type.strip():
                    clauses.append("asset_type = ?")
                    args.append(asset_type.strip())
                if isinstance(asset_id, str) and asset_id.strip():
                    clauses.append("asset_id = ?")
                    args.append(asset_id.strip())
                if isinstance(currency, str) and currency.strip():
                    clauses.append("currency = ?")
                    args.append(currency.strip())
                if isinstance(trade_source, str) and trade_source.strip():
                    clauses.append("trade_source = ?")
                    args.append(trade_source.strip())
                where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
                rows = conn.execute(
                    """
                    SELECT trade_id, trade_source, buyer_player_id, seller_player_id, asset_type, asset_id,
                           quantity, currency, unit_price, gross_total, maker_fee, taker_fee,
                           region_id, listing_id, metadata_json, created_utc
                    FROM market_trade_log
                    """
                    + where_clause
                    + """
                    ORDER BY created_utc DESC
                    LIMIT ?
                    """,
                    (*args, int(limit)),
                ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            metadata_raw = payload.get("metadata_json")
            metadata: dict[str, Any] = {}
            if isinstance(metadata_raw, str) and metadata_raw.strip():
                try:
                    parsed = json.loads(metadata_raw)
                    if isinstance(parsed, dict):
                        metadata = parsed
                except json.JSONDecodeError:
                    metadata = {}
            payload.pop("metadata_json", None)
            payload["metadata"] = metadata
            out.append(payload)
        return out

    def market_price_summary(
        self,
        asset_type: str,
        asset_id: str,
        currency: str = "credits",
        lookback_limit: int = 120,
    ) -> dict[str, Any]:
        rows = self.list_market_trade_history(
            limit=max(12, min(500, int(lookback_limit))),
            asset_type=asset_type,
            asset_id=asset_id,
            currency=currency,
        )
        if not rows:
            return {
                "asset_type": asset_type,
                "asset_id": asset_id,
                "currency": currency,
                "sample_size": 0,
                "avg_unit_price": None,
                "median_unit_price": None,
                "vwap_unit_price": None,
            }
        unit_prices: list[float] = []
        weighted_numerator = 0.0
        weighted_denominator = 0.0
        for row in rows:
            unit_price = row.get("unit_price")
            quantity = row.get("quantity")
            if (
                isinstance(unit_price, (int, float))
                and not isinstance(unit_price, bool)
                and isinstance(quantity, (int, float))
                and not isinstance(quantity, bool)
                and float(quantity) > 0
            ):
                unit_prices.append(float(unit_price))
                weighted_numerator += float(unit_price) * float(quantity)
                weighted_denominator += float(quantity)
        if not unit_prices:
            return {
                "asset_type": asset_type,
                "asset_id": asset_id,
                "currency": currency,
                "sample_size": 0,
                "avg_unit_price": None,
                "median_unit_price": None,
                "vwap_unit_price": None,
            }
        unit_prices.sort()
        n = len(unit_prices)
        median = (
            unit_prices[n // 2]
            if n % 2 == 1
            else (unit_prices[(n // 2) - 1] + unit_prices[n // 2]) / 2.0
        )
        avg = sum(unit_prices) / float(n)
        vwap = weighted_numerator / max(1e-9, weighted_denominator)
        precision = 8 if currency == "voidcoin" else 4
        return {
            "asset_type": asset_type,
            "asset_id": asset_id,
            "currency": currency,
            "sample_size": n,
            "avg_unit_price": round(avg, precision),
            "median_unit_price": round(median, precision),
            "vwap_unit_price": round(vwap, precision),
        }

    def create_contract_job(
        self,
        player_id: str,
        template_id: str,
        objective_target: float,
        expires_utc: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        contract_job_id = f"contract_job.{uuid.uuid4().hex[:14]}"
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO player_contracts (
                      contract_job_id, player_id, template_id, status, assigned_utc, expires_utc,
                      progress_value, objective_target, payload_json, updated_utc
                    ) VALUES (?, ?, ?, 'active', ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        contract_job_id,
                        player_id.strip(),
                        template_id.strip(),
                        now,
                        expires_utc,
                        float(objective_target),
                        json.dumps(payload, ensure_ascii=True),
                        now,
                    ),
                )
                conn.commit()
        return self.get_contract_job(player_id=player_id, contract_job_id=contract_job_id)

    def _sync_contract_statuses(self, conn: sqlite3.Connection, player_id: str) -> None:
        now = self._utc_now()
        conn.execute(
            """
            UPDATE player_contracts
            SET status = 'expired', updated_utc = ?
            WHERE player_id = ?
              AND status = 'active'
              AND expires_utc <= ?
            """,
            (now, player_id.strip(), now),
        )

    def list_contract_jobs(
        self, player_id: str, status: str | None = None, limit: int = 40
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                self._sync_contract_statuses(conn, player_id=player_id)
                if status is None:
                    rows = conn.execute(
                        """
                        SELECT contract_job_id, template_id, status, assigned_utc, expires_utc,
                               progress_value, objective_target, payload_json, updated_utc
                        FROM player_contracts
                        WHERE player_id = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT contract_job_id, template_id, status, assigned_utc, expires_utc,
                               progress_value, objective_target, payload_json, updated_utc
                        FROM player_contracts
                        WHERE player_id = ? AND status = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), status, limit),
                    ).fetchall()
                conn.commit()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                payload = {}
            out.append(
                {
                    "contract_job_id": row["contract_job_id"],
                    "template_id": row["template_id"],
                    "status": row["status"],
                    "assigned_utc": row["assigned_utc"],
                    "expires_utc": row["expires_utc"],
                    "progress_value": float(row["progress_value"]),
                    "objective_target": float(row["objective_target"]),
                    "progress_ratio": round(
                        min(1.0, max(0.0, float(row["progress_value"]) / max(1e-9, float(row["objective_target"])))),
                        4,
                    ),
                    "payload": payload if isinstance(payload, dict) else {},
                    "updated_utc": row["updated_utc"],
                }
            )
        return out

    def get_contract_job(self, player_id: str, contract_job_id: str) -> dict[str, Any]:
        jobs = self.list_contract_jobs(player_id=player_id, status=None, limit=200)
        for row in jobs:
            if row["contract_job_id"] == contract_job_id:
                return row
        raise StateStoreError(f"Unknown contract_job_id '{contract_job_id}'")

    def set_contract_progress(
        self, player_id: str, contract_job_id: str, progress_value: float, status: str | None = None
    ) -> dict[str, Any]:
        if not player_id.strip() or not contract_job_id.strip():
            raise StateStoreError("player_id and contract_job_id are required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT objective_target
                    FROM player_contracts
                    WHERE player_id = ? AND contract_job_id = ?
                    """,
                    (player_id.strip(), contract_job_id.strip()),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown contract_job_id '{contract_job_id}'")
                objective_target = float(row["objective_target"])
                next_status = status
                if next_status is None:
                    next_status = "completed" if float(progress_value) + 1e-9 >= objective_target else "active"
                conn.execute(
                    """
                    UPDATE player_contracts
                    SET progress_value = ?, status = ?, updated_utc = ?
                    WHERE player_id = ? AND contract_job_id = ?
                    """,
                    (
                        max(0.0, float(progress_value)),
                        next_status,
                        now,
                        player_id.strip(),
                        contract_job_id.strip(),
                    ),
                )
                conn.commit()
        return self.get_contract_job(player_id=player_id, contract_job_id=contract_job_id)

    def create_mission_job(
        self,
        player_id: str,
        mission_id: str,
        objective_target: float,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not mission_id.strip():
            raise StateStoreError("mission_id is required")
        mission_job_id = f"mission_job.{uuid.uuid4().hex[:14]}"
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO player_missions (
                      mission_job_id, player_id, mission_id, status, accepted_utc,
                      progress_value, objective_target, payload_json, updated_utc
                    ) VALUES (?, ?, ?, 'active', ?, 0, ?, ?, ?)
                    """,
                    (
                        mission_job_id,
                        player_id.strip(),
                        mission_id.strip(),
                        now,
                        max(1.0, float(objective_target)),
                        json.dumps(payload, ensure_ascii=True),
                        now,
                    ),
                )
                conn.commit()
        return self.get_mission_job(player_id=player_id, mission_job_id=mission_job_id)

    def list_mission_jobs(
        self, player_id: str, status: str | None = None, limit: int = 40
    ) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                if status is None:
                    rows = conn.execute(
                        """
                        SELECT mission_job_id, mission_id, status, accepted_utc,
                               progress_value, objective_target, payload_json, updated_utc
                        FROM player_missions
                        WHERE player_id = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT mission_job_id, mission_id, status, accepted_utc,
                               progress_value, objective_target, payload_json, updated_utc
                        FROM player_missions
                        WHERE player_id = ? AND status = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (player_id.strip(), status, limit),
                    ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                payload = {}
            objective_target = max(1e-9, float(row["objective_target"]))
            progress_value = max(0.0, float(row["progress_value"]))
            out.append(
                {
                    "mission_job_id": row["mission_job_id"],
                    "mission_id": row["mission_id"],
                    "status": row["status"],
                    "accepted_utc": row["accepted_utc"],
                    "progress_value": progress_value,
                    "objective_target": objective_target,
                    "progress_ratio": round(min(1.0, progress_value / objective_target), 4),
                    "payload": payload if isinstance(payload, dict) else {},
                    "updated_utc": row["updated_utc"],
                }
            )
        return out

    def get_mission_job(self, player_id: str, mission_job_id: str) -> dict[str, Any]:
        rows = self.list_mission_jobs(player_id=player_id, status=None, limit=300)
        for row in rows:
            if row["mission_job_id"] == mission_job_id:
                return row
        raise StateStoreError(f"Unknown mission_job_id '{mission_job_id}'")

    def set_mission_progress(
        self,
        player_id: str,
        mission_job_id: str,
        progress_value: float,
        status: str | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip() or not mission_job_id.strip():
            raise StateStoreError("player_id and mission_job_id are required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT objective_target
                    FROM player_missions
                    WHERE player_id = ? AND mission_job_id = ?
                    """,
                    (player_id.strip(), mission_job_id.strip()),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown mission_job_id '{mission_job_id}'")
                objective_target = float(row["objective_target"])
                next_progress = max(0.0, float(progress_value))
                next_status = status
                if next_status is None:
                    next_status = "completed" if next_progress + 1e-9 >= objective_target else "active"
                conn.execute(
                    """
                    UPDATE player_missions
                    SET progress_value = ?, status = ?, updated_utc = ?
                    WHERE player_id = ? AND mission_job_id = ?
                    """,
                    (
                        next_progress,
                        next_status,
                        now,
                        player_id.strip(),
                        mission_job_id.strip(),
                    ),
                )
                conn.commit()
        return self.get_mission_job(player_id=player_id, mission_job_id=mission_job_id)

    def list_claimed_mission_ids(self, player_id: str) -> list[str]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                rows = conn.execute(
                    """
                    SELECT DISTINCT mission_id
                    FROM player_missions
                    WHERE player_id = ? AND status = 'claimed'
                    """,
                    (player_id.strip(),),
                ).fetchall()
        out: list[str] = []
        for row in rows:
            mission_id = row["mission_id"]
            if isinstance(mission_id, str):
                out.append(mission_id)
        return sorted(out)

    def count_discovered_worlds(self, player_id: str) -> int:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM discovered_bodies WHERE player_id = ?",
                    (player_id.strip(),),
                ).fetchone()
        return int(row["c"]) if row is not None else 0

    def count_world_structures(self, player_id: str) -> int:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM world_structures ws
                    WHERE ws.world_id IN (
                      SELECT world_id FROM claimed_worlds WHERE player_id = ?
                    )
                    """,
                    (player_id.strip(),),
                ).fetchone()
        return int(row["c"]) if row is not None else 0

    def get_battle_metrics(self, player_id: str) -> dict[str, int]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT battles_won, battles_lost, battles_fled
                    FROM player_battle_metrics
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO player_battle_metrics (
                          player_id, battles_won, battles_lost, battles_fled, updated_utc
                        ) VALUES (?, 0, 0, 0, ?)
                        """,
                        (player_id.strip(), now),
                    )
                    conn.commit()
                    return {"battles_won": 0, "battles_lost": 0, "battles_fled": 0}
        return {
            "battles_won": int(row["battles_won"]),
            "battles_lost": int(row["battles_lost"]),
            "battles_fled": int(row["battles_fled"]),
        }

    def increment_battle_metrics(
        self,
        player_id: str,
        *,
        won: int = 0,
        lost: int = 0,
        fled: int = 0,
    ) -> dict[str, int]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = self._utc_now()
        current = self.get_battle_metrics(player_id=player_id)
        next_won = max(0, int(current["battles_won"]) + int(won))
        next_lost = max(0, int(current["battles_lost"]) + int(lost))
        next_fled = max(0, int(current["battles_fled"]) + int(fled))
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                conn.execute(
                    """
                    INSERT INTO player_battle_metrics (
                      player_id, battles_won, battles_lost, battles_fled, updated_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      battles_won = excluded.battles_won,
                      battles_lost = excluded.battles_lost,
                      battles_fled = excluded.battles_fled,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        next_won,
                        next_lost,
                        next_fled,
                        now,
                    ),
                )
                conn.commit()
        return self.get_battle_metrics(player_id=player_id)

    @staticmethod
    def _decode_json_object(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return dict(raw)
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            if isinstance(parsed, dict):
                return parsed
        return {}

    def _insert_legion_event_locked(
        self,
        conn: sqlite3.Connection,
        *,
        legion_id: str,
        event_type: str,
        actor_player_id: str | None,
        payload: dict[str, Any],
        created_utc: str,
    ) -> None:
        event_id = f"levt.{uuid.uuid4().hex[:16]}"
        actor_value = actor_player_id.strip() if isinstance(actor_player_id, str) and actor_player_id.strip() else None
        conn.execute(
            """
            INSERT INTO legion_event_log (
              event_id, legion_id, actor_player_id, event_type, payload_json, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                legion_id.strip(),
                actor_value,
                event_type.strip(),
                json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
                created_utc,
            ),
        )

    @staticmethod
    def _legion_visibility_value(raw: Any) -> str:
        value = str(raw or "invite_only").strip().casefold()
        if value not in LEGION_VISIBILITY_VALUES:
            raise StateStoreError(
                "visibility must be one of: {}".format(", ".join(sorted(LEGION_VISIBILITY_VALUES)))
            )
        return value

    @staticmethod
    def _member_role_value(raw: Any) -> str:
        value = str(raw or "member").strip().casefold()
        if value not in LEGION_MEMBER_ROLE_VALUES:
            raise StateStoreError(
                "role must be one of: {}".format(", ".join(sorted(LEGION_MEMBER_ROLE_VALUES)))
            )
        return value

    def get_player_faction_affiliation(self, player_id: str) -> dict[str, Any] | None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT player_id, faction_id, standing, role, joined_utc, updated_utc
                    FROM player_faction_affiliations
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
        if row is None:
            return None
        return {
            "player_id": row["player_id"],
            "faction_id": row["faction_id"],
            "standing": round(float(row["standing"]), 4),
            "role": row["role"],
            "joined_utc": row["joined_utc"],
            "updated_utc": row["updated_utc"],
        }

    def set_player_faction_affiliation(
        self,
        *,
        player_id: str,
        faction_id: str,
        standing: float = 0.0,
        role: str = "member",
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not faction_id.strip():
            raise StateStoreError("faction_id is required")
        if not math.isfinite(float(standing)):
            raise StateStoreError("standing must be finite")
        role_value = str(role or "member").strip()
        if len(role_value) < 2 or len(role_value) > 40:
            raise StateStoreError("role must be 2-40 characters")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                existing = conn.execute(
                    "SELECT joined_utc FROM player_faction_affiliations WHERE player_id = ?",
                    (player_id.strip(),),
                ).fetchone()
                joined_utc = existing["joined_utc"] if existing is not None else now
                conn.execute(
                    """
                    INSERT INTO player_faction_affiliations (
                      player_id, faction_id, standing, role, joined_utc, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(player_id) DO UPDATE SET
                      faction_id = excluded.faction_id,
                      standing = excluded.standing,
                      role = excluded.role,
                      updated_utc = excluded.updated_utc
                    """,
                    (
                        player_id.strip(),
                        faction_id.strip(),
                        float(standing),
                        role_value,
                        joined_utc,
                        now,
                    ),
                )
                conn.commit()
        affiliation = self.get_player_faction_affiliation(player_id=player_id)
        if not isinstance(affiliation, dict):
            raise StateStoreError("Unable to load faction affiliation after update")
        return affiliation

    def clear_player_faction_affiliation(self, player_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                cursor = conn.execute(
                    "DELETE FROM player_faction_affiliations WHERE player_id = ?",
                    (player_id.strip(),),
                )
                conn.commit()
                cleared = int(cursor.rowcount or 0) > 0
        return {"player_id": player_id.strip(), "cleared": cleared}

    def count_faction_affiliations(self) -> dict[str, int]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT faction_id, COUNT(*) AS c
                    FROM player_faction_affiliations
                    GROUP BY faction_id
                    """
                ).fetchall()
        return {
            str(row["faction_id"]): int(row["c"])
            for row in rows
            if isinstance(row["faction_id"], str)
        }

    def count_legions_by_faction(self) -> dict[str, int]:
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT faction_id, COUNT(*) AS c
                    FROM legions
                    WHERE faction_id IS NOT NULL AND TRIM(faction_id) <> ''
                    GROUP BY faction_id
                    """
                ).fetchall()
        return {
            str(row["faction_id"]): int(row["c"])
            for row in rows
            if isinstance(row["faction_id"], str)
        }

    def _read_legion_locked(self, conn: sqlite3.Connection, legion_id: str) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT legion_id, name, tagline, description, faction_id, visibility,
                   min_combat_rank, owner_player_id, policy_json, treasury_credits,
                   created_utc, updated_utc
            FROM legions
            WHERE legion_id = ?
            """,
            (legion_id.strip(),),
        ).fetchone()
        if row is None:
            raise StateStoreError(f"Unknown legion_id '{legion_id}'")
        return row

    def _legion_payload_from_row_locked(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        viewer_player_id: str | None = None,
    ) -> dict[str, Any]:
        legion_id = str(row["legion_id"])
        active_members = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM legion_members
            WHERE legion_id = ? AND status = 'active'
            """,
            (legion_id,),
        ).fetchone()
        pending_requests = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM legion_join_requests
            WHERE legion_id = ? AND status = 'pending'
            """,
            (legion_id,),
        ).fetchone()
        policy = self._decode_json_object(row["policy_json"])
        payload = {
            "legion_id": legion_id,
            "name": row["name"],
            "tagline": row["tagline"],
            "description": row["description"],
            "faction_id": row["faction_id"],
            "visibility": row["visibility"],
            "min_combat_rank": int(row["min_combat_rank"]),
            "owner_player_id": row["owner_player_id"],
            "treasury_credits": round(float(row["treasury_credits"]), 4),
            "policy": policy,
            "member_counts": {
                "active": int(active_members["c"]) if active_members is not None else 0,
                "pending_requests": int(pending_requests["c"]) if pending_requests is not None else 0,
            },
            "created_utc": row["created_utc"],
            "updated_utc": row["updated_utc"],
        }
        if isinstance(viewer_player_id, str) and viewer_player_id.strip():
            membership_row = conn.execute(
                """
                SELECT role, status, joined_utc, contribution_score, updated_utc
                FROM legion_members
                WHERE legion_id = ? AND player_id = ? AND status = 'active'
                """,
                (legion_id, viewer_player_id.strip()),
            ).fetchone()
            payload["viewer_membership"] = (
                {
                    "player_id": viewer_player_id.strip(),
                    "role": membership_row["role"],
                    "status": membership_row["status"],
                    "joined_utc": membership_row["joined_utc"],
                    "contribution_score": round(float(membership_row["contribution_score"]), 4),
                    "updated_utc": membership_row["updated_utc"],
                }
                if membership_row is not None
                else None
            )
        return payload

    def get_legion(self, legion_id: str, viewer_player_id: str | None = None) -> dict[str, Any]:
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        with self._lock:
            with self._connect() as conn:
                row = self._read_legion_locked(conn, legion_id=legion_id)
                return self._legion_payload_from_row_locked(
                    conn,
                    row=row,
                    viewer_player_id=viewer_player_id,
                )

    def list_legions(
        self,
        *,
        limit: int = 40,
        faction_id: str | None = None,
        visibility: str | None = None,
        search: str | None = None,
        viewer_player_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        if visibility is not None:
            visibility = self._legion_visibility_value(visibility)
        sql = (
            "SELECT legion_id, name, tagline, description, faction_id, visibility, "
            "min_combat_rank, owner_player_id, policy_json, treasury_credits, created_utc, updated_utc "
            "FROM legions"
        )
        where_clauses: list[str] = []
        params: list[Any] = []
        if isinstance(faction_id, str) and faction_id.strip():
            where_clauses.append("faction_id = ?")
            params.append(faction_id.strip())
        if isinstance(visibility, str):
            where_clauses.append("visibility = ?")
            params.append(visibility)
        if isinstance(search, str) and search.strip():
            where_clauses.append("(LOWER(name) LIKE ? OR LOWER(tagline) LIKE ? OR LOWER(description) LIKE ?)")
            like_value = f"%{search.strip().casefold()}%"
            params.extend([like_value, like_value, like_value])
        if where_clauses:
            sql = f"{sql} WHERE {' AND '.join(where_clauses)}"
        sql = f"{sql} ORDER BY updated_utc DESC LIMIT ?"
        params.append(int(limit))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(sql, tuple(params)).fetchall()
                return [
                    self._legion_payload_from_row_locked(
                        conn,
                        row=row,
                        viewer_player_id=viewer_player_id,
                    )
                    for row in rows
                ]

    def get_player_active_legion_membership(self, player_id: str) -> dict[str, Any] | None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT legion_id, role, status, joined_utc, contribution_score, updated_utc
                    FROM legion_members
                    WHERE player_id = ? AND status = 'active'
                    ORDER BY joined_utc DESC
                    LIMIT 1
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if row is None:
                    return None
                legion_row = self._read_legion_locked(conn, legion_id=str(row["legion_id"]))
                legion = self._legion_payload_from_row_locked(
                    conn,
                    row=legion_row,
                    viewer_player_id=player_id.strip(),
                )
                return {
                    "player_id": player_id.strip(),
                    "legion_id": row["legion_id"],
                    "role": row["role"],
                    "status": row["status"],
                    "joined_utc": row["joined_utc"],
                    "contribution_score": round(float(row["contribution_score"]), 4),
                    "updated_utc": row["updated_utc"],
                    "legion": legion,
                }

    def list_legion_members(
        self,
        *,
        legion_id: str,
        status: str | None = "active",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        if status is not None:
            status_value = str(status).strip().casefold()
            if status_value not in LEGION_MEMBER_STATUS_VALUES:
                raise StateStoreError(
                    "status must be one of: {}".format(
                        ", ".join(sorted(LEGION_MEMBER_STATUS_VALUES))
                    )
                )
            status = status_value
        with self._lock:
            with self._connect() as conn:
                _ = self._read_legion_locked(conn, legion_id=legion_id)
                if status is None:
                    rows = conn.execute(
                        """
                        SELECT lm.legion_id, lm.player_id, lm.role, lm.status, lm.joined_utc,
                               lm.contribution_score, lm.updated_utc, p.captain_name,
                               COALESCE(cp.combat_rank, 1) AS combat_rank
                        FROM legion_members lm
                        JOIN profiles p ON p.player_id = lm.player_id
                        LEFT JOIN player_combat_progress cp ON cp.player_id = lm.player_id
                        WHERE lm.legion_id = ?
                        ORDER BY
                          CASE lm.role
                            WHEN 'leader' THEN 0
                            WHEN 'officer' THEN 1
                            ELSE 2
                          END,
                          lm.joined_utc ASC
                        LIMIT ?
                        """,
                        (legion_id.strip(), int(limit)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT lm.legion_id, lm.player_id, lm.role, lm.status, lm.joined_utc,
                               lm.contribution_score, lm.updated_utc, p.captain_name,
                               COALESCE(cp.combat_rank, 1) AS combat_rank
                        FROM legion_members lm
                        JOIN profiles p ON p.player_id = lm.player_id
                        LEFT JOIN player_combat_progress cp ON cp.player_id = lm.player_id
                        WHERE lm.legion_id = ? AND lm.status = ?
                        ORDER BY
                          CASE lm.role
                            WHEN 'leader' THEN 0
                            WHEN 'officer' THEN 1
                            ELSE 2
                          END,
                          lm.joined_utc ASC
                        LIMIT ?
                        """,
                        (legion_id.strip(), status, int(limit)),
                    ).fetchall()
        return [
            {
                "legion_id": row["legion_id"],
                "player_id": row["player_id"],
                "captain_name": row["captain_name"],
                "combat_rank": int(row["combat_rank"]),
                "role": row["role"],
                "status": row["status"],
                "joined_utc": row["joined_utc"],
                "contribution_score": round(float(row["contribution_score"]), 4),
                "updated_utc": row["updated_utc"],
            }
            for row in rows
        ]

    def create_legion(
        self,
        *,
        owner_player_id: str,
        name: str,
        tagline: str,
        description: str,
        faction_id: str | None,
        visibility: str,
        min_combat_rank: int,
        policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not owner_player_id.strip():
            raise StateStoreError("owner_player_id is required")
        name_value = str(name).strip()
        if len(name_value) < 3 or len(name_value) > 56:
            raise StateStoreError("name must be 3-56 characters")
        tagline_value = str(tagline).strip()
        if len(tagline_value) < 4 or len(tagline_value) > 140:
            raise StateStoreError("tagline must be 4-140 characters")
        description_value = str(description).strip()
        if len(description_value) < 12 or len(description_value) > 3000:
            raise StateStoreError("description must be 12-3000 characters")
        visibility_value = self._legion_visibility_value(visibility)
        rank_value = int(min_combat_rank)
        if rank_value < 1 or rank_value > 500:
            raise StateStoreError("min_combat_rank must be between 1 and 500")
        faction_value = (
            faction_id.strip()
            if isinstance(faction_id, str) and faction_id.strip()
            else None
        )
        now = self._utc_now()
        legion_id = f"legion.{uuid.uuid4().hex[:12]}"
        policy_payload = dict(policy) if isinstance(policy, dict) else {}
        if "charter" not in policy_payload:
            policy_payload["charter"] = "Operate under treaty law and active democratic governance."
        if "tax_rate_pct" not in policy_payload:
            policy_payload["tax_rate_pct"] = 5.0
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=owner_player_id)
                active_membership = conn.execute(
                    """
                    SELECT legion_id
                    FROM legion_members
                    WHERE player_id = ? AND status = 'active'
                    LIMIT 1
                    """,
                    (owner_player_id.strip(),),
                ).fetchone()
                if active_membership is not None:
                    raise StateStoreError("Player is already in an active legion")
                try:
                    conn.execute(
                        """
                        INSERT INTO legions (
                          legion_id, name, tagline, description, faction_id, visibility,
                          min_combat_rank, owner_player_id, policy_json, treasury_credits,
                          created_utc, updated_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            legion_id,
                            name_value,
                            tagline_value,
                            description_value,
                            faction_value,
                            visibility_value,
                            rank_value,
                            owner_player_id.strip(),
                            json.dumps(policy_payload, ensure_ascii=True, separators=(",", ":")),
                            0.0,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise StateStoreError(f"Legion name '{name_value}' is already in use") from exc
                conn.execute(
                    """
                    INSERT INTO legion_members (
                      legion_id, player_id, role, status, joined_utc, contribution_score, updated_utc
                    ) VALUES (?, ?, 'leader', 'active', ?, ?, ?)
                    """,
                    (legion_id, owner_player_id.strip(), now, 0.0, now),
                )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id,
                    event_type="legion_created",
                    actor_player_id=owner_player_id.strip(),
                    payload={
                        "name": name_value,
                        "faction_id": faction_value,
                        "visibility": visibility_value,
                    },
                    created_utc=now,
                )
                conn.commit()
        return self.get_legion(legion_id=legion_id, viewer_player_id=owner_player_id.strip())

    def request_or_join_legion(
        self,
        *,
        player_id: str,
        legion_id: str,
        message: str = "",
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        message_value = str(message or "").strip()
        if len(message_value) > 260:
            raise StateStoreError("message must be <= 260 characters")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                legion_row = self._read_legion_locked(conn, legion_id=legion_id)
                active_membership = conn.execute(
                    """
                    SELECT legion_id
                    FROM legion_members
                    WHERE player_id = ? AND status = 'active'
                    LIMIT 1
                    """,
                    (player_id.strip(),),
                ).fetchone()
                if active_membership is not None:
                    current_legion_id = str(active_membership["legion_id"])
                    if current_legion_id == legion_id.strip():
                        membership = conn.execute(
                            """
                            SELECT legion_id, player_id, role, status, joined_utc,
                                   contribution_score, updated_utc
                            FROM legion_members
                            WHERE legion_id = ? AND player_id = ? AND status = 'active'
                            """,
                            (legion_id.strip(), player_id.strip()),
                        ).fetchone()
                        if membership is None:
                            raise StateStoreError("Unable to load current legion membership")
                        return {
                            "mode": "already_member",
                            "membership": {
                                "legion_id": membership["legion_id"],
                                "player_id": membership["player_id"],
                                "role": membership["role"],
                                "status": membership["status"],
                                "joined_utc": membership["joined_utc"],
                                "contribution_score": round(float(membership["contribution_score"]), 4),
                                "updated_utc": membership["updated_utc"],
                            },
                        }
                    raise StateStoreError("Player is already in another active legion")

                faction_id = legion_row["faction_id"]
                if isinstance(faction_id, str) and faction_id.strip():
                    affiliation = conn.execute(
                        """
                        SELECT faction_id
                        FROM player_faction_affiliations
                        WHERE player_id = ?
                        """,
                        (player_id.strip(),),
                    ).fetchone()
                    if affiliation is None or str(affiliation["faction_id"]).strip() != faction_id.strip():
                        raise StateStoreError(
                            "Legion requires faction alignment '{}' before joining".format(
                                faction_id.strip()
                            )
                        )

                rank_row = conn.execute(
                    """
                    SELECT combat_rank
                    FROM player_combat_progress
                    WHERE player_id = ?
                    """,
                    (player_id.strip(),),
                ).fetchone()
                player_rank = int(rank_row["combat_rank"]) if rank_row is not None else 1
                min_rank = int(legion_row["min_combat_rank"])
                if player_rank < min_rank:
                    raise StateStoreError(
                        f"Legion requires combat rank {min_rank} (current {player_rank})"
                    )

                visibility = str(legion_row["visibility"]).strip().casefold()
                if visibility == "closed":
                    raise StateStoreError("Legion is currently closed to new applicants")
                if visibility == "open":
                    existing = conn.execute(
                        """
                        SELECT joined_utc, contribution_score
                        FROM legion_members
                        WHERE legion_id = ? AND player_id = ?
                        """,
                        (legion_id.strip(), player_id.strip()),
                    ).fetchone()
                    joined_utc = existing["joined_utc"] if existing is not None else now
                    contribution = float(existing["contribution_score"]) if existing is not None else 0.0
                    conn.execute(
                        """
                        INSERT INTO legion_members (
                          legion_id, player_id, role, status, joined_utc, contribution_score, updated_utc
                        ) VALUES (?, ?, 'member', 'active', ?, ?, ?)
                        ON CONFLICT(legion_id, player_id) DO UPDATE SET
                          role = 'member',
                          status = 'active',
                          updated_utc = excluded.updated_utc
                        """,
                        (
                            legion_id.strip(),
                            player_id.strip(),
                            joined_utc,
                            contribution,
                            now,
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE legion_join_requests
                        SET status = 'approved', updated_utc = ?
                        WHERE legion_id = ? AND player_id = ? AND status = 'pending'
                        """,
                        (now, legion_id.strip(), player_id.strip()),
                    )
                    self._insert_legion_event_locked(
                        conn,
                        legion_id=legion_id.strip(),
                        event_type="member_joined",
                        actor_player_id=player_id.strip(),
                        payload={"mode": "open_join"},
                        created_utc=now,
                    )
                    conn.execute(
                        "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                        (now, legion_id.strip()),
                    )
                    conn.commit()
                    membership = conn.execute(
                        """
                        SELECT legion_id, player_id, role, status, joined_utc,
                               contribution_score, updated_utc
                        FROM legion_members
                        WHERE legion_id = ? AND player_id = ? AND status = 'active'
                        """,
                        (legion_id.strip(), player_id.strip()),
                    ).fetchone()
                    if membership is None:
                        raise StateStoreError("Unable to load joined legion membership")
                    return {
                        "mode": "joined",
                        "membership": {
                            "legion_id": membership["legion_id"],
                            "player_id": membership["player_id"],
                            "role": membership["role"],
                            "status": membership["status"],
                            "joined_utc": membership["joined_utc"],
                            "contribution_score": round(float(membership["contribution_score"]), 4),
                            "updated_utc": membership["updated_utc"],
                        },
                    }

                existing_pending = conn.execute(
                    """
                    SELECT request_id, legion_id, player_id, status, message, created_utc, updated_utc
                    FROM legion_join_requests
                    WHERE legion_id = ? AND player_id = ? AND status = 'pending'
                    ORDER BY updated_utc DESC
                    LIMIT 1
                    """,
                    (legion_id.strip(), player_id.strip()),
                ).fetchone()
                if existing_pending is not None:
                    return {
                        "mode": "requested",
                        "request": {
                            "request_id": existing_pending["request_id"],
                            "legion_id": existing_pending["legion_id"],
                            "player_id": existing_pending["player_id"],
                            "status": existing_pending["status"],
                            "message": existing_pending["message"],
                            "created_utc": existing_pending["created_utc"],
                            "updated_utc": existing_pending["updated_utc"],
                        },
                    }
                request_id = f"lreq.{uuid.uuid4().hex[:16]}"
                conn.execute(
                    """
                    INSERT INTO legion_join_requests (
                      request_id, legion_id, player_id, status, message, created_utc, updated_utc
                    ) VALUES (?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        request_id,
                        legion_id.strip(),
                        player_id.strip(),
                        message_value,
                        now,
                        now,
                    ),
                )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id.strip(),
                    event_type="join_request_submitted",
                    actor_player_id=player_id.strip(),
                    payload={"request_id": request_id},
                    created_utc=now,
                )
                conn.execute(
                    "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                    (now, legion_id.strip()),
                )
                conn.commit()
                return {
                    "mode": "requested",
                    "request": {
                        "request_id": request_id,
                        "legion_id": legion_id.strip(),
                        "player_id": player_id.strip(),
                        "status": "pending",
                        "message": message_value,
                        "created_utc": now,
                        "updated_utc": now,
                    },
                }

    def list_legion_join_requests(
        self,
        *,
        limit: int = 60,
        legion_id: str | None = None,
        player_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        where_clauses: list[str] = []
        params: list[Any] = []
        if isinstance(legion_id, str) and legion_id.strip():
            where_clauses.append("r.legion_id = ?")
            params.append(legion_id.strip())
        if isinstance(player_id, str) and player_id.strip():
            where_clauses.append("r.player_id = ?")
            params.append(player_id.strip())
        if isinstance(status, str) and status.strip():
            status_value = status.strip().casefold()
            if status_value not in LEGION_REQUEST_STATUS_VALUES:
                raise StateStoreError(
                    "status must be one of: {}".format(
                        ", ".join(sorted(LEGION_REQUEST_STATUS_VALUES))
                    )
                )
            where_clauses.append("r.status = ?")
            params.append(status_value)
        sql = (
            "SELECT r.request_id, r.legion_id, l.name AS legion_name, r.player_id, p.captain_name, "
            "r.status, r.message, r.created_utc, r.updated_utc "
            "FROM legion_join_requests r "
            "JOIN legions l ON l.legion_id = r.legion_id "
            "JOIN profiles p ON p.player_id = r.player_id"
        )
        if where_clauses:
            sql = f"{sql} WHERE {' AND '.join(where_clauses)}"
        sql = f"{sql} ORDER BY r.updated_utc DESC LIMIT ?"
        params.append(int(limit))
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "request_id": row["request_id"],
                "legion_id": row["legion_id"],
                "legion_name": row["legion_name"],
                "player_id": row["player_id"],
                "captain_name": row["captain_name"],
                "status": row["status"],
                "message": row["message"],
                "created_utc": row["created_utc"],
                "updated_utc": row["updated_utc"],
            }
            for row in rows
        ]

    def respond_legion_join_request(
        self,
        *,
        actor_player_id: str,
        request_id: str,
        decision: str,
    ) -> dict[str, Any]:
        if not actor_player_id.strip():
            raise StateStoreError("actor_player_id is required")
        if not request_id.strip():
            raise StateStoreError("request_id is required")
        decision_value = str(decision).strip().casefold()
        if decision_value not in {"approve", "reject"}:
            raise StateStoreError("decision must be 'approve' or 'reject'")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=actor_player_id)
                request_row = conn.execute(
                    """
                    SELECT request_id, legion_id, player_id, status, message, created_utc, updated_utc
                    FROM legion_join_requests
                    WHERE request_id = ?
                    """,
                    (request_id.strip(),),
                ).fetchone()
                if request_row is None:
                    raise StateStoreError(f"Unknown request_id '{request_id}'")
                if str(request_row["status"]) != "pending":
                    raise StateStoreError(
                        f"Join request is not pending (status={request_row['status']})"
                    )
                legion_id = str(request_row["legion_id"])
                actor_membership = conn.execute(
                    """
                    SELECT role
                    FROM legion_members
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (legion_id, actor_player_id.strip()),
                ).fetchone()
                if actor_membership is None:
                    raise StateStoreError("Actor is not an active member of this legion")
                actor_role = str(actor_membership["role"]).casefold()
                if actor_role not in {"leader", "officer"}:
                    raise StateStoreError("Only leader/officer can process join requests")
                target_player_id = str(request_row["player_id"])
                membership_payload: dict[str, Any] | None = None
                next_status = "rejected"
                if decision_value == "approve":
                    existing_active = conn.execute(
                        """
                        SELECT legion_id
                        FROM legion_members
                        WHERE player_id = ? AND status = 'active'
                        LIMIT 1
                        """,
                        (target_player_id,),
                    ).fetchone()
                    if existing_active is not None and str(existing_active["legion_id"]) != legion_id:
                        raise StateStoreError("Target player is already in another active legion")
                    existing_member = conn.execute(
                        """
                        SELECT joined_utc, contribution_score
                        FROM legion_members
                        WHERE legion_id = ? AND player_id = ?
                        """,
                        (legion_id, target_player_id),
                    ).fetchone()
                    joined_utc = existing_member["joined_utc"] if existing_member is not None else now
                    contribution = (
                        float(existing_member["contribution_score"])
                        if existing_member is not None
                        else 0.0
                    )
                    conn.execute(
                        """
                        INSERT INTO legion_members (
                          legion_id, player_id, role, status, joined_utc, contribution_score, updated_utc
                        ) VALUES (?, ?, 'member', 'active', ?, ?, ?)
                        ON CONFLICT(legion_id, player_id) DO UPDATE SET
                          role = 'member',
                          status = 'active',
                          updated_utc = excluded.updated_utc
                        """,
                        (legion_id, target_player_id, joined_utc, contribution, now),
                    )
                    membership_row = conn.execute(
                        """
                        SELECT legion_id, player_id, role, status, joined_utc,
                               contribution_score, updated_utc
                        FROM legion_members
                        WHERE legion_id = ? AND player_id = ? AND status = 'active'
                        """,
                        (legion_id, target_player_id),
                    ).fetchone()
                    membership_payload = (
                        {
                            "legion_id": membership_row["legion_id"],
                            "player_id": membership_row["player_id"],
                            "role": membership_row["role"],
                            "status": membership_row["status"],
                            "joined_utc": membership_row["joined_utc"],
                            "contribution_score": round(float(membership_row["contribution_score"]), 4),
                            "updated_utc": membership_row["updated_utc"],
                        }
                        if membership_row is not None
                        else None
                    )
                    next_status = "approved"
                conn.execute(
                    """
                    UPDATE legion_join_requests
                    SET status = ?, updated_utc = ?
                    WHERE request_id = ?
                    """,
                    (next_status, now, request_id.strip()),
                )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id,
                    event_type=(
                        "join_request_approved" if next_status == "approved" else "join_request_rejected"
                    ),
                    actor_player_id=actor_player_id.strip(),
                    payload={
                        "request_id": request_id.strip(),
                        "target_player_id": target_player_id,
                    },
                    created_utc=now,
                )
                conn.execute(
                    "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                    (now, legion_id),
                )
                conn.commit()
        with self._lock:
            with self._connect() as conn:
                request_row = conn.execute(
                    """
                    SELECT r.request_id, r.legion_id, l.name AS legion_name, r.player_id, p.captain_name,
                           r.status, r.message, r.created_utc, r.updated_utc
                    FROM legion_join_requests r
                    JOIN legions l ON l.legion_id = r.legion_id
                    JOIN profiles p ON p.player_id = r.player_id
                    WHERE r.request_id = ?
                    """,
                    (request_id.strip(),),
                ).fetchone()
        request_payload = (
            {
                "request_id": request_row["request_id"],
                "legion_id": request_row["legion_id"],
                "legion_name": request_row["legion_name"],
                "player_id": request_row["player_id"],
                "captain_name": request_row["captain_name"],
                "status": request_row["status"],
                "message": request_row["message"],
                "created_utc": request_row["created_utc"],
                "updated_utc": request_row["updated_utc"],
            }
            if request_row is not None
            else None
        )
        return {
            "request": request_payload,
            "membership": membership_payload,
            "decision": next_status,
        }

    def set_legion_member_role(
        self,
        *,
        actor_player_id: str,
        legion_id: str,
        target_player_id: str,
        role: str,
    ) -> dict[str, Any]:
        if not actor_player_id.strip():
            raise StateStoreError("actor_player_id is required")
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        if not target_player_id.strip():
            raise StateStoreError("target_player_id is required")
        role_value = self._member_role_value(role)
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                _ = self._read_legion_locked(conn, legion_id=legion_id)
                actor_row = conn.execute(
                    """
                    SELECT role
                    FROM legion_members
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (legion_id.strip(), actor_player_id.strip()),
                ).fetchone()
                if actor_row is None:
                    raise StateStoreError("Actor is not an active member of this legion")
                if str(actor_row["role"]).casefold() != "leader":
                    raise StateStoreError("Only legion leader can change member roles")
                target_row = conn.execute(
                    """
                    SELECT role
                    FROM legion_members
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (legion_id.strip(), target_player_id.strip()),
                ).fetchone()
                if target_row is None:
                    raise StateStoreError("Target player is not an active member of this legion")
                target_role = str(target_row["role"]).casefold()
                if role_value == "leader":
                    conn.execute(
                        """
                        UPDATE legion_members
                        SET role = CASE
                          WHEN player_id = ? THEN 'leader'
                          WHEN role = 'leader' THEN 'officer'
                          ELSE role
                        END,
                        updated_utc = ?
                        WHERE legion_id = ? AND status = 'active'
                        """,
                        (target_player_id.strip(), now, legion_id.strip()),
                    )
                    conn.execute(
                        "UPDATE legions SET owner_player_id = ?, updated_utc = ? WHERE legion_id = ?",
                        (target_player_id.strip(), now, legion_id.strip()),
                    )
                else:
                    if target_role == "leader":
                        leader_count_row = conn.execute(
                            """
                            SELECT COUNT(*) AS c
                            FROM legion_members
                            WHERE legion_id = ? AND status = 'active' AND role = 'leader'
                            """,
                            (legion_id.strip(),),
                        ).fetchone()
                        leader_count = int(leader_count_row["c"]) if leader_count_row is not None else 0
                        if leader_count <= 1:
                            raise StateStoreError(
                                "Cannot demote the only leader; assign another leader first"
                            )
                    conn.execute(
                        """
                        UPDATE legion_members
                        SET role = ?, updated_utc = ?
                        WHERE legion_id = ? AND player_id = ? AND status = 'active'
                        """,
                        (role_value, now, legion_id.strip(), target_player_id.strip()),
                    )
                    conn.execute(
                        "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                        (now, legion_id.strip()),
                    )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id.strip(),
                    event_type="member_role_updated",
                    actor_player_id=actor_player_id.strip(),
                    payload={
                        "target_player_id": target_player_id.strip(),
                        "role": role_value,
                    },
                    created_utc=now,
                )
                conn.commit()
        members = self.list_legion_members(
            legion_id=legion_id.strip(),
            status="active",
            limit=400,
        )
        target = next(
            (row for row in members if row.get("player_id") == target_player_id.strip()),
            None,
        )
        if not isinstance(target, dict):
            raise StateStoreError("Unable to load updated member role")
        return target

    def leave_legion(
        self,
        *,
        player_id: str,
        legion_id: str | None = None,
        successor_player_id: str | None = None,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                if isinstance(legion_id, str) and legion_id.strip():
                    membership = conn.execute(
                        """
                        SELECT legion_id, role
                        FROM legion_members
                        WHERE player_id = ? AND legion_id = ? AND status = 'active'
                        """,
                        (player_id.strip(), legion_id.strip()),
                    ).fetchone()
                else:
                    membership = conn.execute(
                        """
                        SELECT legion_id, role
                        FROM legion_members
                        WHERE player_id = ? AND status = 'active'
                        ORDER BY joined_utc DESC
                        LIMIT 1
                        """,
                        (player_id.strip(),),
                    ).fetchone()
                if membership is None:
                    raise StateStoreError("Player is not in an active legion")
                legion_id_value = str(membership["legion_id"])
                role_value = str(membership["role"]).casefold()
                _ = self._read_legion_locked(conn, legion_id=legion_id_value)
                active_members = conn.execute(
                    """
                    SELECT player_id, role
                    FROM legion_members
                    WHERE legion_id = ? AND status = 'active'
                    ORDER BY joined_utc ASC
                    """,
                    (legion_id_value,),
                ).fetchall()
                if role_value == "leader":
                    if len(active_members) > 1:
                        if not isinstance(successor_player_id, str) or not successor_player_id.strip():
                            raise StateStoreError(
                                "Leader must provide successor_player_id before leaving"
                            )
                        successor = next(
                            (
                                row
                                for row in active_members
                                if str(row["player_id"]).strip() == successor_player_id.strip()
                            ),
                            None,
                        )
                        if successor is None:
                            raise StateStoreError("successor_player_id must be an active legion member")
                        if successor_player_id.strip() == player_id.strip():
                            raise StateStoreError("successor_player_id must be different from player_id")
                        conn.execute(
                            """
                            UPDATE legion_members
                            SET role = 'leader', updated_utc = ?
                            WHERE legion_id = ? AND player_id = ? AND status = 'active'
                            """,
                            (now, legion_id_value, successor_player_id.strip()),
                        )
                        conn.execute(
                            """
                            UPDATE legions
                            SET owner_player_id = ?, updated_utc = ?
                            WHERE legion_id = ?
                            """,
                            (successor_player_id.strip(), now, legion_id_value),
                        )
                conn.execute(
                    """
                    UPDATE legion_members
                    SET status = 'left', updated_utc = ?
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (now, legion_id_value, player_id.strip()),
                )
                remaining_active = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM legion_members
                    WHERE legion_id = ? AND status = 'active'
                    """,
                    (legion_id_value,),
                ).fetchone()
                remaining_count = int(remaining_active["c"]) if remaining_active is not None else 0
                if remaining_count <= 0:
                    legion_row = self._read_legion_locked(conn, legion_id=legion_id_value)
                    policy = self._decode_json_object(legion_row["policy_json"])
                    policy["dissolved"] = True
                    policy["dissolved_utc"] = now
                    conn.execute(
                        """
                        UPDATE legions
                        SET visibility = 'closed', policy_json = ?, updated_utc = ?
                        WHERE legion_id = ?
                        """,
                        (
                            json.dumps(policy, ensure_ascii=True, separators=(",", ":")),
                            now,
                            legion_id_value,
                        ),
                    )
                else:
                    conn.execute(
                        "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                        (now, legion_id_value),
                    )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id_value,
                    event_type="member_left",
                    actor_player_id=player_id.strip(),
                    payload={
                        "successor_player_id": (
                            successor_player_id.strip()
                            if isinstance(successor_player_id, str) and successor_player_id.strip()
                            else None
                        )
                    },
                    created_utc=now,
                )
                conn.commit()
        return {
            "player_id": player_id.strip(),
            "legion_id": legion_id_value,
            "status": "left",
            "left_utc": now,
        }

    def _proposal_vote_tally_locked(
        self, conn: sqlite3.Connection, proposal_id: str
    ) -> dict[str, Any]:
        rows = conn.execute(
            """
            SELECT vote, COUNT(*) AS c
            FROM legion_governance_votes
            WHERE proposal_id = ?
            GROUP BY vote
            """,
            (proposal_id.strip(),),
        ).fetchall()
        counts = {"yes": 0, "no": 0, "abstain": 0}
        for row in rows:
            vote_value = str(row["vote"]).casefold()
            if vote_value in counts:
                counts[vote_value] = int(row["c"])
        total_votes = int(sum(counts.values()))
        return {
            "yes": counts["yes"],
            "no": counts["no"],
            "abstain": counts["abstain"],
            "total": total_votes,
        }

    def _proposal_payload_from_row_locked(
        self, conn: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        tally = self._proposal_vote_tally_locked(conn, proposal_id=str(row["proposal_id"]))
        payload = self._decode_json_object(row["payload_json"])
        resolution = self._decode_json_object(row["resolution_json"])
        return {
            "proposal_id": row["proposal_id"],
            "legion_id": row["legion_id"],
            "proposer_player_id": row["proposer_player_id"],
            "title": row["title"],
            "proposal_type": row["proposal_type"],
            "payload": payload,
            "status": row["status"],
            "required_yes_votes": int(row["required_yes_votes"]),
            "expires_utc": row["expires_utc"],
            "resolution": resolution,
            "votes": tally,
            "created_utc": row["created_utc"],
            "updated_utc": row["updated_utc"],
        }

    def list_legion_proposals(
        self,
        *,
        legion_id: str,
        status: str | None = None,
        limit: int = 80,
    ) -> list[dict[str, Any]]:
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        status_filter = None
        if isinstance(status, str) and status.strip():
            status_filter = str(status).strip().casefold()
            if status_filter not in LEGION_PROPOSAL_STATUS_VALUES:
                raise StateStoreError(
                    "status must be one of: {}".format(
                        ", ".join(sorted(LEGION_PROPOSAL_STATUS_VALUES))
                    )
                )
        with self._lock:
            with self._connect() as conn:
                _ = self._read_legion_locked(conn, legion_id=legion_id)
                if status_filter is None:
                    rows = conn.execute(
                        """
                        SELECT proposal_id, legion_id, proposer_player_id, title, proposal_type,
                               payload_json, status, required_yes_votes, expires_utc,
                               resolution_json, created_utc, updated_utc
                        FROM legion_governance_proposals
                        WHERE legion_id = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (legion_id.strip(), int(limit)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT proposal_id, legion_id, proposer_player_id, title, proposal_type,
                               payload_json, status, required_yes_votes, expires_utc,
                               resolution_json, created_utc, updated_utc
                        FROM legion_governance_proposals
                        WHERE legion_id = ? AND status = ?
                        ORDER BY updated_utc DESC
                        LIMIT ?
                        """,
                        (legion_id.strip(), status_filter, int(limit)),
                    ).fetchall()
                return [self._proposal_payload_from_row_locked(conn, row=row) for row in rows]

    def get_legion_proposal(self, proposal_id: str) -> dict[str, Any]:
        if not proposal_id.strip():
            raise StateStoreError("proposal_id is required")
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT proposal_id, legion_id, proposer_player_id, title, proposal_type,
                           payload_json, status, required_yes_votes, expires_utc,
                           resolution_json, created_utc, updated_utc
                    FROM legion_governance_proposals
                    WHERE proposal_id = ?
                    """,
                    (proposal_id.strip(),),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown proposal_id '{proposal_id}'")
                return self._proposal_payload_from_row_locked(conn, row=row)

    def create_legion_proposal(
        self,
        *,
        player_id: str,
        legion_id: str,
        title: str,
        proposal_type: str,
        payload: dict[str, Any] | None = None,
        expires_hours: float = 48.0,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        title_value = str(title).strip()
        if len(title_value) < 4 or len(title_value) > 180:
            raise StateStoreError("title must be 4-180 characters")
        proposal_type_value = str(proposal_type).strip().casefold()
        if proposal_type_value not in LEGION_PROPOSAL_TYPE_VALUES:
            raise StateStoreError(
                "proposal_type must be one of: {}".format(
                    ", ".join(sorted(LEGION_PROPOSAL_TYPE_VALUES))
                )
            )
        payload_value = dict(payload) if isinstance(payload, dict) else {}
        expires_hours_value = float(expires_hours)
        if not math.isfinite(expires_hours_value):
            raise StateStoreError("expires_hours must be finite")
        expires_hours_value = max(1.0, min(168.0, expires_hours_value))
        now = self._utc_now()
        proposal_id = f"lprop.{uuid.uuid4().hex[:16]}"
        expires_epoch = int(time.time() + expires_hours_value * 3600.0)
        expires_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_epoch))
        with self._lock:
            with self._connect() as conn:
                _ = self._read_legion_locked(conn, legion_id=legion_id)
                member_row = conn.execute(
                    """
                    SELECT role
                    FROM legion_members
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (legion_id.strip(), player_id.strip()),
                ).fetchone()
                if member_row is None:
                    raise StateStoreError("Only active legion members can create proposals")
                active_count_row = conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM legion_members
                    WHERE legion_id = ? AND status = 'active'
                    """,
                    (legion_id.strip(),),
                ).fetchone()
                active_count = max(1, int(active_count_row["c"]) if active_count_row is not None else 1)
                required_raw = payload_value.get("required_yes_votes")
                if isinstance(required_raw, bool) or not isinstance(required_raw, (int, float)):
                    required_yes_votes = max(1, int(math.ceil(active_count * 0.5)))
                else:
                    required_yes_votes = int(required_raw)
                required_yes_votes = max(1, min(active_count, required_yes_votes))
                conn.execute(
                    """
                    INSERT INTO legion_governance_proposals (
                      proposal_id, legion_id, proposer_player_id, title, proposal_type,
                      payload_json, status, required_yes_votes, expires_utc,
                      resolution_json, created_utc, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
                    """,
                    (
                        proposal_id,
                        legion_id.strip(),
                        player_id.strip(),
                        title_value,
                        proposal_type_value,
                        json.dumps(payload_value, ensure_ascii=True, separators=(",", ":")),
                        required_yes_votes,
                        expires_utc,
                        json.dumps({}, ensure_ascii=True),
                        now,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO legion_governance_votes (
                      proposal_id, voter_player_id, vote, weight, cast_utc
                    ) VALUES (?, ?, 'yes', ?, ?)
                    ON CONFLICT(proposal_id, voter_player_id) DO UPDATE SET
                      vote = excluded.vote,
                      weight = excluded.weight,
                      cast_utc = excluded.cast_utc
                    """,
                    (proposal_id, player_id.strip(), 1.0, now),
                )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id.strip(),
                    event_type="proposal_created",
                    actor_player_id=player_id.strip(),
                    payload={"proposal_id": proposal_id, "proposal_type": proposal_type_value},
                    created_utc=now,
                )
                conn.execute(
                    "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                    (now, legion_id.strip()),
                )
                conn.commit()
        return self.get_legion_proposal(proposal_id=proposal_id)

    def cast_legion_vote(
        self,
        *,
        player_id: str,
        proposal_id: str,
        vote: str,
    ) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not proposal_id.strip():
            raise StateStoreError("proposal_id is required")
        vote_value = str(vote).strip().casefold()
        if vote_value not in LEGION_VOTE_VALUES:
            raise StateStoreError(
                "vote must be one of: {}".format(", ".join(sorted(LEGION_VOTE_VALUES)))
            )
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                proposal_row = conn.execute(
                    """
                    SELECT proposal_id, legion_id, status, expires_utc
                    FROM legion_governance_proposals
                    WHERE proposal_id = ?
                    """,
                    (proposal_id.strip(),),
                ).fetchone()
                if proposal_row is None:
                    raise StateStoreError(f"Unknown proposal_id '{proposal_id}'")
                if str(proposal_row["status"]) != "open":
                    raise StateStoreError("Proposal is not open for voting")
                expires_epoch = self._utc_to_epoch(str(proposal_row["expires_utc"]))
                if int(time.time()) >= expires_epoch:
                    conn.execute(
                        """
                        UPDATE legion_governance_proposals
                        SET status = 'expired', updated_utc = ?
                        WHERE proposal_id = ? AND status = 'open'
                        """,
                        (now, proposal_id.strip()),
                    )
                    conn.commit()
                    raise StateStoreError("Proposal voting window has expired")
                legion_id = str(proposal_row["legion_id"])
                member_row = conn.execute(
                    """
                    SELECT 1
                    FROM legion_members
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (legion_id, player_id.strip()),
                ).fetchone()
                if member_row is None:
                    raise StateStoreError("Only active legion members can vote on this proposal")
                conn.execute(
                    """
                    INSERT INTO legion_governance_votes (
                      proposal_id, voter_player_id, vote, weight, cast_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(proposal_id, voter_player_id) DO UPDATE SET
                      vote = excluded.vote,
                      weight = excluded.weight,
                      cast_utc = excluded.cast_utc
                    """,
                    (
                        proposal_id.strip(),
                        player_id.strip(),
                        vote_value,
                        1.0,
                        now,
                    ),
                )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id,
                    event_type="proposal_vote_cast",
                    actor_player_id=player_id.strip(),
                    payload={"proposal_id": proposal_id.strip(), "vote": vote_value},
                    created_utc=now,
                )
                conn.execute(
                    """
                    UPDATE legion_governance_proposals
                    SET updated_utc = ?
                    WHERE proposal_id = ?
                    """,
                    (now, proposal_id.strip()),
                )
                conn.commit()
        return self.get_legion_proposal(proposal_id=proposal_id.strip())

    def _enact_legion_proposal_locked(
        self, conn: sqlite3.Connection, proposal_row: sqlite3.Row, now: str
    ) -> dict[str, Any]:
        legion_id = str(proposal_row["legion_id"])
        proposal_type = str(proposal_row["proposal_type"]).casefold()
        payload = self._decode_json_object(proposal_row["payload_json"])
        legion_row = self._read_legion_locked(conn, legion_id=legion_id)
        policy = self._decode_json_object(legion_row["policy_json"])
        enactment: dict[str, Any] = {"proposal_type": proposal_type}
        if proposal_type == "set_visibility":
            visibility = self._legion_visibility_value(payload.get("visibility"))
            conn.execute(
                """
                UPDATE legions
                SET visibility = ?, updated_utc = ?
                WHERE legion_id = ?
                """,
                (visibility, now, legion_id),
            )
            enactment["visibility"] = visibility
        elif proposal_type == "set_min_combat_rank":
            raw_rank = payload.get("min_combat_rank")
            if isinstance(raw_rank, bool) or not isinstance(raw_rank, (int, float)):
                raise StateStoreError("Proposal payload missing numeric min_combat_rank")
            min_rank = int(raw_rank)
            if min_rank < 1 or min_rank > 500:
                raise StateStoreError("min_combat_rank must be between 1 and 500")
            conn.execute(
                """
                UPDATE legions
                SET min_combat_rank = ?, updated_utc = ?
                WHERE legion_id = ?
                """,
                (min_rank, now, legion_id),
            )
            enactment["min_combat_rank"] = min_rank
        elif proposal_type == "set_faction_alignment":
            faction_raw = payload.get("faction_id")
            faction_id = (
                str(faction_raw).strip()
                if isinstance(faction_raw, str) and str(faction_raw).strip()
                else None
            )
            conn.execute(
                """
                UPDATE legions
                SET faction_id = ?, updated_utc = ?
                WHERE legion_id = ?
                """,
                (faction_id, now, legion_id),
            )
            enactment["faction_id"] = faction_id
        elif proposal_type == "update_charter":
            charter_raw = payload.get("charter")
            if not isinstance(charter_raw, str) or len(charter_raw.strip()) < 12:
                raise StateStoreError("Proposal payload missing charter text (>= 12 chars)")
            charter_value = charter_raw.strip()
            if len(charter_value) > 3000:
                raise StateStoreError("charter must be <= 3000 characters")
            policy["charter"] = charter_value
            conn.execute(
                """
                UPDATE legions
                SET policy_json = ?, updated_utc = ?
                WHERE legion_id = ?
                """,
                (
                    json.dumps(policy, ensure_ascii=True, separators=(",", ":")),
                    now,
                    legion_id,
                ),
            )
            enactment["charter"] = charter_value
        elif proposal_type == "set_tax_rate_pct":
            tax_raw = payload.get("tax_rate_pct")
            if isinstance(tax_raw, bool) or not isinstance(tax_raw, (int, float)):
                raise StateStoreError("Proposal payload missing numeric tax_rate_pct")
            tax_rate = float(tax_raw)
            if not math.isfinite(tax_rate) or tax_rate < 0.0 or tax_rate > 25.0:
                raise StateStoreError("tax_rate_pct must be between 0 and 25")
            policy["tax_rate_pct"] = round(tax_rate, 4)
            conn.execute(
                """
                UPDATE legions
                SET policy_json = ?, updated_utc = ?
                WHERE legion_id = ?
                """,
                (
                    json.dumps(policy, ensure_ascii=True, separators=(",", ":")),
                    now,
                    legion_id,
                ),
            )
            enactment["tax_rate_pct"] = round(tax_rate, 4)
        else:
            raise StateStoreError(f"Unsupported proposal_type '{proposal_type}'")
        return enactment

    def finalize_legion_proposal(
        self,
        *,
        actor_player_id: str,
        proposal_id: str,
    ) -> dict[str, Any]:
        if not actor_player_id.strip():
            raise StateStoreError("actor_player_id is required")
        if not proposal_id.strip():
            raise StateStoreError("proposal_id is required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=actor_player_id)
                proposal_row = conn.execute(
                    """
                    SELECT proposal_id, legion_id, proposer_player_id, title, proposal_type,
                           payload_json, status, required_yes_votes, expires_utc,
                           resolution_json, created_utc, updated_utc
                    FROM legion_governance_proposals
                    WHERE proposal_id = ?
                    """,
                    (proposal_id.strip(),),
                ).fetchone()
                if proposal_row is None:
                    raise StateStoreError(f"Unknown proposal_id '{proposal_id}'")
                current_status = str(proposal_row["status"]).casefold()
                if current_status != "open":
                    return self._proposal_payload_from_row_locked(conn, row=proposal_row)
                legion_id = str(proposal_row["legion_id"])
                actor_role = conn.execute(
                    """
                    SELECT role
                    FROM legion_members
                    WHERE legion_id = ? AND player_id = ? AND status = 'active'
                    """,
                    (legion_id, actor_player_id.strip()),
                ).fetchone()
                if actor_role is None:
                    raise StateStoreError("Actor is not an active member of this legion")
                role_value = str(actor_role["role"]).casefold()
                if role_value not in {"leader", "officer"}:
                    raise StateStoreError("Only leader/officer can finalize proposals")
                vote_tally = self._proposal_vote_tally_locked(conn, proposal_id=proposal_id.strip())
                yes_votes = int(vote_tally["yes"])
                no_votes = int(vote_tally["no"])
                required_yes_votes = max(1, int(proposal_row["required_yes_votes"]))
                expires_epoch = self._utc_to_epoch(str(proposal_row["expires_utc"]))
                now_epoch = int(time.time())
                next_status = "open"
                resolution: dict[str, Any] = {
                    "finalized_by": actor_player_id.strip(),
                    "finalized_utc": now,
                    "required_yes_votes": required_yes_votes,
                    "votes": vote_tally,
                }
                if yes_votes >= required_yes_votes and yes_votes > no_votes:
                    enactment = self._enact_legion_proposal_locked(conn, proposal_row, now=now)
                    next_status = "enacted"
                    resolution["decision"] = "enacted"
                    resolution["enactment"] = enactment
                elif now_epoch >= expires_epoch:
                    next_status = "expired"
                    resolution["decision"] = "expired"
                elif no_votes >= required_yes_votes:
                    next_status = "rejected"
                    resolution["decision"] = "rejected"
                else:
                    raise StateStoreError(
                        "Proposal has not met enact/reject thresholds and is not expired"
                    )
                conn.execute(
                    """
                    UPDATE legion_governance_proposals
                    SET status = ?, resolution_json = ?, updated_utc = ?
                    WHERE proposal_id = ?
                    """,
                    (
                        next_status,
                        json.dumps(resolution, ensure_ascii=True, separators=(",", ":")),
                        now,
                        proposal_id.strip(),
                    ),
                )
                self._insert_legion_event_locked(
                    conn,
                    legion_id=legion_id,
                    event_type="proposal_finalized",
                    actor_player_id=actor_player_id.strip(),
                    payload={
                        "proposal_id": proposal_id.strip(),
                        "status": next_status,
                        "votes": vote_tally,
                    },
                    created_utc=now,
                )
                conn.execute(
                    "UPDATE legions SET updated_utc = ? WHERE legion_id = ?",
                    (now, legion_id),
                )
                conn.commit()
                refreshed = conn.execute(
                    """
                    SELECT proposal_id, legion_id, proposer_player_id, title, proposal_type,
                           payload_json, status, required_yes_votes, expires_utc,
                           resolution_json, created_utc, updated_utc
                    FROM legion_governance_proposals
                    WHERE proposal_id = ?
                    """,
                    (proposal_id.strip(),),
                ).fetchone()
                if refreshed is None:
                    raise StateStoreError("Unable to reload finalized proposal")
                return self._proposal_payload_from_row_locked(conn, row=refreshed)

    def list_legion_events(self, *, legion_id: str, limit: int = 120) -> list[dict[str, Any]]:
        if not legion_id.strip():
            raise StateStoreError("legion_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                _ = self._read_legion_locked(conn, legion_id=legion_id)
                rows = conn.execute(
                    """
                    SELECT e.event_id, e.legion_id, e.actor_player_id, p.captain_name,
                           e.event_type, e.payload_json, e.created_utc
                    FROM legion_event_log e
                    LEFT JOIN profiles p ON p.player_id = e.actor_player_id
                    WHERE e.legion_id = ?
                    ORDER BY e.created_utc DESC
                    LIMIT ?
                    """,
                    (legion_id.strip(), int(limit)),
                ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "legion_id": row["legion_id"],
                "actor_player_id": row["actor_player_id"],
                "actor_captain_name": row["captain_name"],
                "event_type": row["event_type"],
                "payload": self._decode_json_object(row["payload_json"]),
                "created_utc": row["created_utc"],
            }
            for row in rows
        ]

    def claim_world(self, player_id: str, world: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(world, dict):
            raise StateStoreError("world must be an object")
        world_id = world.get("world_id")
        body_class = world.get("body_class")
        world_name = world.get("name")

        if not isinstance(player_id, str) or not player_id.strip():
            raise StateStoreError("player_id is required")
        if not isinstance(world_id, str) or not world_id:
            raise StateStoreError("world.world_id is required")
        if not isinstance(body_class, str) or not body_class:
            raise StateStoreError("world.body_class is required")
        if not isinstance(world_name, str) or not world_name:
            raise StateStoreError("world.name is required")

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload_json = json.dumps(world, ensure_ascii=True)

        with self._lock:
            with self._connect() as conn:
                profile_exists = conn.execute(
                    "SELECT 1 FROM profiles WHERE player_id = ?",
                    (player_id,),
                ).fetchone()
                if profile_exists is None:
                    raise StateStoreError(
                        f"Profile not found for player_id='{player_id}'. Save profile first."
                    )

                existing = conn.execute(
                    "SELECT player_id FROM claimed_worlds WHERE world_id = ?",
                    (world_id,),
                ).fetchone()
                if existing is not None and existing["player_id"] != player_id:
                    raise StateStoreError(
                        f"world_id '{world_id}' is already claimed by another player"
                    )

                conn.execute(
                    """
                    INSERT INTO claimed_worlds (
                      world_id, player_id, body_class, world_name, payload_json, discovered_utc, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(world_id) DO UPDATE SET
                      player_id=excluded.player_id,
                      body_class=excluded.body_class,
                      world_name=excluded.world_name,
                      payload_json=excluded.payload_json,
                      updated_utc=excluded.updated_utc
                    """,
                    (world_id, player_id, body_class, world_name, payload_json, now, now),
                )
                conn.commit()

        return self.get_world(world_id=world_id, player_id=player_id)

    def list_worlds_for_player(self, player_id: str) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT world_id, payload_json
                    FROM claimed_worlds
                    WHERE player_id = ?
                    ORDER BY updated_utc DESC
                    """,
                    (player_id.strip(),),
                ).fetchall()
                structure_rows = conn.execute(
                    """
                    SELECT world_id, structure_id
                    FROM world_structures
                    WHERE world_id IN (
                      SELECT world_id FROM claimed_worlds WHERE player_id = ?
                    )
                    """,
                    (player_id.strip(),),
                ).fetchall()

        structures_by_world: dict[str, list[str]] = {}
        for row in structure_rows:
            world_id = row["world_id"]
            structures_by_world.setdefault(world_id, []).append(row["structure_id"])

        items: list[dict[str, Any]] = []
        for row in rows:
            world = json.loads(row["payload_json"])
            world["built_structures"] = sorted(structures_by_world.get(row["world_id"], []))
            items.append(world)
        return items

    def add_world_structure(self, player_id: str, world_id: str, structure_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not world_id.strip():
            raise StateStoreError("world_id is required")
        if not structure_id.strip():
            raise StateStoreError("structure_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        with self._lock:
            with self._connect() as conn:
                world_row = conn.execute(
                    "SELECT player_id FROM claimed_worlds WHERE world_id = ?",
                    (world_id,),
                ).fetchone()
                if world_row is None:
                    raise StateStoreError(f"Unknown world_id '{world_id}'")
                if world_row["player_id"] != player_id:
                    raise StateStoreError("Cannot modify world owned by another player")

                conn.execute(
                    """
                    INSERT OR IGNORE INTO world_structures (world_id, structure_id, built_utc)
                    VALUES (?, ?, ?)
                    """,
                    (world_id, structure_id, now),
                )
                conn.execute(
                    """
                    UPDATE claimed_worlds
                    SET updated_utc = ?
                    WHERE world_id = ?
                    """,
                    (now, world_id),
                )
                conn.commit()

        return self.get_world(world_id=world_id, player_id=player_id)

    def get_world(self, world_id: str, player_id: str) -> dict[str, Any]:
        if not world_id.strip():
            raise StateStoreError("world_id is required")
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        with self._lock:
            with self._connect() as conn:
                world_row = conn.execute(
                    """
                    SELECT payload_json, player_id
                    FROM claimed_worlds
                    WHERE world_id = ?
                    """,
                    (world_id.strip(),),
                ).fetchone()
                if world_row is None:
                    raise StateStoreError(f"Unknown world_id '{world_id}'")
                if world_row["player_id"] != player_id:
                    raise StateStoreError("World access denied for player_id")

                structures = conn.execute(
                    """
                    SELECT structure_id
                    FROM world_structures
                    WHERE world_id = ?
                    ORDER BY structure_id
                    """,
                    (world_id.strip(),),
                ).fetchall()

        world = json.loads(world_row["payload_json"])
        world["built_structures"] = [row["structure_id"] for row in structures]
        return world

    def transfer_world_ownership(
        self,
        world_id: str,
        from_player_id: str,
        to_player_id: str,
    ) -> dict[str, Any]:
        if not world_id.strip():
            raise StateStoreError("world_id is required")
        if not from_player_id.strip() or not to_player_id.strip():
            raise StateStoreError("from_player_id and to_player_id are required")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=from_player_id.strip())
                self._assert_profile_exists(conn, player_id=to_player_id.strip())
                row = conn.execute(
                    """
                    SELECT payload_json, player_id
                    FROM claimed_worlds
                    WHERE world_id = ?
                    """,
                    (world_id.strip(),),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown world_id '{world_id}'")
                owner_player_id = row["player_id"]
                if owner_player_id != from_player_id.strip():
                    raise StateStoreError(
                        f"world_id '{world_id}' is not owned by player '{from_player_id}'"
                    )
                conn.execute(
                    """
                    UPDATE claimed_worlds
                    SET player_id = ?, updated_utc = ?
                    WHERE world_id = ?
                    """,
                    (to_player_id.strip(), now, world_id.strip()),
                )
                conn.execute(
                    """
                    INSERT INTO discovered_bodies (
                      world_id, player_id, payload_json, discovered_utc, updated_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(world_id) DO UPDATE SET
                      player_id = excluded.player_id,
                      payload_json = excluded.payload_json,
                      updated_utc = excluded.updated_utc
                    """,
                    (world_id.strip(), to_player_id.strip(), row["payload_json"], now, now),
                )
                conn.commit()
        return self.get_world(world_id=world_id.strip(), player_id=to_player_id.strip())

    def update_world_payload(self, player_id: str, world: dict[str, Any]) -> dict[str, Any]:
        world_id = world.get("world_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise StateStoreError("player_id is required")
        if not isinstance(world_id, str) or not world_id.strip():
            raise StateStoreError("world.world_id is required")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload_json = json.dumps(world, ensure_ascii=True)

        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT player_id
                    FROM claimed_worlds
                    WHERE world_id = ?
                    """,
                    (world_id.strip(),),
                ).fetchone()
                if row is None:
                    raise StateStoreError(f"Unknown world_id '{world_id}'")
                if row["player_id"] != player_id.strip():
                    raise StateStoreError("Cannot modify world owned by another player")
                conn.execute(
                    """
                    UPDATE claimed_worlds
                    SET payload_json = ?, updated_utc = ?
                    WHERE world_id = ? AND player_id = ?
                    """,
                    (payload_json, now, world_id.strip(), player_id.strip()),
                )
                conn.commit()
        return self.get_world(world_id=world_id.strip(), player_id=player_id.strip())

    def catalog_discovered_worlds(self, player_id: str, worlds: list[dict[str, Any]]) -> None:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not isinstance(worlds, list):
            raise StateStoreError("worlds must be an array")
        now = self._utc_now()
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                for row in worlds:
                    if not isinstance(row, dict):
                        continue
                    world_id = row.get("world_id")
                    if not isinstance(world_id, str) or not world_id.strip():
                        continue
                    payload_json = json.dumps(row, ensure_ascii=True, separators=(",", ":"))
                    conn.execute(
                        """
                        INSERT INTO discovered_bodies (
                          world_id, player_id, payload_json, discovered_utc, updated_utc
                        ) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(world_id) DO UPDATE SET
                          player_id = excluded.player_id,
                          payload_json = excluded.payload_json,
                          updated_utc = excluded.updated_utc
                        """,
                        (
                            world_id.strip(),
                            player_id.strip(),
                            payload_json,
                            now,
                            now,
                        ),
                    )
                conn.commit()

    def list_discovered_worlds(self, player_id: str, limit: int = 120) -> list[dict[str, Any]]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if limit <= 0:
            raise StateStoreError("limit must be > 0")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                rows = conn.execute(
                    """
                    SELECT payload_json
                    FROM discovered_bodies
                    WHERE player_id = ?
                    ORDER BY updated_utc DESC
                    LIMIT ?
                    """,
                    (player_id.strip(), limit),
                ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                items.append(payload)
        return items

    def get_discovered_world(self, player_id: str, world_id: str) -> dict[str, Any]:
        if not player_id.strip():
            raise StateStoreError("player_id is required")
        if not world_id.strip():
            raise StateStoreError("world_id is required")
        with self._lock:
            with self._connect() as conn:
                self._assert_profile_exists(conn, player_id=player_id)
                row = conn.execute(
                    """
                    SELECT payload_json
                    FROM discovered_bodies
                    WHERE player_id = ? AND world_id = ?
                    """,
                    (player_id.strip(), world_id.strip()),
                ).fetchone()
        if row is None:
            raise StateStoreError(
                f"world_id '{world_id}' has not been discovered by player '{player_id}'"
            )
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError as exc:
            raise StateStoreError(f"Stored discovered world payload is invalid for '{world_id}'") from exc
        if not isinstance(payload, dict):
            raise StateStoreError(f"Stored discovered world payload is invalid for '{world_id}'")
        return payload

    def counts(self) -> dict[str, int]:
        with self._lock:
            with self._connect() as conn:
                profile_count = conn.execute("SELECT COUNT(*) AS c FROM profiles").fetchone()["c"]
                world_count = conn.execute("SELECT COUNT(*) AS c FROM claimed_worlds").fetchone()["c"]
                discovered_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM discovered_bodies"
                ).fetchone()["c"]
                structure_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM world_structures"
                ).fetchone()["c"]
                wallet_count = conn.execute("SELECT COUNT(*) AS c FROM wallets").fetchone()["c"]
                inventory_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM element_inventory"
                ).fetchone()["c"]
                life_support_state_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM player_life_support_state"
                ).fetchone()["c"]
                asset_count = conn.execute("SELECT COUNT(*) AS c FROM player_assets").fetchone()["c"]
                tech_unlock_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM research_unlocks"
                ).fetchone()["c"]
                research_job_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM research_jobs"
                ).fetchone()["c"]
                crafted_instance_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM crafted_instances"
                ).fetchone()["c"]
                fleet_count = conn.execute("SELECT COUNT(*) AS c FROM fleet_state").fetchone()["c"]
                mfg_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM manufacturing_jobs"
                ).fetchone()["c"]
                reverse_count = conn.execute("SELECT COUNT(*) AS c FROM reverse_jobs").fetchone()["c"]
                listing_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM market_listings"
                ).fetchone()["c"]
                contract_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM player_contracts"
                ).fetchone()["c"]
                mission_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM player_missions"
                ).fetchone()["c"]
                battle_metrics_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM player_battle_metrics"
                ).fetchone()["c"]
                faction_affiliation_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM player_faction_affiliations"
                ).fetchone()["c"]
                legion_count = conn.execute("SELECT COUNT(*) AS c FROM legions").fetchone()["c"]
                legion_member_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM legion_members"
                ).fetchone()["c"]
                legion_request_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM legion_join_requests"
                ).fetchone()["c"]
                legion_proposal_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM legion_governance_proposals"
                ).fetchone()["c"]
                legion_vote_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM legion_governance_votes"
                ).fetchone()["c"]
                legion_event_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM legion_event_log"
                ).fetchone()["c"]
                covert_cooldown_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM covert_op_cooldowns"
                ).fetchone()["c"]
                covert_log_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM covert_op_log"
                ).fetchone()["c"]
        return {
            "profiles": int(profile_count),
            "claimed_worlds": int(world_count),
            "discovered_bodies": int(discovered_count),
            "built_structures": int(structure_count),
            "wallets": int(wallet_count),
            "inventory_rows": int(inventory_count),
            "life_support_state": int(life_support_state_count),
            "assets": int(asset_count),
            "crafted_instances": int(crafted_instance_count),
            "tech_unlocks": int(tech_unlock_count),
            "research_jobs": int(research_job_count),
            "fleet_state": int(fleet_count),
            "manufacturing_jobs": int(mfg_count),
            "reverse_jobs": int(reverse_count),
            "market_listings": int(listing_count),
            "player_contracts": int(contract_count),
            "player_missions": int(mission_count),
            "player_battle_metrics": int(battle_metrics_count),
            "player_faction_affiliations": int(faction_affiliation_count),
            "legions": int(legion_count),
            "legion_members": int(legion_member_count),
            "legion_join_requests": int(legion_request_count),
            "legion_governance_proposals": int(legion_proposal_count),
            "legion_governance_votes": int(legion_vote_count),
            "legion_event_log": int(legion_event_count),
            "covert_op_cooldowns": int(covert_cooldown_count),
            "covert_op_log": int(covert_log_count),
        }


class MockServerHandler(BaseHTTPRequestHandler):
    """HTTP handler for SpaceShift mock endpoints."""

    seed_store: SeedStore
    state_store: PersistentState
    server_version = "SpaceShiftMock/1.0"
    protocol_version = "HTTP/1.1"
    auth_required = env_flag("SPACESHIFT_AUTH_REQUIRED", default=True)
    auth_mode = env_casefold_choice(
        "SPACESHIFT_AUTH_MODE",
        default=DEFAULT_AUTH_MODE,
        allowed_values=AUTH_MODE_VALUES,
    )
    admin_login_enabled = env_flag("SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN", default=False)
    admin_god_mode_enabled = env_flag("SPACESHIFT_ENABLE_ADMIN_GOD_MODE", default=False)
    session_ttl_seconds = env_nonnegative_int("SPACESHIFT_SESSION_TTL_SECONDS", default=86400)
    jwt_issuer = str(os.getenv("SPACESHIFT_JWT_ISSUER", "")).strip()
    jwt_audience = str(os.getenv("SPACESHIFT_JWT_AUDIENCE", "")).strip()
    jwt_algorithms = env_upper_csv(
        "SPACESHIFT_JWT_ALGORITHMS", default=DEFAULT_JWT_ALGORITHMS
    )
    jwt_jwks_url = str(os.getenv("SPACESHIFT_JWT_JWKS_URL", "")).strip()
    jwt_hs256_secret = os.getenv("SPACESHIFT_JWT_HS256_SECRET")
    admin_username = os.getenv("SPACESHIFT_ADMIN_USERNAME", "admin")
    admin_password = os.getenv("SPACESHIFT_ADMIN_PASSWORD", "admin")
    player_login_enabled = env_flag("SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN", default=False)
    player_username = os.getenv("SPACESHIFT_PLAYER_USERNAME", "player")
    player_password = os.getenv("SPACESHIFT_PLAYER_PASSWORD", "player")
    market_escrow_player_id = os.getenv("SPACESHIFT_MARKET_ESCROW_PLAYER_ID", "player.market_escrow")
    allowed_origins = {
        item.strip()
        for item in os.getenv(
            "SPACESHIFT_ALLOWED_ORIGINS",
            "http://127.0.0.1:8081,http://localhost:8081,http://127.0.0.1:19006,http://localhost:19006",
        ).split(",")
        if item.strip()
    }
    allow_all_origins = "*" in allowed_origins
    _auth_lock = Lock()
    _session_by_token: dict[str, dict[str, Any]] = {}
    _token_by_player: dict[str, str] = {}
    _jwks_lock = Lock()
    _jwks_cache: dict[str, Any] | None = None
    _jwks_cache_epoch = 0
    _encounter_lock = Lock()
    _encounters: dict[str, dict[str, Any]] = {}
    _bootstrap_lock = Lock()
    _bootstrapped_players: set[str] = set()

    @classmethod
    def validate_auth_configuration(cls) -> None:
        if cls.auth_mode == "local":
            return
        if cls.auth_mode != "jwt":
            raise ValueError(
                f"Unsupported SPACESHIFT_AUTH_MODE '{cls.auth_mode}'. "
                "Allowed values: local, jwt."
            )
        if not cls.jwt_issuer:
            raise ValueError(
                "SPACESHIFT_JWT_ISSUER is required when SPACESHIFT_AUTH_MODE=jwt."
            )
        if not cls.jwt_audience:
            raise ValueError(
                "SPACESHIFT_JWT_AUDIENCE is required when SPACESHIFT_AUTH_MODE=jwt."
            )
        normalized_algorithms = tuple(
            dict.fromkeys(alg.strip().upper() for alg in cls.jwt_algorithms if alg.strip())
        )
        if not normalized_algorithms:
            raise ValueError(
                "SPACESHIFT_JWT_ALGORITHMS must include at least one algorithm when "
                "SPACESHIFT_AUTH_MODE=jwt."
            )
        unsupported = sorted(set(normalized_algorithms) - JWT_SUPPORTED_ALGORITHMS)
        if unsupported:
            raise ValueError(
                "SPACESHIFT_JWT_ALGORITHMS includes unsupported values: "
                + ", ".join(unsupported)
                + ". Supported values: HS256, RS256."
            )
        cls.jwt_algorithms = normalized_algorithms
        if "HS256" in cls.jwt_algorithms:
            if not isinstance(cls.jwt_hs256_secret, str) or not cls.jwt_hs256_secret:
                raise ValueError(
                    "SPACESHIFT_JWT_HS256_SECRET is required when HS256 is enabled "
                    "in SPACESHIFT_JWT_ALGORITHMS."
                )
        if "RS256" in cls.jwt_algorithms and not cls.jwt_jwks_url:
            raise ValueError(
                "SPACESHIFT_JWT_JWKS_URL is required when RS256 is enabled in "
                "SPACESHIFT_JWT_ALGORITHMS."
            )

    def _extract_bearer_token(self) -> str | None:
        auth_header = str(self.headers.get("Authorization", "")).strip()
        if auth_header and auth_header.casefold().startswith("bearer "):
            token = auth_header[7:].strip()
            if token:
                return token
        raw_header = str(self.headers.get("X-Auth-Token", "")).strip()
        if raw_header:
            return raw_header
        return None

    @classmethod
    def _issue_session_token(cls, player_id: str, role: str) -> dict[str, Any]:
        now_epoch = int(time.time())
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_epoch))
        if cls.auth_mode == "jwt":
            # JWT mode expects externally-issued bearer tokens and does not mint
            # local session tokens.
            return {
                "player_id": player_id,
                "token_type": "external-jwt",
                "role": role,
                "issued_utc": now,
            }
        token = secrets.token_urlsafe(32)
        expires_epoch: int | None = None
        expires_utc: str | None = None
        if cls.session_ttl_seconds > 0:
            expires_epoch = now_epoch + int(cls.session_ttl_seconds)
            expires_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_epoch))
        session_payload = {
            "player_id": player_id,
            "role": role,
            "issued_utc": now,
            "issued_epoch": now_epoch,
        }
        auth_payload: dict[str, Any] = {
            "player_id": player_id,
            "token_type": "bearer",
            "access_token": token,
            "role": role,
            "issued_utc": now,
        }
        if expires_epoch is not None and isinstance(expires_utc, str):
            session_payload["expires_epoch"] = expires_epoch
            session_payload["expires_utc"] = expires_utc
            auth_payload["expires_utc"] = expires_utc
        with cls._auth_lock:
            previous = cls._token_by_player.get(player_id)
            if isinstance(previous, str):
                cls._session_by_token.pop(previous, None)
            cls._token_by_player[player_id] = token
            cls._session_by_token[token] = session_payload
        return auth_payload

    @classmethod
    def _lookup_session(cls, token: str) -> dict[str, Any] | None:
        with cls._auth_lock:
            payload = cls._session_by_token.get(token)
            if not isinstance(payload, dict):
                return None
            expires_epoch = payload.get("expires_epoch")
            session_expired = False
            if expires_epoch is not None:
                if isinstance(expires_epoch, bool) or not isinstance(expires_epoch, (int, float)):
                    session_expired = True
                else:
                    session_expired = int(expires_epoch) <= int(time.time())
            if session_expired:
                cls._session_by_token.pop(token, None)
                player_id = payload.get("player_id")
                if isinstance(player_id, str):
                    normalized_player_id = player_id.strip()
                    if cls._token_by_player.get(normalized_player_id) == token:
                        cls._token_by_player.pop(normalized_player_id, None)
                return None
            return dict(payload)

    @classmethod
    def _revoke_player_sessions(cls, player_id: str) -> int:
        if not isinstance(player_id, str) or not player_id.strip():
            return 0
        removed = 0
        with cls._auth_lock:
            active_token = cls._token_by_player.pop(player_id.strip(), None)
            if isinstance(active_token, str):
                if cls._session_by_token.pop(active_token, None) is not None:
                    removed += 1
            stale_tokens = [
                token
                for token, session in cls._session_by_token.items()
                if isinstance(session, dict)
                and isinstance(session.get("player_id"), str)
                and str(session["player_id"]).strip() == player_id.strip()
            ]
            for token in stale_tokens:
                if cls._session_by_token.pop(token, None) is not None:
                    removed += 1
        return removed

    @classmethod
    def _jwt_identity_player_key(cls, *, issuer: str, subject: str) -> str:
        digest = hashlib.sha256(f"{issuer}|{subject}".encode("utf-8")).hexdigest()
        return f"player.idp.{digest[:24]}"

    @classmethod
    def _jwt_parse_parts(
        cls, token: str
    ) -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
        if not isinstance(token, str) or not token.strip():
            raise AuthError("Missing Authorization bearer token")
        parts = token.strip().split(".")
        if len(parts) != 3:
            raise AuthError("Authorization token is malformed")
        try:
            signing_input = f"{parts[0]}.{parts[1]}".encode("ascii", errors="strict")
        except UnicodeEncodeError as exc:
            raise AuthError("Authorization token is malformed") from exc
        header_raw = base64url_decode(parts[0], label="header")
        payload_raw = base64url_decode(parts[1], label="payload")
        signature = base64url_decode(parts[2], label="signature")
        try:
            header = json.loads(header_raw.decode("utf-8"))
            payload = json.loads(payload_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthError("Authorization token contains invalid JSON") from exc
        if not isinstance(header, dict) or not isinstance(payload, dict):
            raise AuthError("Authorization token header/payload must be objects")
        return header, payload, signature, signing_input

    @classmethod
    def _jwt_claim_int(cls, claims: dict[str, Any], name: str) -> int:
        raw = claims.get(name)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise AuthError(f"Authorization token is missing numeric '{name}' claim")
        value = float(raw)
        if not math.isfinite(value):
            raise AuthError(f"Authorization token claim '{name}' must be finite")
        return int(value)

    @classmethod
    def _jwt_role_from_claims(cls, claims: dict[str, Any]) -> str:
        role = claims.get("role")
        if isinstance(role, str) and role.strip().casefold() == "admin":
            return "admin"
        roles = claims.get("roles")
        if isinstance(roles, list):
            for item in roles:
                if isinstance(item, str) and item.strip().casefold() == "admin":
                    return "admin"
        return "player"

    @classmethod
    def _jwt_validate_claims(cls, claims: dict[str, Any]) -> tuple[str, str]:
        now_epoch = int(time.time())
        exp = cls._jwt_claim_int(claims, "exp")
        nbf = cls._jwt_claim_int(claims, "nbf")
        if exp <= now_epoch:
            raise AuthError("Authorization token expired")
        if nbf > now_epoch:
            raise AuthError("Authorization token not active yet")

        issuer = claims.get("iss")
        if not isinstance(issuer, str) or not issuer.strip():
            raise AuthError("Authorization token is missing 'iss' claim")
        issuer_value = issuer.strip()
        if issuer_value != cls.jwt_issuer:
            raise AuthError("Authorization token issuer is invalid")

        aud_claim = claims.get("aud")
        aud_ok = False
        if isinstance(aud_claim, str):
            aud_ok = aud_claim.strip() == cls.jwt_audience
        elif isinstance(aud_claim, list):
            aud_ok = any(
                isinstance(item, str) and item.strip() == cls.jwt_audience
                for item in aud_claim
            )
        if not aud_ok:
            raise AuthError("Authorization token audience is invalid")

        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject.strip():
            raise AuthError("Authorization token is missing 'sub' claim")
        return issuer_value, subject.strip()

    @classmethod
    def _jwt_verify_hs256(cls, signing_input: bytes, signature: bytes) -> None:
        secret = cls.jwt_hs256_secret
        if not isinstance(secret, str) or not secret:
            raise AuthError("Authorization token verification is not configured for HS256")
        expected = hmac.new(
            secret.encode("utf-8"),
            signing_input,
            digestmod=hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(expected, signature):
            raise AuthError("Authorization token signature is invalid")

    @classmethod
    def _fetch_jwks(cls) -> dict[str, Any]:
        now_epoch = int(time.time())
        with cls._jwks_lock:
            cached = cls._jwks_cache
            cache_age = now_epoch - int(cls._jwks_cache_epoch)
            if isinstance(cached, dict) and cache_age < DEFAULT_JWKS_CACHE_SECONDS:
                return cached
        if not cls.jwt_jwks_url:
            raise AuthError("Authorization token verification is missing JWKS URL")
        request = Request(
            cls.jwt_jwks_url,
            headers={"Accept": "application/json"},
            method="GET",
        )
        try:
            with urlopen(request, timeout=3.0) as response:
                raw = response.read()
        except Exception as exc:
            raise AuthError("Unable to fetch JWKS for Authorization token validation") from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthError("JWKS response is not valid JSON") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("keys"), list):
            raise AuthError("JWKS response does not include a valid 'keys' array")
        with cls._jwks_lock:
            cls._jwks_cache = payload
            cls._jwks_cache_epoch = now_epoch
        return payload

    @classmethod
    def _select_rsa_jwk(cls, kid: str | None) -> dict[str, Any]:
        payload = cls._fetch_jwks()
        keys = payload.get("keys")
        if not isinstance(keys, list):
            raise AuthError("JWKS payload does not include keys")
        candidates: list[dict[str, Any]] = []
        for row in keys:
            if not isinstance(row, dict):
                continue
            if str(row.get("kty", "")).upper() != "RSA":
                continue
            key_use_raw = row.get("use")
            key_use = (
                key_use_raw.strip().casefold()
                if isinstance(key_use_raw, str)
                else ""
            )
            if key_use not in {"sig", ""}:
                continue
            key_alg_raw = row.get("alg")
            key_alg = (
                key_alg_raw.strip().upper()
                if isinstance(key_alg_raw, str)
                else ""
            )
            if key_alg not in {"RS256", ""}:
                continue
            candidates.append(row)
        if not candidates:
            raise AuthError("JWKS does not include an RSA signing key")
        if isinstance(kid, str) and kid.strip():
            for row in candidates:
                if str(row.get("kid", "")).strip() == kid.strip():
                    return row
            raise AuthError("Authorization token key id was not found in JWKS")
        if len(candidates) == 1:
            return candidates[0]
        raise AuthError("Authorization token key id is required when JWKS has multiple keys")

    @classmethod
    def _verify_rs256_signature(
        cls,
        signing_input: bytes,
        signature: bytes,
        modulus: int,
        exponent: int,
    ) -> bool:
        if modulus <= 0 or exponent <= 0:
            return False
        key_size = (modulus.bit_length() + 7) // 8
        if key_size <= 0 or len(signature) != key_size:
            return False
        signature_int = int.from_bytes(signature, byteorder="big", signed=False)
        if signature_int <= 0 or signature_int >= modulus:
            return False
        encoded = pow(signature_int, exponent, modulus).to_bytes(
            key_size, byteorder="big", signed=False
        )
        digest_info = JWT_RS256_SHA256_DIGESTINFO_PREFIX + hashlib.sha256(
            signing_input
        ).digest()
        min_length = len(digest_info) + 11
        if len(encoded) < min_length:
            return False
        if not encoded.startswith(b"\x00\x01"):
            return False
        separator_index = encoded.find(b"\x00", 2)
        if separator_index < 10:
            return False
        padding = encoded[2:separator_index]
        if not padding or any(byte != 0xFF for byte in padding):
            return False
        return hmac.compare_digest(encoded[separator_index + 1 :], digest_info)

    @classmethod
    def _jwt_verify_rs256(
        cls,
        header: dict[str, Any],
        signing_input: bytes,
        signature: bytes,
    ) -> None:
        kid = header.get("kid")
        kid_value = str(kid).strip() if isinstance(kid, str) else None
        jwk = cls._select_rsa_jwk(kid=kid_value)
        modulus_raw = jwk.get("n")
        exponent_raw = jwk.get("e")
        if not isinstance(modulus_raw, str) or not isinstance(exponent_raw, str):
            raise AuthError("JWKS RSA key is missing modulus/exponent")
        modulus_bytes = base64url_decode(modulus_raw, label="jwk.n")
        exponent_bytes = base64url_decode(exponent_raw, label="jwk.e")
        modulus = int.from_bytes(modulus_bytes, byteorder="big", signed=False)
        exponent = int.from_bytes(exponent_bytes, byteorder="big", signed=False)
        if not cls._verify_rs256_signature(
            signing_input=signing_input,
            signature=signature,
            modulus=modulus,
            exponent=exponent,
        ):
            raise AuthError("Authorization token signature is invalid")

    @classmethod
    def _verify_jwt_bearer_token(cls, token: str) -> dict[str, Any]:
        header, claims, signature, signing_input = cls._jwt_parse_parts(token)
        alg_raw = header.get("alg")
        if not isinstance(alg_raw, str) or not alg_raw.strip():
            raise AuthError("Authorization token header is missing 'alg'")
        algorithm = alg_raw.strip().upper()
        if algorithm not in cls.jwt_algorithms:
            raise AuthError("Authorization token algorithm is not allowed")
        if algorithm == "HS256":
            cls._jwt_verify_hs256(signing_input=signing_input, signature=signature)
        elif algorithm == "RS256":
            cls._jwt_verify_rs256(
                header=header,
                signing_input=signing_input,
                signature=signature,
            )
        else:
            raise AuthError("Authorization token algorithm is unsupported")

        issuer, subject = cls._jwt_validate_claims(claims)
        player_id = cls._jwt_identity_player_key(issuer=issuer, subject=subject)
        return {
            "player_id": player_id,
            "role": cls._jwt_role_from_claims(claims),
            "token_type": "jwt",
            "issuer": issuer,
            "subject": subject,
            "audience": cls.jwt_audience,
            "auth_mode": "jwt",
            "issued_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def _require_authenticated_player(self, expected_player_id: str | None = None) -> str:
        if not self.auth_required:
            if isinstance(expected_player_id, str) and expected_player_id.strip():
                return expected_player_id.strip()
            raise AuthError("player_id is required")
        token = self._extract_bearer_token()
        if not isinstance(token, str) or not token.strip():
            raise AuthError("Missing Authorization bearer token")
        if self.auth_mode == "jwt":
            session = self._verify_jwt_bearer_token(token.strip())
        else:
            session = self._lookup_session(token.strip())
            if not isinstance(session, dict):
                raise AuthError("Invalid or expired Authorization token")
        player_id = session.get("player_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise AuthError("Invalid Authorization token payload")
        if isinstance(expected_player_id, str) and expected_player_id.strip():
            if player_id.strip() != expected_player_id.strip():
                raise AuthError("player_id does not match authenticated identity")
        self._request_session = session
        return player_id.strip()

    def _has_admin_privileges(self, player_id: str) -> bool:
        if not isinstance(player_id, str) or not player_id.strip():
            return False
        if not self.auth_required:
            return player_id.strip() == "admin"
        session = getattr(self, "_request_session", None)
        if not isinstance(session, dict):
            return False
        return (
            session.get("role") == "admin"
            and isinstance(session.get("player_id"), str)
            and str(session["player_id"]).strip() == player_id.strip()
        )

    def _issue_encounter(self, player_id: str, contact: dict[str, Any]) -> str:
        encounter_id = f"enc.{uuid.uuid4().hex[:16]}"
        expires_epoch = int(time.time()) + 600
        with self._encounter_lock:
            self._encounters[encounter_id] = {
                "player_id": player_id.strip(),
                "contact": dict(contact),
                "expires_epoch": expires_epoch,
            }
        return encounter_id

    def _ensure_market_escrow_profile(self) -> None:
        escrow_id = self.market_escrow_player_id.strip()
        if not escrow_id:
            raise ValueError("Market escrow profile id is invalid")
        if self.state_store.profile_exists(escrow_id):
            return
        starter_catalog = self._starter_ship_catalog()
        starter_id = (
            str(starter_catalog[0]["id"])
            if starter_catalog and isinstance(starter_catalog[0], dict) and isinstance(starter_catalog[0].get("id"), str)
            else None
        )
        self.state_store.upsert_profile(
            {
                "player_id": escrow_id,
                "captain_name": "Market Escrow",
                "display_name": "Market Escrow",
                "auth_mode": "guest",
                "email": "",
                "starting_ship_id": starter_id,
                "tutorial_mode": "skip",
                "player_memory": {"system_profile": True, "escrow_profile": True},
            }
        )
        self.state_store.bootstrap_player(
            player_id=escrow_id,
            starter_inventory={},
            starter_tech_ids=[],
        )

    def _resolve_encounter(
        self,
        player_id: str,
        encounter_id: str,
    ) -> dict[str, Any]:
        now_epoch = int(time.time())
        with self._encounter_lock:
            row = self._encounters.get(encounter_id.strip())
            if not isinstance(row, dict):
                raise ValueError("Unknown encounter_id")
            if row.get("player_id") != player_id.strip():
                raise ValueError("encounter_id does not belong to this player")
            expires_epoch = int(row.get("expires_epoch", 0))
            if expires_epoch <= now_epoch:
                self._encounters.pop(encounter_id.strip(), None)
                raise ValueError("encounter_id has expired")
            contact = row.get("contact")
            if not isinstance(contact, dict):
                raise ValueError("encounter contact payload is invalid")
            return dict(contact)

    def do_OPTIONS(self) -> None:  # noqa: N802 (http method naming)
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802 (http method naming)
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query, keep_blank_values=True)

            if path == "/health":
                self._reject_unknown_query_keys(query, allowed=set())
                state_counts = self.state_store.counts()
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "status": "ok",
                        "service": "spaceshift-mock-server",
                        "counts": {
                            "missions": len(self.seed_store.missions),
                            "modules": len(self.seed_store.modules),
                            "ship_hulls": len(self.seed_store.ship_hulls),
                            "starter_ships": len(self.seed_store.starter_ships),
                            "tech_nodes": len(self.seed_store.tech_tree),
                            "research_tracks": len(self.seed_store.research_tracks),
                            "market_regions": len(self.seed_store.market_regions),
                            "contract_templates": len(self.seed_store.contract_templates),
                            "ai_opponents": len(self.seed_store.ai_opponents),
                            "consumables": len(self.seed_store.consumables),
                            "reverse_engineering_recipes": len(self.seed_store.reverse_engineering_recipes),
                            "manufacturing_profiles": len(self.seed_store.manufacturing_profiles),
                            "races": len(self.seed_store.races),
                            "factions": len(self.seed_store.factions),
                            "professions": len(self.seed_store.professions),
                            "abilities": len(self.seed_store.abilities),
                            "artifacts": len(self.seed_store.artifacts),
                            "blueprints": len(self.seed_store.blueprints),
                            "events": len(self.seed_store.events),
                            "planet_types": len(self.seed_store.planet_types),
                            "elements": len(self.seed_store.elements),
                            "celestial_templates": len(self.seed_store.celestial_templates),
                            "structures": len(self.seed_store.structures),
                            "lore_entries": len(self.seed_store.lore_codex),
                            "materials": len(self.seed_store.materials),
                            "crafting_substitutions": len(self.seed_store.crafting_substitutions),
                            "profiles": state_counts["profiles"],
                            "claimed_worlds": state_counts["claimed_worlds"],
                            "discovered_bodies": state_counts["discovered_bodies"],
                            "built_structures": state_counts["built_structures"],
                            "wallets": state_counts["wallets"],
                            "inventory_rows": state_counts["inventory_rows"],
                            "life_support_state": state_counts["life_support_state"],
                            "assets": state_counts["assets"],
                            "crafted_instances": state_counts["crafted_instances"],
                            "tech_unlocks": state_counts["tech_unlocks"],
                            "research_jobs": state_counts["research_jobs"],
                            "fleet_state": state_counts["fleet_state"],
                            "manufacturing_jobs": state_counts["manufacturing_jobs"],
                            "reverse_jobs": state_counts["reverse_jobs"],
                            "market_listings": state_counts["market_listings"],
                            "player_contracts": state_counts["player_contracts"],
                            "player_missions": state_counts["player_missions"],
                            "player_battle_metrics": state_counts["player_battle_metrics"],
                            "player_faction_affiliations": state_counts["player_faction_affiliations"],
                            "legions": state_counts["legions"],
                            "legion_members": state_counts["legion_members"],
                            "legion_join_requests": state_counts["legion_join_requests"],
                            "legion_governance_proposals": state_counts["legion_governance_proposals"],
                            "legion_governance_votes": state_counts["legion_governance_votes"],
                            "legion_event_log": state_counts["legion_event_log"],
                        },
                    },
                )
                return

            if path == "/api/missions":
                self._reject_unknown_query_keys(query, allowed={"limit", "player_id", "status"})
                limit = self._parse_limit(query)
                player_id = self._parse_optional_string(query, "player_id")
                status = self._parse_optional_string(query, "status")
                if status is not None and status not in {"active", "completed", "claimed", "available"}:
                    raise ValueError("status must be one of: active, completed, claimed, available")
                if not isinstance(player_id, str) or not player_id.strip():
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "total": len(self.seed_store.missions),
                            "limit": limit,
                            "items": self.seed_store.missions[:limit],
                        },
                    )
                    return
                self._ensure_player_bootstrap(player_id.strip())
                jobs = self.state_store.list_mission_jobs(
                    player_id=player_id.strip(),
                    status=None,
                    limit=3000,
                )
                latest_by_mission: dict[str, dict[str, Any]] = {}
                for row in jobs:
                    if not isinstance(row, dict):
                        continue
                    mission_id = row.get("mission_id")
                    if not isinstance(mission_id, str):
                        continue
                    existing = latest_by_mission.get(mission_id)
                    if not isinstance(existing, dict):
                        latest_by_mission[mission_id] = row
                        continue
                    if str(row.get("updated_utc", "")) > str(existing.get("updated_utc", "")):
                        latest_by_mission[mission_id] = row
                catalog_items: list[dict[str, Any]] = []
                for mission in self.seed_store.missions:
                    if not isinstance(mission, dict):
                        continue
                    mission_id = mission.get("id")
                    if not isinstance(mission_id, str):
                        continue
                    runtime = latest_by_mission.get(mission_id)
                    runtime_status = str(runtime.get("status")) if isinstance(runtime, dict) else "available"
                    if status is not None and runtime_status != status:
                        continue
                    objective = self._mission_objective(mission=mission)
                    requirement_state = self._mission_requirements(
                        player_id=player_id.strip(),
                        mission=mission,
                    )
                    row = dict(mission)
                    row["runtime_status"] = runtime_status
                    row["runtime_job"] = runtime
                    row["objective"] = objective
                    row["eligibility"] = requirement_state
                    catalog_items.append(row)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(catalog_items),
                        "limit": limit,
                        "player_id": player_id.strip(),
                        "status": status,
                        "items": catalog_items[:limit],
                    },
                )
                return

            if path == "/api/modules":
                self._reject_unknown_query_keys(query, allowed={"family"})
                family = self._parse_optional_string(query, "family")
                modules = self.seed_store.modules
                if family is not None:
                    family_key = family.casefold()
                    modules = [
                        module
                        for module in modules
                        if isinstance(module, dict)
                        and isinstance(module.get("family"), str)
                        and module["family"].casefold() == family_key
                    ]
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(modules),
                        "family": family,
                        "available_families": self.seed_store.module_families(),
                        "items": modules,
                    },
                )
                return

            if path == "/api/tech-tree":
                self._reject_unknown_query_keys(query, allowed={"branch", "tier", "limit"})
                branch = self._parse_optional_string(query, "branch")
                tier = self._parse_optional_int(query, "tier")
                limit = self._parse_limit(query)
                nodes = self.seed_store.tech_tree

                if branch is not None:
                    branch_key = branch.casefold()
                    nodes = [
                        node
                        for node in nodes
                        if isinstance(node, dict)
                        and isinstance(node.get("branch"), str)
                        and node["branch"].casefold() == branch_key
                    ]

                if tier is not None:
                    nodes = [
                        node
                        for node in nodes
                        if isinstance(node, dict)
                        and isinstance(node.get("tier"), int)
                        and node["tier"] == tier
                    ]

                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(nodes),
                        "branch": branch,
                        "tier": tier,
                        "limit": limit,
                        "items": nodes[:limit],
                    },
                )
                return

            if path == "/api/races":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.races})
                return

            if path == "/api/factions":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_optional_string(query, "player_id")
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                faction_counts = self.state_store.count_faction_affiliations()
                legion_counts = self.state_store.count_legions_by_faction()
                player_affiliation = (
                    self.state_store.get_player_faction_affiliation(player_id=player_id.strip())
                    if isinstance(player_id, str) and player_id.strip()
                    else None
                )
                aligned_faction_id = (
                    player_affiliation.get("faction_id")
                    if isinstance(player_affiliation, dict)
                    and isinstance(player_affiliation.get("faction_id"), str)
                    else None
                )
                items: list[dict[str, Any]] = []
                for row in self.seed_store.factions:
                    if not isinstance(row, dict):
                        continue
                    faction_id = row.get("id")
                    if not isinstance(faction_id, str):
                        continue
                    payload = dict(row)
                    payload["member_count"] = int(faction_counts.get(faction_id, 0))
                    payload["legion_count"] = int(legion_counts.get(faction_id, 0))
                    payload["is_aligned"] = bool(
                        isinstance(aligned_faction_id, str) and aligned_faction_id == faction_id
                    )
                    items.append(payload)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "player_id": player_id,
                        "player_affiliation": player_affiliation,
                        "items": items,
                    },
                )
                return

            if path == "/api/factions/status":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                affiliation = self.state_store.get_player_faction_affiliation(player_id=player_id)
                faction = None
                if isinstance(affiliation, dict):
                    faction_id = affiliation.get("faction_id")
                    if isinstance(faction_id, str):
                        faction = self.seed_store.faction_index().get(faction_id)
                legion_membership = self.state_store.get_player_active_legion_membership(
                    player_id=player_id
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "faction_affiliation": affiliation,
                        "faction": faction,
                        "legion_membership": legion_membership,
                    },
                )
                return

            if path == "/api/professions":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.professions})
                return

            if path == "/api/legions":
                self._reject_unknown_query_keys(
                    query,
                    allowed={"limit", "faction_id", "visibility", "search", "player_id"},
                )
                limit = self._parse_market_limit(query)
                faction_id = self._parse_optional_string(query, "faction_id")
                visibility = self._parse_optional_string(query, "visibility")
                search = self._parse_optional_string(query, "search")
                player_id = self._parse_optional_string(query, "player_id")
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                items = self.state_store.list_legions(
                    limit=limit,
                    faction_id=faction_id,
                    visibility=visibility,
                    search=search,
                    viewer_player_id=player_id,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "faction_id": faction_id,
                        "visibility": visibility,
                        "search": search,
                        "player_id": player_id,
                        "items": items,
                    },
                )
                return

            if path == "/api/legions/detail":
                self._reject_unknown_query_keys(query, allowed={"legion_id", "player_id"})
                legion_id = self._parse_required_query_string(query, "legion_id")
                player_id = self._parse_optional_string(query, "player_id")
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                legion = self.state_store.get_legion(
                    legion_id=legion_id,
                    viewer_player_id=player_id,
                )
                members = self.state_store.list_legion_members(
                    legion_id=legion_id,
                    status="active",
                    limit=120,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "legion": legion,
                        "members": members,
                    },
                )
                return

            if path == "/api/legions/members":
                self._reject_unknown_query_keys(query, allowed={"legion_id", "status", "limit"})
                legion_id = self._parse_required_query_string(query, "legion_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                items = self.state_store.list_legion_members(
                    legion_id=legion_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "legion_id": legion_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/legions/requests":
                self._reject_unknown_query_keys(
                    query, allowed={"player_id", "legion_id", "status", "limit"}
                )
                player_id = self._parse_required_query_string(query, "player_id")
                legion_id = self._parse_optional_string(query, "legion_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                filter_player_id: str | None = player_id
                if isinstance(legion_id, str) and legion_id.strip():
                    membership = self.state_store.get_player_active_legion_membership(
                        player_id=player_id
                    )
                    if (
                        isinstance(membership, dict)
                        and membership.get("legion_id") == legion_id.strip()
                        and str(membership.get("role", "")).casefold() in {"leader", "officer"}
                    ):
                        filter_player_id = None
                items = self.state_store.list_legion_join_requests(
                    limit=limit,
                    legion_id=legion_id,
                    player_id=filter_player_id,
                    status=status,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "legion_id": legion_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/legions/governance":
                self._reject_unknown_query_keys(
                    query, allowed={"legion_id", "status", "limit", "player_id"}
                )
                legion_id = self._parse_required_query_string(query, "legion_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                player_id = self._parse_optional_string(query, "player_id")
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                items = self.state_store.list_legion_proposals(
                    legion_id=legion_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "legion_id": legion_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/legions/events":
                self._reject_unknown_query_keys(query, allowed={"legion_id", "limit"})
                legion_id = self._parse_required_query_string(query, "legion_id")
                limit = self._parse_market_limit(query)
                items = self.state_store.list_legion_events(
                    legion_id=legion_id,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "legion_id": legion_id,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/legions/me":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                faction_affiliation = self.state_store.get_player_faction_affiliation(
                    player_id=player_id
                )
                membership = self.state_store.get_player_active_legion_membership(
                    player_id=player_id
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "faction_affiliation": faction_affiliation,
                        "legion_membership": membership,
                    },
                )
                return

            if path == "/api/abilities":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.abilities})
                return

            if path == "/api/artifacts":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.artifacts})
                return

            if path == "/api/blueprints":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.blueprints})
                return

            if path == "/api/events":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.events})
                return

            if path == "/api/planet-types":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, {"items": self.seed_store.planet_types})
                return

            if path == "/api/starter-ships":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_optional_string(query, "player_id")
                selected_starting_ship_id = None
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                    profile = self.state_store.get_profile(player_id=player_id.strip())
                    profile_ship_id = profile.get("starting_ship_id")
                    if isinstance(profile_ship_id, str) and profile_ship_id.strip():
                        selected_starting_ship_id = profile_ship_id.strip()
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(self._starter_ship_catalog()),
                        "selected_starting_ship_id": selected_starting_ship_id,
                        "items": self._starter_ship_catalog(),
                    },
                )
                return

            if path == "/api/elements":
                self._reject_unknown_query_keys(
                    query, allowed={"limit", "symbol", "group_block", "standard_state"}
                )
                limit = self._parse_limit(query)
                symbol = self._parse_optional_string(query, "symbol")
                group_block = self._parse_optional_string(query, "group_block")
                standard_state = self._parse_optional_string(query, "standard_state")
                items = self.seed_store.elements

                if symbol is not None:
                    symbol_key = symbol.casefold()
                    items = [
                        element
                        for element in items
                        if isinstance(element, dict)
                        and isinstance(element.get("symbol"), str)
                        and element["symbol"].casefold() == symbol_key
                    ]
                if group_block is not None:
                    group_key = group_block.casefold()
                    items = [
                        element
                        for element in items
                        if isinstance(element, dict)
                        and isinstance(element.get("group_block"), str)
                        and element["group_block"].casefold() == group_key
                    ]
                if standard_state is not None:
                    state_key = standard_state.casefold()
                    items = [
                        element
                        for element in items
                        if isinstance(element, dict)
                        and isinstance(element.get("standard_state"), str)
                        and element["standard_state"].casefold() == state_key
                    ]

                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "items": items[:limit],
                    },
                )
                return

            if path == "/api/materials":
                self._reject_unknown_query_keys(
                    query, allowed={"limit", "category", "real_world_basis"}
                )
                limit = self._parse_limit(query)
                category = self._parse_optional_string(query, "category")
                real_world_basis_raw = self._parse_optional_string(query, "real_world_basis")
                real_world_basis: bool | None = None
                if real_world_basis_raw is not None:
                    parsed = real_world_basis_raw.strip().casefold()
                    if parsed in {"true", "1", "yes"}:
                        real_world_basis = True
                    elif parsed in {"false", "0", "no"}:
                        real_world_basis = False
                    else:
                        raise ValueError(
                            "Query parameter 'real_world_basis' must be true/false"
                        )

                items = self.seed_store.materials
                if category is not None:
                    category_key = category.casefold()
                    items = [
                        item
                        for item in items
                        if isinstance(item, dict)
                        and isinstance(item.get("category"), str)
                        and item["category"].casefold() == category_key
                    ]
                if real_world_basis is not None:
                    items = [
                        item
                        for item in items
                        if isinstance(item, dict)
                        and isinstance(item.get("real_world_basis"), bool)
                        and item["real_world_basis"] is real_world_basis
                    ]

                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "category": category,
                        "real_world_basis": real_world_basis,
                        "items": items[:limit],
                    },
                )
                return

            if path == "/api/crafting/substitutions":
                self._reject_unknown_query_keys(
                    query, allowed={"item_id", "limit", "search"}
                )
                item_id = self._parse_optional_string(query, "item_id")
                search = self._parse_optional_string(query, "search")
                limit = self._parse_limit(query)
                rows = self.seed_store.crafting_substitutions
                if item_id is not None:
                    rows = self.seed_store.substitutions_for_item(item_id=item_id)
                if search is not None:
                    rows = [
                        row
                        for row in rows
                        if self._substitution_matches_search(
                            substitution=row, query=search
                        )
                    ]
                items = [self._summarize_substitution(row) for row in rows]
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "item_id": item_id,
                        "search": search,
                        "items": items[:limit],
                    },
                )
                return

            if path == "/api/celestial-templates":
                self._reject_unknown_query_keys(query, allowed={"body_class"})
                body_class = self._parse_optional_string(query, "body_class")
                templates = self.seed_store.celestial_templates

                if body_class is not None:
                    body_class_key = body_class.casefold()
                    templates = [
                        item
                        for item in templates
                        if isinstance(item, dict)
                        and isinstance(item.get("body_class"), str)
                        and item["body_class"].casefold() == body_class_key
                    ]

                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(templates),
                        "body_class": body_class,
                        "items": templates,
                    },
                )
                return

            if path == "/api/structures":
                self._reject_unknown_query_keys(query, allowed={"domain", "category"})
                domain = self._parse_optional_string(query, "domain")
                category = self._parse_optional_string(query, "category")
                structures = self.seed_store.structures

                if domain is not None:
                    domain_key = domain.casefold()
                    structures = [
                        item
                        for item in structures
                        if isinstance(item, dict)
                        and isinstance(item.get("domain"), str)
                        and item["domain"].casefold() in {domain_key, "any"}
                    ]

                if category is not None:
                    category_key = category.casefold()
                    structures = [
                        item
                        for item in structures
                        if isinstance(item, dict)
                        and isinstance(item.get("category"), str)
                        and item["category"].casefold() == category_key
                    ]

                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(structures),
                        "domain": domain,
                        "category": category,
                        "items": structures,
                    },
                )
                return

            if path == "/api/lore":
                self._reject_unknown_query_keys(query, allowed={"limit", "arc"})
                limit = self._parse_limit(query)
                arc = self._parse_optional_string(query, "arc")
                entries = self.seed_store.lore_codex
                if arc is not None:
                    arc_key = arc.casefold()
                    entries = [
                        entry
                        for entry in entries
                        if isinstance(entry, dict)
                        and isinstance(entry.get("arc"), str)
                        and entry["arc"].casefold() == arc_key
                    ]
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(entries),
                        "limit": limit,
                        "arc": arc,
                        "items": entries[:limit],
                    },
                )
                return

            if path == "/api/profile":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                profile = self.state_store.get_profile(player_id=player_id)
                profile["action_energy"] = self._get_player_action_energy(player_id=player_id)
                profile["life_support"] = self._life_support_status(player_id=player_id, force_tick=False)
                self._send_json(HTTPStatus.OK, profile)
                return

            if path == "/api/profile/memory":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                profile = self.state_store.get_profile(player_id=player_id)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "player_memory": profile.get("player_memory", {}),
                        "updated_utc": profile.get("updated_utc"),
                    },
                )
                return

            if path == "/api/energy":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                energy = self._get_player_action_energy(player_id=player_id)
                self._send_json(HTTPStatus.OK, {"player_id": player_id, "energy": energy})
                return

            if path == "/api/profile/pvp-visibility":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                settings = self.state_store.get_pvp_visibility_setting(player_id=player_id)
                self._send_json(
                    HTTPStatus.OK,
                    {"player_id": player_id, "pvp_visibility": settings},
                )
                return

            if path == "/api/combat/progress":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                progress = self.state_store.get_combat_progress(player_id=player_id)
                self._send_json(
                    HTTPStatus.OK,
                    {"player_id": player_id, "progress": progress},
                )
                return

            if path == "/api/economy/wallet":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                wallet = self.state_store.get_wallet(player_id=player_id)
                life_support = self._life_support_status(player_id=player_id, force_tick=False)
                payload = dict(wallet)
                payload["life_support"] = {
                    "inventory": life_support.get("inventory", {}),
                    "demand_per_hour": life_support.get("demand_per_hour", {}),
                    "production_per_hour": life_support.get("production_per_hour", {}),
                    "shortage_stress": (
                        life_support.get("state", {}).get("shortage_stress", 0.0)
                        if isinstance(life_support.get("state"), dict)
                        else 0.0
                    ),
                }
                self._send_json(HTTPStatus.OK, payload)
                return

            if path == "/api/economy/inventory":
                self._reject_unknown_query_keys(query, allowed={"player_id", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_inventory(player_id=player_id, limit=limit)
                life_support = self._life_support_status(player_id=player_id, force_tick=False)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "total": len(items),
                        "items": items,
                        "life_support": {
                            "inventory": life_support.get("inventory", {}),
                            "demand_per_hour": life_support.get("demand_per_hour", {}),
                            "production_per_hour": life_support.get("production_per_hour", {}),
                            "surplus_per_hour": life_support.get("surplus_per_hour", {}),
                            "state": life_support.get("state", {}),
                        },
                    },
                )
                return

            if path == "/api/life-support/status":
                self._reject_unknown_query_keys(query, allowed={"player_id", "force_tick"})
                player_id = self._parse_required_query_string(query, "player_id")
                force_tick = self._parse_bool_query(query, key="force_tick", default=False)
                self._ensure_player_bootstrap(player_id)
                payload = self._life_support_status(player_id=player_id, force_tick=force_tick)
                self._send_json(HTTPStatus.OK, payload)
                return

            if path == "/api/research/unlocks":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                tech_ids = self.state_store.list_unlocked_tech(player_id=player_id)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "total": len(tech_ids),
                        "items": tech_ids,
                    },
                )
                return

            if path == "/api/research/tracks":
                self._reject_unknown_query_keys(
                    query,
                    allowed={"limit", "entry_tech_id", "objective_type", "player_id"},
                )
                limit = self._parse_market_limit(query)
                entry_tech_id = self._parse_optional_string(query, "entry_tech_id")
                objective_type = self._parse_optional_string(query, "objective_type")
                player_id = self._parse_optional_string(query, "player_id")

                tracks = self.seed_store.research_tracks
                if entry_tech_id is not None:
                    entry_key = entry_tech_id.casefold()
                    tracks = [
                        track
                        for track in tracks
                        if isinstance(track, dict)
                        and isinstance(track.get("entry_tech_id"), str)
                        and track["entry_tech_id"].casefold() == entry_key
                    ]
                if objective_type is not None:
                    objective_key = objective_type.casefold()
                    tracks = [
                        track
                        for track in tracks
                        if isinstance(track, dict)
                        and isinstance(track.get("stages"), list)
                        and any(
                            isinstance(stage, dict)
                            and isinstance(stage.get("objective_type"), str)
                            and stage["objective_type"].casefold() == objective_key
                            for stage in track["stages"]
                        )
                    ]

                unlocked: set[str] = set()
                if player_id is not None:
                    self._ensure_player_bootstrap(player_id)
                    unlocked = set(self.state_store.list_unlocked_tech(player_id=player_id))

                items: list[dict[str, Any]] = []
                for track in tracks:
                    if not isinstance(track, dict):
                        continue
                    row = dict(track)
                    if player_id is not None:
                        entry = row.get("entry_tech_id")
                        row["is_unlocked"] = isinstance(entry, str) and entry in unlocked
                    items.append(row)

                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "entry_tech_id": entry_tech_id,
                        "objective_type": objective_type,
                        "player_id": player_id,
                        "items": items[:limit],
                    },
                )
                return

            if path == "/api/research/compute":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                compute = self._player_compute_profile(player_id=player_id)
                self._send_json(HTTPStatus.OK, compute)
                return

            if path == "/api/research/jobs":
                self._reject_unknown_query_keys(query, allowed={"player_id", "status", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_research_jobs(
                    player_id=player_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/manufacturing/jobs":
                self._reject_unknown_query_keys(query, allowed={"player_id", "status", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_manufacturing_jobs(
                    player_id=player_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/reverse-engineering/jobs":
                self._reject_unknown_query_keys(query, allowed={"player_id", "status", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_reverse_jobs(
                    player_id=player_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/inventory/storage":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                storage = self._compute_storage_profile(player_id=player_id)
                self._send_json(HTTPStatus.OK, storage)
                return

            if path == "/api/assets/smuggled":
                self._reject_unknown_query_keys(query, allowed={"player_id", "asset_type", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                asset_type = self._parse_optional_string(query, "asset_type")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_smuggled_assets(
                    player_id=player_id,
                    asset_type=asset_type,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "asset_type": asset_type,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/assets":
                self._reject_unknown_query_keys(query, allowed={"player_id", "asset_type", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                asset_type = self._parse_optional_string(query, "asset_type")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_assets(
                    player_id=player_id,
                    asset_type=asset_type,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "asset_type": asset_type,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/assets/instances":
                self._reject_unknown_query_keys(
                    query, allowed={"player_id", "asset_type", "asset_id", "limit"}
                )
                player_id = self._parse_required_query_string(query, "player_id")
                asset_type = self._parse_optional_string(query, "asset_type")
                asset_id = self._parse_optional_string(query, "asset_id")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_crafted_instances(
                    player_id=player_id,
                    asset_type=asset_type,
                    asset_id=asset_id,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "asset_type": asset_type,
                        "asset_id": asset_id,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/market/snapshot":
                self._reject_unknown_query_keys(query, allowed={"player_id", "limit", "region_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                limit = self._parse_market_limit(query)
                region_id = self._parse_optional_string(query, "region_id")
                self._ensure_player_bootstrap(player_id)
                snapshot = self._market_snapshot(
                    player_id=player_id,
                    limit=limit,
                    region_id=region_id,
                )
                self._send_json(HTTPStatus.OK, snapshot)
                return

            if path == "/api/market/listings":
                self._reject_unknown_query_keys(
                    query,
                    allowed={"limit", "asset_type", "asset_id", "region_id", "seller_player_id", "status"},
                )
                limit = self._parse_market_limit(query)
                asset_type = self._parse_optional_string(query, "asset_type")
                asset_id = self._parse_optional_string(query, "asset_id")
                region_id = self._parse_optional_string(query, "region_id")
                seller_player_id = self._parse_optional_string(query, "seller_player_id")
                status = self._parse_optional_string(query, "status") or "active"
                items = self.state_store.list_listings(
                    limit=limit,
                    asset_type=asset_type,
                    asset_id=asset_id,
                    region_id=region_id,
                    seller_player_id=seller_player_id,
                    status=status,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "asset_type": asset_type,
                        "asset_id": asset_id,
                        "region_id": region_id,
                        "seller_player_id": seller_player_id,
                        "status": status,
                        "items": items,
                    },
                )
                return

            if path == "/api/market/history":
                self._reject_unknown_query_keys(
                    query,
                    allowed={"limit", "asset_type", "asset_id", "currency", "trade_source"},
                )
                limit = self._parse_market_limit(query)
                asset_type = self._parse_optional_string(query, "asset_type")
                asset_id = self._parse_optional_string(query, "asset_id")
                currency = self._parse_optional_string(query, "currency")
                trade_source = self._parse_optional_string(query, "trade_source")
                rows = self.state_store.list_market_trade_history(
                    limit=limit,
                    asset_type=asset_type,
                    asset_id=asset_id,
                    currency=currency,
                    trade_source=trade_source,
                )
                price_summary = None
                if (
                    isinstance(asset_type, str)
                    and asset_type.strip()
                    and isinstance(asset_id, str)
                    and asset_id.strip()
                ):
                    price_summary = self.state_store.market_price_summary(
                        asset_type=asset_type.strip(),
                        asset_id=asset_id.strip(),
                        currency=currency.strip() if isinstance(currency, str) and currency.strip() else "credits",
                    )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(rows),
                        "limit": limit,
                        "asset_type": asset_type,
                        "asset_id": asset_id,
                        "currency": currency,
                        "trade_source": trade_source,
                        "price_summary": price_summary,
                        "items": rows,
                    },
                )
                return

            if path == "/api/market/policy":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, self._p2p_policy())
                return

            if path == "/api/market/regions":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(self.seed_store.market_regions),
                        "items": self.seed_store.market_regions,
                    },
                )
                return

            if path == "/api/consumables":
                self._reject_unknown_query_keys(query, allowed={"limit"})
                limit = self._parse_market_limit(query)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(self.seed_store.consumables),
                        "limit": limit,
                        "items": self.seed_store.consumables[:limit],
                    },
                )
                return

            if path == "/api/ai/opponents":
                self._reject_unknown_query_keys(query, allowed={"limit", "role"})
                limit = self._parse_market_limit(query)
                role = self._parse_optional_string(query, "role")
                items = self.seed_store.ai_opponents
                if isinstance(role, str):
                    role_key = role.casefold()
                    items = [
                        row
                        for row in items
                        if isinstance(row, dict)
                        and isinstance(row.get("role"), str)
                        and row["role"].casefold() == role_key
                    ]
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(items),
                        "limit": limit,
                        "role": role,
                        "items": items[:limit],
                    },
                )
                return

            if path == "/api/contracts/board":
                self._reject_unknown_query_keys(query, allowed={"limit", "type"})
                limit = self._parse_market_limit(query)
                contract_type = self._parse_optional_string(query, "type")
                templates = self.seed_store.contract_templates
                if contract_type is not None:
                    key = contract_type.casefold()
                    templates = [
                        item
                        for item in templates
                        if isinstance(item, dict)
                        and isinstance(item.get("type"), str)
                        and item["type"].casefold() == key
                    ]
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "total": len(templates),
                        "limit": limit,
                        "type": contract_type,
                        "items": templates[:limit],
                    },
                )
                return

            if path == "/api/contracts/jobs":
                self._reject_unknown_query_keys(query, allowed={"player_id", "status", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_contract_jobs(
                    player_id=player_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/missions/jobs":
                self._reject_unknown_query_keys(query, allowed={"player_id", "status", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                status = self._parse_optional_string(query, "status")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_mission_jobs(
                    player_id=player_id,
                    status=status,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "status": status,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/fleet/status":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                fleet = self._ensure_fleet_initialized(player_id)
                self._send_json(HTTPStatus.OK, fleet)
                return

            if path == "/api/fairplay/policy":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, self._fairplay_policy_payload())
                return

            if path == "/api/combat/contacts":
                self._reject_unknown_query_keys(query, allowed={"player_id", "count", "seed"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                count = self._parse_bounded_int(
                    query=query,
                    key="count",
                    default=6,
                    minimum=1,
                    maximum=20,
                )
                seed = self._parse_seed(query)
                result = self._combat_contacts(player_id=player_id, count=count, seed=seed)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/covert/policy":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_optional_string(query, "player_id")
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                self._send_json(
                    HTTPStatus.OK,
                    self._covert_policy_payload(
                        player_id=player_id.strip() if isinstance(player_id, str) and player_id.strip() else None
                    ),
                )
                return

            if path == "/api/covert/cooldowns":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                self._ensure_player_bootstrap(player_id)
                rows = self.state_store.list_covert_cooldowns(player_id=player_id)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "total": len(rows),
                        "items": rows,
                    },
                )
                return

            if path == "/api/covert/logs":
                self._reject_unknown_query_keys(query, allowed={"player_id", "perspective", "op_type", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                perspective = self._parse_optional_string(query, "perspective") or "both"
                op_type = self._parse_optional_string(query, "op_type")
                limit = self._parse_market_limit(query)
                self._ensure_player_bootstrap(player_id)
                rows = self.state_store.list_covert_logs(
                    player_id=player_id,
                    perspective=perspective,
                    op_type=op_type,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "perspective": perspective,
                        "op_type": op_type,
                        "total": len(rows),
                        "items": rows,
                    },
                )
                return

            if path == "/api/worlds/owned":
                self._reject_unknown_query_keys(query, allowed={"player_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                worlds = self.state_store.list_worlds_for_player(player_id=player_id)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "total": len(worlds),
                        "items": worlds,
                    },
                )
                return

            if path == "/api/worlds/detail":
                self._reject_unknown_query_keys(query, allowed={"player_id", "world_id"})
                player_id = self._parse_required_query_string(query, "player_id")
                world_id = self._parse_required_query_string(query, "world_id")
                world = self.state_store.get_world(world_id=world_id, player_id=player_id)
                projection = self._project_world_structure(
                    {"world": world, "structure_ids": world.get("built_structures", [])}
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "world": world,
                        "projection": projection,
                    },
                )
                return

            if path == "/api/worlds/population-projection":
                self._reject_unknown_query_keys(query, allowed={"player_id", "world_id", "days"})
                player_id = self._parse_required_query_string(query, "player_id")
                world_id = self._parse_required_query_string(query, "world_id")
                days = self._parse_bounded_float(
                    query=query,
                    key="days",
                    default=30.0,
                    minimum=0.0,
                    maximum=3650.0,
                )
                self._ensure_player_bootstrap(player_id)
                world = self.state_store.get_world(world_id=world_id, player_id=player_id)
                projection = self._project_world_population(world=world, days=days)
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "world_id": world_id,
                        "projection": projection,
                    },
                )
                return

            if path == "/api/discovery/scan":
                self._reject_unknown_query_keys(
                    query, allowed={"player_id", "body_class", "count", "seed", "scan_power"}
                )
                player_id = self._parse_optional_string(query, "player_id")
                body_class = self._parse_optional_string(query, "body_class")
                count = self._parse_bounded_int(
                    query=query,
                    key="count",
                    default=DEFAULT_SCAN_COUNT,
                    minimum=1,
                    maximum=MAX_SCAN_COUNT,
                )
                seed = self._parse_seed(query)
                scan_power = self._parse_bounded_float(
                    query=query,
                    key="scan_power",
                    default=DEFAULT_SCAN_POWER,
                    minimum=10.0,
                    maximum=MAX_SCAN_POWER,
                )
                energy: dict[str, Any] | None = None
                if isinstance(player_id, str) and player_id.strip():
                    self._ensure_player_bootstrap(player_id.strip())
                    energy = self._consume_player_action_energy(
                        player_id=player_id.strip(),
                        amount=ENERGY_COST_DISCOVERY_SCAN,
                        reason="discovery_scan",
                    )
                result = self._run_discovery_scan(
                    player_id=player_id.strip() if isinstance(player_id, str) and player_id.strip() else None,
                    body_class=body_class,
                    count=count,
                    seed=seed,
                    scan_power=scan_power,
                )
                if energy is not None:
                    result["player_id"] = player_id.strip()
                    result["energy_cost"] = ENERGY_COST_DISCOVERY_SCAN
                    result["energy"] = energy
                    items_payload = result.get("items", [])
                    if isinstance(items_payload, list):
                        self.state_store.catalog_discovered_worlds(
                            player_id=player_id.strip(),
                            worlds=items_payload,
                        )
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/discovery/catalog":
                self._reject_unknown_query_keys(query, allowed={"player_id", "limit"})
                player_id = self._parse_required_query_string(query, "player_id")
                limit = self._parse_limit(query)
                self._ensure_player_bootstrap(player_id)
                items = self.state_store.list_discovered_worlds(
                    player_id=player_id,
                    limit=limit,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id,
                        "total": len(items),
                        "limit": limit,
                        "items": items,
                    },
                )
                return

            if path == "/api/admin/players":
                result = self._admin_list_players(query)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/admin/actions":
                self._reject_unknown_query_keys(
                    query, allowed={"player_id", "limit", "target_player_id"}
                )
                admin_player_id = self._parse_required_query_string(query, "player_id")
                self._require_admin(admin_player_id=admin_player_id)
                limit = self._parse_market_limit(query)
                target_player_id = self._parse_optional_string(query, "target_player_id")
                items = self.state_store.list_admin_actions(
                    limit=limit,
                    admin_player_id=admin_player_id,
                    target_player_id=target_player_id,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "admin_player_id": admin_player_id,
                        "target_player_id": target_player_id,
                        "total": len(items),
                        "items": items,
                    },
                )
                return

            if path == "/api/manifest":
                self._reject_unknown_query_keys(query, allowed=set())
                self._send_json(HTTPStatus.OK, self.seed_store.manifest)
                return

            self._send_error(
                HTTPStatus.NOT_FOUND,
                f"Unknown endpoint: {path}",
            )

        except AuthError as exc:
            self._send_error(HTTPStatus.UNAUTHORIZED, str(exc))
        except (ValueError, StateStoreError) as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
        except Exception as exc:  # pragma: no cover - defensive path
            logging.exception("Unhandled server error")
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Internal server error: {exc}")

    def do_POST(self) -> None:  # noqa: N802 (http method naming)
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query, keep_blank_values=True)

            if path == "/api/admin/login":
                self._reject_unknown_query_keys(query, allowed=set())
                if not self.admin_login_enabled:
                    self._send_error(
                        HTTPStatus.FORBIDDEN,
                        "Admin login is disabled by server configuration",
                    )
                    return
                payload = self._parse_json_body()
                username = payload.get("username")
                password = payload.get("password")
                if username != self.admin_username or password != self.admin_password:
                    raise ValueError("Invalid admin credentials")
                profile_payload = {
                    "player_id": "admin",
                    "captain_name": "Administrator",
                    "display_name": "Administrator",
                    "auth_mode": "guest",
                    "email": "",
                    "starting_ship_id": "ship.aegis_support_cruiser",
                    "tutorial_mode": "skip",
                    "player_memory": {
                        "onboarding": {
                            "completed": True,
                            "mode": "skip",
                        },
                        "roles": {"admin": True},
                    },
                }
                self.state_store.upsert_profile(profile_payload)
                self._ensure_player_bootstrap("admin", skip_auth=True)
                if self.admin_god_mode_enabled:
                    self._grant_admin_god_mode("admin")
                auth = self._issue_session_token(player_id="admin", role="admin")
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "player_id": "admin",
                        "god_mode": bool(self.admin_god_mode_enabled),
                        "auth": auth,
                        "wallet": self.state_store.get_wallet("admin"),
                    },
                )
                return

            if path == "/api/admin/players/moderate":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._admin_kick_player(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/admin/crafting/jackpot":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._admin_force_jackpot_craft(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/covert/steal":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._execute_covert_op(payload, op_type="steal")
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/covert/sabotage":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._execute_covert_op(payload, op_type="sabotage")
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/covert/hack":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._execute_covert_op(payload, op_type="hack")
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/player/login":
                self._reject_unknown_query_keys(query, allowed=set())
                if not self.player_login_enabled:
                    self._send_error(
                        HTTPStatus.FORBIDDEN,
                        "Player dev login is disabled by server configuration",
                    )
                    return
                payload = self._parse_json_body()
                username = payload.get("username")
                password = payload.get("password")
                if username != self.player_username or password != self.player_password:
                    raise ValueError("Invalid player credentials")
                player_id = "player"
                if not self.state_store.profile_exists(player_id):
                    starter_catalog = self._starter_ship_catalog()
                    starter_id = (
                        str(starter_catalog[0]["id"])
                        if starter_catalog
                        and isinstance(starter_catalog[0], dict)
                        and isinstance(starter_catalog[0].get("id"), str)
                        else None
                    )
                    self.state_store.upsert_profile(
                        {
                            "player_id": player_id,
                            "captain_name": "Player Captain",
                            "display_name": "Player Captain",
                            "auth_mode": "guest",
                            "email": "",
                            "starting_ship_id": starter_id,
                            "tutorial_mode": "guided",
                            "player_memory": {
                                "onboarding": {"completed": False, "mode": "guided"},
                                "roles": {"player": True},
                            },
                        }
                    )
                self._ensure_player_bootstrap(player_id, skip_auth=True)
                auth = self._issue_session_token(player_id=player_id, role="player")
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "player_id": player_id,
                        "auth": auth,
                        "profile": self.state_store.get_profile(player_id),
                        "wallet": self.state_store.get_wallet(player_id),
                    },
                )
                return

            if path == "/api/combat/simulate":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                normalized = self._normalize_combat_payload(payload)
                result = self._simulate_combat(normalized)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/combat/odds":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                odds_profile: dict[str, Any] | None = None
                player_id_raw = payload.get("player_id")
                if isinstance(player_id_raw, str) and player_id_raw.strip():
                    player_id = player_id_raw.strip()
                    self._ensure_player_bootstrap(player_id)
                    odds_profile = self._resolve_authenticated_player_combat_profile(
                        player_id=player_id,
                    )
                    attacker_payload = payload.get("attacker")
                    attacker = dict(attacker_payload) if isinstance(attacker_payload, dict) else {}
                    attacker_name_raw = attacker.get("name", "Player")
                    attacker_name = (
                        attacker_name_raw.strip()
                        if isinstance(attacker_name_raw, str) and attacker_name_raw.strip()
                        else "Player"
                    )
                    attacker["name"] = attacker_name
                    attacker["stats"] = odds_profile.get(
                        "stats",
                        self._player_combat_stats(player_id=player_id),
                    )
                    payload = dict(payload)
                    payload["attacker"] = attacker
                normalized = self._normalize_combat_payload(payload)
                result = self._estimate_combat_odds(normalized)
                if isinstance(odds_profile, dict):
                    result["attacker_source"] = str(
                        odds_profile.get("source", "legacy.inventory_projection")
                    )
                    loadout = odds_profile.get("loadout")
                    if isinstance(loadout, dict):
                        result["attacker_loadout"] = loadout
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/combat/engage":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                self._ensure_player_bootstrap(player_id.strip())
                current_energy = self._get_player_action_energy(player_id=player_id.strip())
                if float(current_energy.get("current_energy", 0.0)) + 1e-9 < ENERGY_COST_COMBAT_ENGAGE:
                    raise ValueError(
                        "Insufficient action energy for combat_engage "
                        f"(need {ENERGY_COST_COMBAT_ENGAGE:.2f}, have {float(current_energy.get('current_energy', 0.0)):.2f})"
                    )
                result = self._engage_contact(payload)
                energy = self._consume_player_action_energy(
                    player_id=player_id.strip(),
                    amount=ENERGY_COST_COMBAT_ENGAGE,
                    reason="combat_engage",
                )
                result["energy_cost"] = ENERGY_COST_COMBAT_ENGAGE
                result["energy"] = energy
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/combat/auto-resolve":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                self._ensure_player_bootstrap(player_id.strip())
                current_energy = self._get_player_action_energy(player_id=player_id.strip())
                if float(current_energy.get("current_energy", 0.0)) + 1e-9 < ENERGY_COST_COMBAT_AUTO_RESOLVE:
                    raise ValueError(
                        "Insufficient action energy for combat_auto_resolve "
                        f"(need {ENERGY_COST_COMBAT_AUTO_RESOLVE:.2f}, have {float(current_energy.get('current_energy', 0.0)):.2f})"
                    )
                result = self._auto_resolve_hostile(payload)
                energy = self._consume_player_action_energy(
                    player_id=player_id.strip(),
                    amount=ENERGY_COST_COMBAT_AUTO_RESOLVE,
                    reason="combat_auto_resolve",
                )
                result["energy_cost"] = ENERGY_COST_COMBAT_AUTO_RESOLVE
                result["energy"] = energy
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/fitting/simulate":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._simulate_fitting(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/profile/save":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                requested_player_id = payload.get("player_id")
                if not isinstance(requested_player_id, str) or not requested_player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                requested_player_id = requested_player_id.strip()
                if self.auth_mode != "jwt" and requested_player_id == "admin":
                    raise ValueError("player_id 'admin' is reserved")
                starting_ship_id = payload.get("starting_ship_id")
                if starting_ship_id is not None:
                    if not isinstance(starting_ship_id, str) or not starting_ship_id.strip():
                        raise ValueError("starting_ship_id must be a non-empty string when provided")
                    if self._starter_ship_by_id(starting_ship_id.strip()) is None:
                        raise ValueError(f"Unknown starting_ship_id '{starting_ship_id}'")
                payload_to_save = dict(payload)
                existing_profile = self.state_store.profile_exists(requested_player_id)
                if self.auth_required:
                    if self.auth_mode == "jwt":
                        authenticated_player_id = self._require_authenticated_player()
                        payload_to_save["player_id"] = authenticated_player_id
                        requested_player_id = authenticated_player_id
                        existing_profile = self.state_store.profile_exists(requested_player_id)
                    else:
                        token = self._extract_bearer_token()
                        if token is None:
                            if existing_profile:
                                raise AuthError(
                                    "Existing profile updates require Authorization bearer token"
                                )
                        else:
                            session = self._lookup_session(token)
                            if not isinstance(session, dict):
                                raise AuthError("Invalid or expired Authorization token")
                            if session.get("player_id") != requested_player_id:
                                raise AuthError(
                                    "Authorization token does not match payload player_id"
                                )
                if requested_player_id == "admin":
                    raise ValueError("player_id 'admin' is reserved")
                profile = self.state_store.upsert_profile(payload_to_save)
                self._ensure_player_bootstrap(profile["player_id"], skip_auth=True)
                faction_id_payload = payload_to_save.get("faction_id")
                if isinstance(faction_id_payload, str) and faction_id_payload.strip():
                    faction_id_value = faction_id_payload.strip()
                    if self.seed_store.faction_index().get(faction_id_value) is None:
                        raise ValueError(f"Unknown faction_id '{faction_id_value}'")
                    self.state_store.set_player_faction_affiliation(
                        player_id=profile["player_id"],
                        faction_id=faction_id_value,
                        standing=0.0,
                        role="member",
                    )
                    profile = self.state_store.get_profile(player_id=profile["player_id"])
                profile["action_energy"] = self._get_player_action_energy(
                    player_id=profile["player_id"]
                )
                auth = self._issue_session_token(player_id=profile["player_id"], role="player")
                self._send_json(HTTPStatus.OK, {"profile": profile, "auth": auth})
                return

            if path == "/api/profile/memory":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                memory_payload = payload.get("player_memory", {})
                if not isinstance(memory_payload, dict):
                    raise ValueError("player_memory must be an object")
                merge = payload.get("merge", True)
                if not isinstance(merge, bool):
                    raise ValueError("merge must be boolean when provided")
                self._ensure_player_bootstrap(player_id.strip())
                profile = self.state_store.update_profile_memory(
                    player_id=player_id.strip(),
                    player_memory=memory_payload,
                    merge=merge,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "player_id": player_id.strip(),
                        "player_memory": profile.get("player_memory", {}),
                        "updated_utc": profile.get("updated_utc"),
                    },
                )
                return

            if path == "/api/profile/pvp-visibility":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                enabled = payload.get("allow_high_risk_visibility")
                if not isinstance(enabled, bool):
                    raise ValueError("allow_high_risk_visibility must be boolean")
                threshold_raw = payload.get("high_risk_loss_threshold")
                if threshold_raw is not None and (
                    isinstance(threshold_raw, bool) or not isinstance(threshold_raw, (int, float))
                ):
                    raise ValueError("high_risk_loss_threshold must be numeric when provided")
                self._ensure_player_bootstrap(player_id.strip())
                settings = self.state_store.set_pvp_visibility_setting(
                    player_id=player_id.strip(),
                    allow_high_risk_visibility=enabled,
                    high_risk_loss_threshold=float(threshold_raw) if threshold_raw is not None else None,
                )
                self._send_json(
                    HTTPStatus.OK,
                    {"player_id": player_id.strip(), "pvp_visibility": settings},
                )
                return

            if path == "/api/factions/align":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._faction_align(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/factions/leave":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._faction_leave(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/create":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._create_legion(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/join":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._join_legion(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/requests/respond":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._respond_legion_request(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/leave":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._leave_legion(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/members/role":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._set_legion_member_role(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/governance/propose":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._create_legion_proposal(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/governance/vote":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._vote_legion_proposal(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/legions/governance/finalize":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._finalize_legion_proposal(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/worlds/claim":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                world = payload.get("world")
                world_id = payload.get("world_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if world_id is None and isinstance(world, dict):
                    world_id = world.get("world_id")
                if not isinstance(world_id, str) or not world_id.strip():
                    raise ValueError("world_id must be provided (or world.world_id)")
                self._ensure_player_bootstrap(player_id.strip())
                discovered = self.state_store.get_discovered_world(
                    player_id=player_id.strip(),
                    world_id=world_id.strip(),
                )
                claimed = self.state_store.claim_world(player_id=player_id.strip(), world=discovered)
                self._send_json(HTTPStatus.OK, {"world": claimed})
                return

            if path == "/api/worlds/build-structure":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                world_id = payload.get("world_id")
                structure_id = payload.get("structure_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if not isinstance(world_id, str) or not world_id.strip():
                    raise ValueError("world_id must be a non-empty string")
                if not isinstance(structure_id, str) or not structure_id.strip():
                    raise ValueError("structure_id must be a non-empty string")
                self._ensure_player_bootstrap(player_id.strip())
                result = self._craft_item(
                    player_id=player_id.strip(),
                    item_id=structure_id.strip(),
                    quantity=1,
                    world_id=world_id.strip(),
                )
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/worlds/project-structure":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._project_world_structure(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/worlds/harvest":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                world_id = payload.get("world_id")
                hours = payload.get("hours", 1)
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if not isinstance(world_id, str) or not world_id.strip():
                    raise ValueError("world_id must be a non-empty string")
                if isinstance(hours, bool) or not isinstance(hours, (int, float)):
                    raise ValueError("hours must be numeric")
                if float(hours) <= 0 or float(hours) > 24:
                    raise ValueError("hours must be between 0 and 24")
                self._ensure_player_bootstrap(player_id.strip())
                energy_cost = ENERGY_COST_WORLD_HARVEST * max(1.0, min(4.0, math.sqrt(float(hours))))
                current_energy = self._get_player_action_energy(player_id=player_id.strip())
                if float(current_energy.get("current_energy", 0.0)) + 1e-9 < energy_cost:
                    raise ValueError(
                        "Insufficient action energy for world_harvest "
                        f"(need {float(energy_cost):.2f}, have {float(current_energy.get('current_energy', 0.0)):.2f})"
                    )
                result = self._harvest_world(
                    player_id=player_id.strip(),
                    world_id=world_id.strip(),
                    hours=float(hours),
                )
                energy = self._consume_player_action_energy(
                    player_id=player_id.strip(),
                    amount=energy_cost,
                    reason="world_harvest",
                )
                result["energy_cost"] = round(float(energy_cost), 3)
                result["energy"] = energy
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/market/buy":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._trade_market(payload=payload, side="buy")
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/market/sell":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._trade_market(payload=payload, side="sell")
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/market/listings/create":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._create_market_listing(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/market/listings/cancel":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._cancel_market_listing(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/market/listings/buy":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._buy_market_listing(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/market/exchange":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._exchange_currency(payload=payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/crafting/quote":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                item_id = payload.get("item_id")
                quantity = payload.get("quantity", 1)
                world_id = payload.get("world_id")
                substitution_id = payload.get("substitution_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if not isinstance(item_id, str) or not item_id.strip():
                    raise ValueError("item_id must be a non-empty string")
                if isinstance(quantity, bool) or not isinstance(quantity, int):
                    raise ValueError("quantity must be an integer")
                if quantity <= 0:
                    raise ValueError("quantity must be > 0")
                if world_id is not None and not isinstance(world_id, str):
                    raise ValueError("world_id must be a string when provided")
                if substitution_id is not None and not isinstance(substitution_id, str):
                    raise ValueError("substitution_id must be a string when provided")
                self._ensure_player_bootstrap(player_id.strip())
                quote = self._crafting_quote(
                    player_id=player_id.strip(),
                    item_id=item_id.strip(),
                    quantity=quantity,
                    world_id=world_id.strip() if isinstance(world_id, str) else None,
                    substitution_id=(
                        substitution_id.strip() if isinstance(substitution_id, str) else None
                    ),
                )
                self._send_json(HTTPStatus.OK, quote)
                return

            if path == "/api/research/start":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                tech_id = payload.get("tech_id")
                substitution_id = payload.get("substitution_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if not isinstance(tech_id, str) or not tech_id.strip():
                    raise ValueError("tech_id must be a non-empty string")
                if substitution_id is not None and not isinstance(substitution_id, str):
                    raise ValueError("substitution_id must be a string when provided")
                self._ensure_player_bootstrap(player_id.strip())
                result = self._start_research_job(
                    player_id=player_id.strip(),
                    tech_id=tech_id.strip(),
                    substitution_id=substitution_id.strip() if isinstance(substitution_id, str) else None,
                )
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/research/claim":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                job_id = payload.get("job_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if not isinstance(job_id, str) or not job_id.strip():
                    raise ValueError("job_id must be a non-empty string")
                self._ensure_player_bootstrap(player_id.strip())
                result = self._claim_research_job(
                    player_id=player_id.strip(),
                    job_id=job_id.strip(),
                )
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/crafting/build":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                player_id = payload.get("player_id")
                item_id = payload.get("item_id")
                quantity = payload.get("quantity", 1)
                world_id = payload.get("world_id")
                substitution_id = payload.get("substitution_id")
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError("player_id must be a non-empty string")
                if not isinstance(item_id, str) or not item_id.strip():
                    raise ValueError("item_id must be a non-empty string")
                if isinstance(quantity, bool) or not isinstance(quantity, int):
                    raise ValueError("quantity must be an integer")
                if quantity <= 0:
                    raise ValueError("quantity must be > 0")
                if world_id is not None and not isinstance(world_id, str):
                    raise ValueError("world_id must be a string when provided")
                if substitution_id is not None and not isinstance(substitution_id, str):
                    raise ValueError("substitution_id must be a string when provided")
                self._ensure_player_bootstrap(player_id.strip())
                result = self._craft_item(
                    player_id=player_id.strip(),
                    item_id=item_id.strip(),
                    quantity=quantity,
                    world_id=world_id.strip() if isinstance(world_id, str) else None,
                    substitution_id=(
                        substitution_id.strip() if isinstance(substitution_id, str) else None
                    ),
                )
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/inventory/storage/upgrade":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._upgrade_storage(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/assets/smuggle/move":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._move_smuggled_assets(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/inventory/trash":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._trash_inventory_item(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/assets/instances/level-up":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._level_asset_instance(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/manufacturing/start":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._start_manufacturing_job(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/manufacturing/claim":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._claim_manufacturing_job(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/manufacturing/cancel":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._cancel_manufacturing_job(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/reverse-engineering/start":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._start_reverse_engineering(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/reverse-engineering/claim":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._claim_reverse_engineering(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/missions/accept":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._accept_mission(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/missions/progress":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._progress_mission(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/missions/claim":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._claim_mission(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/contracts/accept":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._accept_contract(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/contracts/complete":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._complete_contract(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            if path == "/api/contracts/abandon":
                self._reject_unknown_query_keys(query, allowed=set())
                payload = self._parse_json_body()
                result = self._abandon_contract(payload)
                self._send_json(HTTPStatus.OK, result)
                return

            self._send_error(HTTPStatus.NOT_FOUND, f"Unknown endpoint: {path}")

        except AuthError as exc:
            self._send_error(HTTPStatus.UNAUTHORIZED, str(exc))
        except (ValueError, StateStoreError) as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
        except Exception as exc:  # pragma: no cover - defensive path
            logging.exception("Unhandled server error")
            self._send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Internal server error: {exc}")

    def end_headers(self) -> None:
        origin = self.headers.get("Origin")
        if self.allow_all_origins:
            if isinstance(origin, str) and origin.strip():
                self.send_header("Access-Control-Allow-Origin", origin)
                self.send_header("Vary", "Origin")
            else:
                self.send_header("Access-Control-Allow-Origin", "*")
        elif isinstance(origin, str) and origin in self.allowed_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token")
        self.send_header("Access-Control-Max-Age", "600")
        super().end_headers()

    def _send_json(self, status: HTTPStatus, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=True, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: HTTPStatus, message: str) -> None:
        self._send_json(status, {"error": {"status": status, "message": message}})

    def _reject_unknown_query_keys(self, query: dict[str, list[str]], allowed: set[str]) -> None:
        unknown = sorted(set(query.keys()) - allowed)
        if unknown:
            joined = ", ".join(unknown)
            raise ValueError(f"Unsupported query parameter(s): {joined}")

    def _parse_limit(self, query: dict[str, list[str]]) -> int:
        values = query.get("limit")
        if not values:
            return DEFAULT_LIMIT
        if len(values) != 1:
            raise ValueError("Query parameter 'limit' must be provided once")

        raw = values[0].strip()
        if not raw:
            raise ValueError("Query parameter 'limit' cannot be empty")

        try:
            limit = int(raw)
        except ValueError as exc:
            raise ValueError("Query parameter 'limit' must be an integer") from exc

        if limit <= 0:
            raise ValueError("Query parameter 'limit' must be greater than 0")
        if limit > MAX_LIMIT:
            raise ValueError(f"Query parameter 'limit' must be <= {MAX_LIMIT}")

        return limit

    def _parse_market_limit(self, query: dict[str, list[str]]) -> int:
        values = query.get("limit")
        if not values:
            return DEFAULT_MARKET_LIMIT
        if len(values) != 1:
            raise ValueError("Query parameter 'limit' must be provided once")
        raw = values[0].strip()
        if not raw:
            raise ValueError("Query parameter 'limit' cannot be empty")
        try:
            limit = int(raw)
        except ValueError as exc:
            raise ValueError("Query parameter 'limit' must be an integer") from exc
        if limit <= 0:
            raise ValueError("Query parameter 'limit' must be greater than 0")
        if limit > MAX_MARKET_LIMIT:
            raise ValueError(f"Query parameter 'limit' must be <= {MAX_MARKET_LIMIT}")
        return limit

    def _parse_optional_int(self, query: dict[str, list[str]], key: str) -> int | None:
        values = query.get(key)
        if values is None:
            return None
        if len(values) != 1:
            raise ValueError(f"Query parameter '{key}' must be provided once")
        raw = values[0].strip()
        if not raw:
            raise ValueError(f"Query parameter '{key}' cannot be empty")
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(f"Query parameter '{key}' must be an integer") from exc

    def _parse_bounded_int(
        self,
        query: dict[str, list[str]],
        key: str,
        default: int,
        minimum: int,
        maximum: int,
    ) -> int:
        parsed = self._parse_optional_int(query, key)
        value = default if parsed is None else parsed
        if value < minimum or value > maximum:
            raise ValueError(f"Query parameter '{key}' must be between {minimum} and {maximum}")
        return value

    def _parse_bounded_float(
        self,
        query: dict[str, list[str]],
        key: str,
        default: float,
        minimum: float,
        maximum: float,
    ) -> float:
        values = query.get(key)
        if values is None:
            value = default
        else:
            if len(values) != 1:
                raise ValueError(f"Query parameter '{key}' must be provided once")
            raw = values[0].strip()
            if not raw:
                raise ValueError(f"Query parameter '{key}' cannot be empty")
            try:
                value = float(raw)
            except ValueError as exc:
                raise ValueError(f"Query parameter '{key}' must be numeric") from exc

        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"Query parameter '{key}' must be finite")
        if value < minimum or value > maximum:
            raise ValueError(
                f"Query parameter '{key}' must be between {minimum} and {maximum}"
            )
        return value

    def _parse_seed(self, query: dict[str, list[str]]) -> int:
        values = query.get("seed")
        if values is None:
            if DETERMINISTIC_MODE:
                return int(stable_hash_int(self.path, "query_seed"))
            return int(time.time())
        if len(values) != 1:
            raise ValueError("Query parameter 'seed' must be provided once")
        raw = values[0].strip()
        if not raw:
            raise ValueError("Query parameter 'seed' cannot be empty")
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError("Query parameter 'seed' must be an integer") from exc

    def _parse_optional_string(self, query: dict[str, list[str]], key: str) -> str | None:
        values = query.get(key)
        if values is None:
            return None
        if len(values) != 1:
            raise ValueError(f"Query parameter '{key}' must be provided once")

        value = values[0].strip()
        if not value:
            raise ValueError(f"Query parameter '{key}' cannot be empty")
        return value

    def _parse_bool_query(
        self,
        query: dict[str, list[str]],
        key: str,
        default: bool = False,
    ) -> bool:
        value = self._parse_optional_string(query, key)
        if value is None:
            return bool(default)
        lowered = value.strip().casefold()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        raise ValueError(
            f"Query parameter '{key}' must be boolean (true/false/1/0/yes/no/on/off)"
        )

    def _parse_required_query_string(self, query: dict[str, list[str]], key: str) -> str:
        value = self._parse_optional_string(query, key)
        if value is None:
            raise ValueError(f"Query parameter '{key}' is required")
        if key == "player_id":
            self._require_authenticated_player(expected_player_id=value)
        return value

    def _parse_json_body(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValueError("Missing Content-Length header")
        try:
            content_length = int(raw_length)
        except ValueError as exc:
            raise ValueError("Invalid Content-Length header") from exc
        if content_length <= 0:
            raise ValueError("Request body must not be empty")
        if content_length > 1_000_000:
            raise ValueError("Request body too large")

        raw_body = self.rfile.read(content_length)
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Request body must be valid JSON") from exc

        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _parse_numeric(self, value: Any, label: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"'{label}' must be numeric")
        num = float(value)
        if math.isnan(num) or math.isinf(num):
            raise ValueError(f"'{label}' must be finite")
        return num

    def _parse_non_negative_stat(self, stats: dict[str, Any], key: str, side: str) -> float:
        if key not in stats:
            raise ValueError(f"Missing stat '{key}' for {side}")
        value = self._parse_numeric(stats[key], f"{side}.stats.{key}")
        if value < 0:
            raise ValueError(f"{side}.stats.{key} must be >= 0")
        return value

    def _normalize_damage_profile(self, raw: Any, label: str) -> dict[str, float]:
        if raw is None:
            equal = 1.0 / float(len(DAMAGE_TYPES))
            return {dtype: equal for dtype in DAMAGE_TYPES}
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be an object when provided")
        out: dict[str, float] = {}
        for dtype in DAMAGE_TYPES:
            value = raw.get(dtype, 0.0)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{label}.{dtype} must be numeric")
            out[dtype] = max(0.0, float(value))
        total = sum(out.values())
        if total <= 1e-9:
            equal = 1.0 / float(len(DAMAGE_TYPES))
            return {dtype: equal for dtype in DAMAGE_TYPES}
        return {dtype: out[dtype] / total for dtype in DAMAGE_TYPES}

    def _normalize_resistance_profile(self, raw: Any, label: str) -> dict[str, float]:
        if raw is None:
            return {dtype: 0.08 for dtype in DAMAGE_TYPES}
        if not isinstance(raw, dict):
            raise ValueError(f"{label} must be an object when provided")
        out: dict[str, float] = {}
        for dtype in DAMAGE_TYPES:
            value = raw.get(dtype, 0.08)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{label}.{dtype} must be numeric")
            out[dtype] = max(0.0, min(0.85, float(value)))
        return out

    def _normalize_side(self, payload: dict[str, Any], side: str) -> dict[str, Any]:
        side_obj = payload.get(side)
        if not isinstance(side_obj, dict):
            raise ValueError(f"Missing or invalid '{side}' object")
        stats = side_obj.get("stats")
        if not isinstance(stats, dict):
            raise ValueError(f"Missing or invalid '{side}.stats' object")

        normalized_stats = {
            key: self._parse_non_negative_stat(stats, key, side) for key in STAT_KEYS
        }
        if normalized_stats["hull"] <= 0:
            raise ValueError(f"{side}.stats.hull must be > 0")
        if normalized_stats["energy"] <= 0:
            raise ValueError(f"{side}.stats.energy must be > 0")
        raw_profiles = side_obj.get("profiles", {})
        if raw_profiles is None:
            raw_profiles = {}
        if not isinstance(raw_profiles, dict):
            raise ValueError(f"{side}.profiles must be an object when provided")
        profiles = {
            "damage_profile": self._normalize_damage_profile(
                raw_profiles.get("damage_profile"),
                f"{side}.profiles.damage_profile",
            ),
            "resistance_profile": self._normalize_resistance_profile(
                raw_profiles.get("resistance_profile"),
                f"{side}.profiles.resistance_profile",
            ),
        }

        return {
            "name": str(side_obj.get("name", side.title()))[:40],
            "stats": normalized_stats,
            "profiles": profiles,
            "race_id": side_obj.get("race_id"),
            "profession_id": side_obj.get("profession_id"),
            "faction_id": side_obj.get("faction_id"),
            "legion_id": side_obj.get("legion_id"),
        }

    def _normalize_tactical_commands(self, context: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        raw = context.get("tactical_commands", {})
        if raw is None:
            return {"attacker": [], "defender": []}
        if not isinstance(raw, dict):
            raise ValueError("context.tactical_commands must be an object when provided")

        normalized: dict[str, list[dict[str, Any]]] = {"attacker": [], "defender": []}
        for side in ("attacker", "defender"):
            rows = raw.get(side, [])
            if rows is None:
                rows = []
            if not isinstance(rows, list):
                raise ValueError(f"context.tactical_commands.{side} must be an array")
            out_rows: list[dict[str, Any]] = []
            seen_rounds: set[int] = set()
            for idx, row in enumerate(rows):
                if not isinstance(row, dict):
                    raise ValueError(f"context.tactical_commands.{side}[{idx}] must be an object")
                round_raw = row.get("round")
                if isinstance(round_raw, bool) or not isinstance(round_raw, int):
                    raise ValueError(
                        f"context.tactical_commands.{side}[{idx}].round must be an integer"
                    )
                if round_raw <= 0 or round_raw > MAX_COMBAT_ROUNDS:
                    raise ValueError(
                        f"context.tactical_commands.{side}[{idx}].round must be within combat round bounds"
                    )
                if round_raw in seen_rounds:
                    raise ValueError(
                        f"context.tactical_commands.{side}[{idx}].round duplicates an existing round"
                    )
                seen_rounds.add(round_raw)
                action = str(row.get("action", "")).strip().casefold()
                if action not in {"main_ability", "boost_thrust", "evade", "stealth_burst"}:
                    raise ValueError(
                        f"context.tactical_commands.{side}[{idx}].action must be one of: main_ability, boost_thrust, evade, stealth_burst"
                    )
                magnitude = row.get("magnitude", 1.0)
                if isinstance(magnitude, bool) or not isinstance(magnitude, (int, float)):
                    raise ValueError(
                        f"context.tactical_commands.{side}[{idx}].magnitude must be numeric"
                    )
                out_rows.append(
                    {
                        "round": int(round_raw),
                        "action": action,
                        "magnitude": max(0.2, min(3.0, float(magnitude))),
                    }
                )
            out_rows.sort(key=lambda item: int(item["round"]))
            normalized[side] = out_rows
        return normalized

    def _normalize_combat_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        attacker = self._normalize_side(payload, "attacker")
        defender = self._normalize_side(payload, "defender")

        context = payload.get("context", {})
        if context is None:
            context = {}
        if not isinstance(context, dict):
            raise ValueError("'context' must be an object when provided")

        mode = str(context.get("mode", "pvp"))
        if mode not in {"pvp", "npc", "elite", "boss", "base"}:
            raise ValueError("context.mode must be one of: pvp, npc, elite, boss, base")

        raw_rounds = context.get("max_rounds", DEFAULT_COMBAT_ROUNDS)
        if isinstance(raw_rounds, bool) or not isinstance(raw_rounds, int):
            raise ValueError("context.max_rounds must be an integer")
        if raw_rounds <= 0 or raw_rounds > MAX_COMBAT_ROUNDS:
            raise ValueError(f"context.max_rounds must be between 1 and {MAX_COMBAT_ROUNDS}")

        seed_value = context.get("seed")
        if seed_value is None:
            if DETERMINISTIC_MODE:
                seed_value = int(stable_hash_int(payload.get("battle_id", "battle"), "combat_seed"))
            else:
                seed_value = int(time.time())
        if isinstance(seed_value, bool) or not isinstance(seed_value, (int, float, str)):
            raise ValueError("context.seed must be int/float/string when provided")
        try:
            seed = int(seed_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("context.seed must be coercible to integer") from exc
        tactical_commands = self._normalize_tactical_commands(context=context)

        return {
            "battle_id": payload.get("battle_id") or str(uuid.uuid4()),
            "attacker": attacker,
            "defender": defender,
            "context": {
                "mode": mode,
                "max_rounds": raw_rounds,
                "seed": seed,
                "counterfire_enabled": bool(context.get("counterfire_enabled", True)),
                "damage_cap": self._parse_numeric(context.get("damage_cap", 5000.0), "context.damage_cap"),
                "tactical_commands": tactical_commands,
            },
        }

    def _default_starter_ship_catalog(self) -> list[dict[str, Any]]:
        hull_index = self.seed_store.hull_index()
        fallback_hull = hull_index.get("hull.settler_scout")
        if not isinstance(fallback_hull, dict):
            for hull in self.seed_store.ship_hulls:
                if isinstance(hull, dict) and isinstance(hull.get("id"), str):
                    fallback_hull = hull
                    break
        if not isinstance(fallback_hull, dict):
            return []
        fallback_hull_id = str(fallback_hull.get("id", "hull.settler_scout"))
        fallback_stats = fallback_hull.get("base_stats", {})
        if not isinstance(fallback_stats, dict):
            fallback_stats = {}
        fallback_slots = fallback_hull.get("module_slots", {})
        if not isinstance(fallback_slots, dict):
            fallback_slots = {}
        growth_profile = {
            "xp_curve": "infinite_softcap",
            "xp_formula": "xp_to_next = 120 * level^1.35 * (1 + 0.011 * max(level-18,0)^0.72)",
            "stat_gain_per_level_pct": {
                "hull": 0.34,
                "shield": 0.27,
                "attack": 0.21,
                "defense": 0.21,
                "energy": 0.29,
            },
            "module_slot_growth": [
                {"level": 20, "slot": "utility", "delta": 1},
                {"level": 40, "slot": "special", "delta": 1},
                {"level": 70, "slot": "weapon", "delta": 1},
                {"level": 110, "slot": "defense", "delta": 1},
            ],
            "design_notes": "No hard level cap. Growth slows with level but continues indefinitely.",
        }
        return [
            {
                "id": "ship.pathfinder_frigate",
                "name": "Pathfinder Frigate",
                "description": "Balanced starter with stronger scan and logistics baseline.",
                "role": "balanced_scout",
                "hull_id": fallback_hull_id,
                "hull_name": str(fallback_hull.get("name", fallback_hull_id)),
                "hull_class": str(fallback_hull.get("class", "scout")),
                "hull_tier": int(fallback_hull.get("tier", 1)),
                "hull_base_stats": fallback_stats,
                "module_slots": fallback_slots,
                "starter_assets": [
                    {
                        "asset_type": "module",
                        "asset_id": "module.scanner_longrange_array_mk1",
                        "quantity": 1,
                    },
                    {"asset_type": "module", "asset_id": "module.relay_grid_mk1", "quantity": 1},
                    {
                        "asset_type": "module",
                        "asset_id": "module.utility_heat_sink_mk1",
                        "quantity": 1,
                    },
                ],
                "growth_profile": growth_profile,
            },
            {
                "id": "ship.vanguard_interceptor",
                "name": "Vanguard Interceptor",
                "description": "Aggressive starter package with early combat pressure.",
                "role": "high_speed_assault",
                "hull_id": fallback_hull_id,
                "hull_name": str(fallback_hull.get("name", fallback_hull_id)),
                "hull_class": str(fallback_hull.get("class", "scout")),
                "hull_tier": int(fallback_hull.get("tier", 1)),
                "hull_base_stats": fallback_stats,
                "module_slots": fallback_slots,
                "starter_assets": [
                    {
                        "asset_type": "module",
                        "asset_id": "module.weapon_laser_bank_mk1",
                        "quantity": 1,
                    },
                    {"asset_type": "module", "asset_id": "module.engine_vector_mk1", "quantity": 1},
                    {
                        "asset_type": "module",
                        "asset_id": "module.weapon_missile_battery_mk1",
                        "quantity": 1,
                    },
                ],
                "growth_profile": growth_profile,
            },
            {
                "id": "ship.aegis_support_cruiser",
                "name": "Aegis Support Cruiser",
                "description": "Survivability package for defensive and support-focused play.",
                "role": "defensive_support",
                "hull_id": fallback_hull_id,
                "hull_name": str(fallback_hull.get("name", fallback_hull_id)),
                "hull_class": str(fallback_hull.get("class", "scout")),
                "hull_tier": int(fallback_hull.get("tier", 1)),
                "hull_base_stats": fallback_stats,
                "module_slots": fallback_slots,
                "starter_assets": [
                    {
                        "asset_type": "module",
                        "asset_id": "module.armor_titanium_plating_mk1",
                        "quantity": 1,
                    },
                    {
                        "asset_type": "module",
                        "asset_id": "module.shield_basic_barrier_mk1",
                        "quantity": 1,
                    },
                    {
                        "asset_type": "module",
                        "asset_id": "module.reactor_fission_core_mk1",
                        "quantity": 1,
                    },
                ],
                "growth_profile": growth_profile,
            },
        ]

    def _starter_ship_catalog(self) -> list[dict[str, Any]]:
        source_rows = [
            row for row in self.seed_store.starter_ships if isinstance(row, dict)
        ]
        hull_index = self.seed_store.hull_index()
        items: list[dict[str, Any]] = []
        for row in source_rows:
            starter_id = row.get("id")
            hull_id = row.get("hull_id")
            if not isinstance(starter_id, str) or not starter_id.strip():
                continue
            if not isinstance(hull_id, str) or not hull_id.strip():
                continue
            hull = hull_index.get(hull_id.strip())
            if not isinstance(hull, dict):
                continue
            normalized_assets: list[dict[str, Any]] = []
            raw_assets = row.get("starter_assets", [])
            if isinstance(raw_assets, list):
                for asset_row in raw_assets:
                    if not isinstance(asset_row, dict):
                        continue
                    asset_type = asset_row.get("asset_type")
                    asset_id = asset_row.get("asset_id")
                    quantity_raw = asset_row.get("quantity", 1)
                    if (
                        not isinstance(asset_type, str)
                        or asset_type.strip() not in {"module", "hull"}
                        or not isinstance(asset_id, str)
                        or not asset_id.strip()
                    ):
                        continue
                    quantity = (
                        int(quantity_raw)
                        if isinstance(quantity_raw, int) and not isinstance(quantity_raw, bool)
                        else 1
                    )
                    normalized_assets.append(
                        {
                            "asset_type": asset_type.strip(),
                            "asset_id": asset_id.strip(),
                            "quantity": max(1, min(20, quantity)),
                        }
                    )
            growth_profile = row.get("growth_profile")
            if not isinstance(growth_profile, dict):
                growth_profile = {}
            hull_stats = hull.get("base_stats", {})
            if not isinstance(hull_stats, dict):
                hull_stats = {}
            module_slots = hull.get("module_slots", {})
            if not isinstance(module_slots, dict):
                module_slots = {}
            items.append(
                {
                    "id": starter_id.strip(),
                    "name": str(row.get("name", starter_id)).strip() or starter_id.strip(),
                    "description": str(row.get("description", "")).strip(),
                    "role": str(row.get("role", "generalist")).strip() or "generalist",
                    "hull_id": hull_id.strip(),
                    "hull_name": str(hull.get("name", hull_id)).strip() or hull_id.strip(),
                    "hull_class": str(hull.get("class", "unknown")).strip() or "unknown",
                    "hull_tier": int(hull.get("tier", 1)),
                    "hull_base_stats": hull_stats,
                    "module_slots": module_slots,
                    "starter_assets": normalized_assets,
                    "growth_profile": growth_profile,
                }
            )
        if items:
            return items
        return self._default_starter_ship_catalog()

    def _starter_ship_by_id(self, starter_ship_id: str) -> dict[str, Any] | None:
        needle = starter_ship_id.strip()
        if not needle:
            return None
        for row in self._starter_ship_catalog():
            if isinstance(row.get("id"), str) and row["id"] == needle:
                return row
        return None

    def _ensure_starter_ship_assets(self, player_id: str) -> None:
        profile = self.state_store.get_profile(player_id=player_id)
        catalog = self._starter_ship_catalog()
        if not catalog:
            return
        configured_starter_id = profile.get("starting_ship_id")
        selected = None
        if isinstance(configured_starter_id, str):
            selected = self._starter_ship_by_id(configured_starter_id)
        if not isinstance(selected, dict):
            selected = catalog[0]
            self.state_store.upsert_profile(
                {
                    "player_id": profile["player_id"],
                    "display_name": profile.get("display_name", profile.get("captain_name", "Captain")),
                    "captain_name": profile.get("captain_name", profile.get("display_name", "Captain")),
                    "auth_mode": profile.get("auth_mode", "guest"),
                    "email": profile.get("email", ""),
                    "race_id": profile.get("race_id"),
                    "profession_id": profile.get("profession_id"),
                    "starting_ship_id": selected.get("id"),
                    "tutorial_mode": profile.get("tutorial_mode"),
                    "planet_type_id": profile.get("planet_type_id"),
                    "player_memory": profile.get("player_memory", {}),
                }
            )

        required_assets: list[dict[str, Any]] = []
        hull_id = selected.get("hull_id")
        if isinstance(hull_id, str) and hull_id.strip():
            required_assets.append(
                {"asset_type": "hull", "asset_id": hull_id.strip(), "quantity": 1}
            )
        starter_assets = selected.get("starter_assets", [])
        if isinstance(starter_assets, list):
            for asset in starter_assets:
                if not isinstance(asset, dict):
                    continue
                asset_type = asset.get("asset_type")
                asset_id = asset.get("asset_id")
                quantity_raw = asset.get("quantity", 1)
                if (
                    not isinstance(asset_type, str)
                    or asset_type.strip() not in {"module", "hull"}
                    or not isinstance(asset_id, str)
                    or not asset_id.strip()
                ):
                    continue
                quantity = (
                    int(quantity_raw)
                    if isinstance(quantity_raw, int) and not isinstance(quantity_raw, bool)
                    else 1
                )
                required_assets.append(
                    {
                        "asset_type": asset_type.strip(),
                        "asset_id": asset_id.strip(),
                        "quantity": max(1, min(20, quantity)),
                    }
                )

        if not required_assets:
            return
        existing = self.state_store.list_assets(player_id=player_id, limit=400)
        existing_qty: dict[tuple[str, str], int] = {}
        for row in existing:
            asset_type = row.get("asset_type")
            asset_id = row.get("asset_id")
            quantity = row.get("quantity")
            if (
                not isinstance(asset_type, str)
                or not isinstance(asset_id, str)
                or not isinstance(quantity, int)
            ):
                continue
            existing_qty[(asset_type, asset_id)] = max(0, int(quantity))

        for req in required_assets:
            key = (str(req["asset_type"]), str(req["asset_id"]))
            target_qty = int(req["quantity"])
            current_qty = existing_qty.get(key, 0)
            if current_qty >= target_qty:
                continue
            self.state_store.add_asset(
                player_id=player_id,
                asset_type=key[0],
                asset_id=key[1],
                quantity=target_qty - current_qty,
            )

    def _ensure_player_bootstrap(self, player_id: str, skip_auth: bool = False) -> None:
        if not skip_auth:
            self._require_authenticated_player(expected_player_id=player_id)
        if player_id.strip() != "admin":
            moderation = self.state_store.get_player_moderation(player_id=player_id.strip())
            if isinstance(moderation, dict) and bool(moderation.get("is_active")):
                reason = str(moderation.get("reason", "Moderation restriction in effect")).strip()
                status = str(moderation.get("status", "restricted")).strip()
                expires_utc = moderation.get("expires_utc")
                expires_suffix = (
                    f", expires {expires_utc}"
                    if isinstance(expires_utc, str) and expires_utc.strip()
                    else ""
                )
                raise ValueError(
                    f"Player '{player_id.strip()}' is {status} ({reason}{expires_suffix})"
                )
        starter_tech_ids = [
            node["id"]
            for node in self.seed_store.tech_tree
            if isinstance(node, dict)
            and isinstance(node.get("id"), str)
            and isinstance(node.get("prerequisites"), list)
            and len(node["prerequisites"]) == 0
        ]
        self.state_store.bootstrap_player(
            player_id=player_id,
            starter_inventory=STARTER_INVENTORY,
            starter_tech_ids=starter_tech_ids,
        )
        self._ensure_starter_ship_assets(player_id=player_id)
        self._ensure_homeworld(player_id=player_id)
        try:
            self._apply_life_support_runtime(player_id=player_id, force=False)
        except Exception:
            logging.exception(
                "Life-support runtime tick failed during bootstrap for player_id=%s",
                player_id,
            )

    def _ensure_homeworld(self, player_id: str) -> None:
        existing_worlds = self.state_store.list_worlds_for_player(player_id=player_id)
        if existing_worlds:
            return
        profile = self.state_store.get_profile(player_id=player_id)
        captain_name = str(profile.get("captain_name", "Captain")).strip() or "Captain"
        planet_type_id = profile.get("planet_type_id")
        planet_type = None
        if isinstance(planet_type_id, str):
            for row in self.seed_store.planet_types:
                if isinstance(row, dict) and row.get("id") == planet_type_id:
                    planet_type = row
                    break
        subtype = "temperate"
        habitability = 0.82
        if isinstance(planet_type, dict):
            subtype = str(planet_type.get("name", subtype))
            raw_habitability = planet_type.get("habitability")
            if isinstance(raw_habitability, (int, float)) and not isinstance(raw_habitability, bool):
                habitability = max(0.72, min(1.0, float(raw_habitability)))
        world_id = f"world.home.{uuid.uuid5(uuid.NAMESPACE_DNS, player_id.strip()).hex[:12]}"
        homeworld = {
            "world_id": world_id,
            "name": f"{captain_name} Prime",
            "template_id": "template.homeworld.habitable",
            "body_class": "planet",
            "subtype": subtype,
            "scan_difficulty": 0.45,
            "richness_multiplier": 1.05,
            "habitability_score": round(habitability, 3),
            "rarity_score": 0.38,
            "population_potential_millions": 960,
            "population_capacity": 960000,
            "population_current": 42000,
            "population_growth_per_day_pct": 2.1,
            "environment_hazard": 0.12,
            "hidden_signature": 0.72,
            "recommended_structures": [
                "structure.orbital_mine_array",
                "structure.quantum_research_hub",
                "structure.defense_grid",
                "structure.trade_relay_port",
            ],
            "element_lodes": [
                {"symbol": "Fe", "name": "Iron", "ratio_pct": 24.0, "estimated_units": 36000, "atomic_number": 26},
                {"symbol": "Si", "name": "Silicon", "ratio_pct": 20.0, "estimated_units": 32000, "atomic_number": 14},
                {"symbol": "Al", "name": "Aluminum", "ratio_pct": 12.0, "estimated_units": 21000, "atomic_number": 13},
                {"symbol": "C", "name": "Carbon", "ratio_pct": 11.0, "estimated_units": 19000, "atomic_number": 6},
                {"symbol": "O", "name": "Oxygen", "ratio_pct": 10.0, "estimated_units": 17000, "atomic_number": 8},
                {"symbol": "Cu", "name": "Copper", "ratio_pct": 6.0, "estimated_units": 9800, "atomic_number": 29},
                {"symbol": "Ni", "name": "Nickel", "ratio_pct": 4.5, "estimated_units": 7200, "atomic_number": 28},
                {"symbol": "Ti", "name": "Titanium", "ratio_pct": 3.8, "estimated_units": 6100, "atomic_number": 22},
            ],
            "traits": ["homeworld", "habitable", "colony_core"],
        }
        self.state_store.claim_world(player_id=player_id, world=homeworld)
        self.state_store.catalog_discovered_worlds(player_id=player_id, worlds=[homeworld])

    def _ensure_fleet_initialized(self, player_id: str) -> dict[str, Any]:
        hull_assets = self.state_store.list_assets(player_id=player_id, asset_type="hull", limit=120)
        hull_index = self.seed_store.hull_index()
        active_hull_id = "hull.settler_scout"
        best_tier = -1
        crew_min = 42.0
        for row in hull_assets:
            hull_id = row.get("asset_id")
            if not isinstance(hull_id, str):
                continue
            hull = hull_index.get(hull_id)
            if not isinstance(hull, dict):
                continue
            tier = int(hull.get("tier", 1))
            if tier > best_tier:
                best_tier = tier
                active_hull_id = hull_id
                base_stats = hull.get("base_stats", {})
                if isinstance(base_stats, dict):
                    raw_crew = base_stats.get("crew_min", 42)
                    if isinstance(raw_crew, (int, float)) and not isinstance(raw_crew, bool):
                        crew_min = max(8.0, float(raw_crew))
        try:
            self.state_store.ensure_fleet_state(
                player_id=player_id,
                active_hull_id=active_hull_id,
                crew_total=crew_min,
            )
        except StateStoreError:
            # If fleet row exists already we still want to return the latest value.
            pass
        fleet = self.state_store.get_fleet_state(player_id=player_id)
        cargo = fleet.get("cargo", {})
        if isinstance(cargo, dict) and len(cargo) == 0:
            inventory = self.state_store.list_inventory(player_id=player_id, limit=6)
            seeded_cargo: dict[str, float] = {}
            for row in inventory:
                symbol = row.get("symbol")
                amount = row.get("amount")
                if not isinstance(symbol, str):
                    continue
                if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                    continue
                if float(amount) <= 0:
                    continue
                seeded = min(220.0, max(6.0, float(amount) * 0.1))
                seeded_cargo[symbol] = round(seeded, 3)
            if seeded_cargo:
                fleet = self.state_store.update_fleet_state(
                    player_id=player_id,
                    cargo=seeded_cargo,
                )
        active_hull = hull_index.get(str(fleet.get("active_hull_id", "")), {})
        base_stats = active_hull.get("base_stats", {}) if isinstance(active_hull, dict) else {}
        if not isinstance(base_stats, dict):
            base_stats = {}
        deck_limit = float(base_stats.get("deck", 1))
        crew_total_raw = fleet.get("crew_total", 0.0)
        crew_total = (
            float(crew_total_raw)
            if isinstance(crew_total_raw, (int, float)) and not isinstance(crew_total_raw, bool)
            else 0.0
        )
        crew_elite_raw = fleet.get("crew_elite", 0.0)
        crew_elite = (
            float(crew_elite_raw)
            if isinstance(crew_elite_raw, (int, float)) and not isinstance(crew_elite_raw, bool)
            else 0.0
        )
        cargo_load_tons = 0.0
        cargo_payload = fleet.get("cargo", {})
        if isinstance(cargo_payload, dict):
            for amount in cargo_payload.values():
                if isinstance(amount, (int, float)) and not isinstance(amount, bool):
                    cargo_load_tons += max(0.0, float(amount))
        cargo_load_tons *= 0.02
        support_metrics = {
            "crew_capacity": float(base_stats.get("crew_capacity", max(1.0, crew_total))),
            "passenger_capacity": float(base_stats.get("passenger_capacity", 0.0)),
            "cargo_capacity_tons": float(base_stats.get("cargo_capacity_tons", 0.0)),
        }
        fleet["ship_space"] = self._compute_ship_space_model(
            base_stats=base_stats,
            support_metrics=support_metrics,
            normalized_modules=[],
            module_index=self.seed_store.module_index(),
            deck_limit=deck_limit,
            deck_used=0.0,
            crew_assigned_total=crew_total,
            crew_assigned_elite=crew_elite,
            passenger_assigned_total=0.0,
            cargo_load_tons=cargo_load_tons,
        )
        return fleet

    def _asset_stack_size(self, asset_type: str) -> int:
        key = str(asset_type or "").strip().casefold()
        return int(ASSET_STACK_SIZE_BY_TYPE.get(key, 8))

    def _asset_slot_usage(self, rows: list[dict[str, Any]]) -> float:
        total = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset_type = row.get("asset_type")
            quantity = row.get("quantity")
            if not isinstance(asset_type, str):
                continue
            if isinstance(quantity, bool) or not isinstance(quantity, (int, float)):
                continue
            qty = max(0.0, float(quantity))
            if qty <= 0:
                continue
            stack_size = max(1, self._asset_stack_size(asset_type))
            total += math.ceil(qty / float(stack_size))
        return float(total)

    def _compute_storage_profile(self, player_id: str, skip_auth: bool = False) -> dict[str, Any]:
        self._ensure_player_bootstrap(player_id, skip_auth=skip_auth)
        fleet = self._ensure_fleet_initialized(player_id=player_id)
        upgrades = self.state_store.get_storage_upgrades(player_id=player_id)
        personal_assets = self.state_store.list_assets(player_id=player_id, limit=1200)
        smuggled_assets = self.state_store.list_smuggled_assets(player_id=player_id, limit=1200)
        crafted_instances_count = self.state_store.count_crafted_instances(player_id=player_id)

        ship_level_raw = fleet.get("ship_level", 1)
        ship_level = int(ship_level_raw) if isinstance(ship_level_raw, int) else 1
        ship_level = max(1, ship_level)

        ship_space = fleet.get("ship_space", {})
        cargo_capacity_tons = 0.0
        if isinstance(ship_space, dict):
            raw = ship_space.get("cargo_capacity_tons", 0.0)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                cargo_capacity_tons = max(0.0, float(raw))

        base_personal = (
            BASE_PERSONAL_STORAGE_SLOTS
            + (ship_level * 2.0)
            + (cargo_capacity_tons * 0.35)
        )
        base_smuggle = (
            BASE_SMUGGLE_STORAGE_SLOTS
            + (ship_level * 0.35)
            + (cargo_capacity_tons * 0.06)
        )
        personal_capacity = max(
            8.0, base_personal + float(upgrades.get("personal_slots_bonus", 0.0))
        )
        smuggle_capacity = max(
            0.0, base_smuggle + float(upgrades.get("smuggle_slots_bonus", 0.0))
        )

        personal_asset_slots = self._asset_slot_usage(personal_assets)
        smuggle_asset_slots = self._asset_slot_usage(smuggled_assets)
        personal_used = personal_asset_slots + float(crafted_instances_count)
        smuggle_used = smuggle_asset_slots

        return {
            "player_id": player_id.strip(),
            "ship_level": ship_level,
            "cargo_capacity_tons": round(cargo_capacity_tons, 3),
            "upgrades": upgrades,
            "personal": {
                "capacity_slots": round(personal_capacity, 3),
                "used_slots": round(personal_used, 3),
                "free_slots": round(max(0.0, personal_capacity - personal_used), 3),
                "utilization_ratio": round(personal_used / max(0.001, personal_capacity), 4),
                "asset_stack_slots": round(personal_asset_slots, 3),
                "crafted_instance_slots": int(crafted_instances_count),
            },
            "smuggle": {
                "capacity_slots": round(smuggle_capacity, 3),
                "used_slots": round(smuggle_used, 3),
                "free_slots": round(max(0.0, smuggle_capacity - smuggle_used), 3),
                "utilization_ratio": round(smuggle_used / max(0.001, smuggle_capacity), 4),
                "detection_risk_base": round(max(0.02, 0.24 - (ship_level * 0.0014)), 4),
            },
            "assets": {
                "personal_stacks": len(personal_assets),
                "smuggled_stacks": len(smuggled_assets),
            },
        }

    def _estimate_personal_slot_delta_for_asset_add(
        self,
        player_id: str,
        asset_type: str,
        asset_id: str,
        quantity: int,
    ) -> float:
        if quantity <= 0:
            return 0.0
        rows = self.state_store.list_assets(
            player_id=player_id,
            asset_type=asset_type,
            limit=1200,
        )
        current = 0
        for row in rows:
            if (
                isinstance(row, dict)
                and row.get("asset_id") == asset_id
                and isinstance(row.get("quantity"), int)
            ):
                current = max(0, int(row["quantity"]))
                break
        stack_size = max(1, self._asset_stack_size(asset_type))
        before_slots = math.ceil(current / float(stack_size)) if current > 0 else 0
        after_slots = math.ceil((current + quantity) / float(stack_size))
        return float(max(0, after_slots - before_slots))

    def _ensure_storage_capacity_for_reward(
        self,
        *,
        player_id: str,
        additional_personal_asset_slots: float = 0.0,
        additional_instance_slots: int = 0,
        additional_smuggle_asset_slots: float = 0.0,
        skip_auth: bool = False,
    ) -> dict[str, Any]:
        profile = self._compute_storage_profile(player_id=player_id, skip_auth=skip_auth)
        personal = profile["personal"]
        smuggle = profile["smuggle"]
        personal_need = float(personal["used_slots"]) + max(0.0, float(additional_personal_asset_slots)) + max(0, int(additional_instance_slots))
        smuggle_need = float(smuggle["used_slots"]) + max(0.0, float(additional_smuggle_asset_slots))
        if personal_need > float(personal["capacity_slots"]) + 1e-9:
            overflow = personal_need - float(personal["capacity_slots"])
            raise ValueError(
                "Personal storage is full (need +{:.2f} slots). Trash items or upgrade storage.".format(
                    overflow
                )
            )
        if smuggle_need > float(smuggle["capacity_slots"]) + 1e-9:
            overflow = smuggle_need - float(smuggle["capacity_slots"])
            raise ValueError(
                "Smuggle storage is full (need +{:.2f} slots). Move items out or upgrade storage.".format(
                    overflow
                )
            )
        return profile

    def _storage_upgrade_cost(
        self,
        player_id: str,
        *,
        category: str,
        levels: int,
    ) -> dict[str, Any]:
        storage = self._compute_storage_profile(player_id=player_id)
        upgrades = storage["upgrades"]
        if category == "personal":
            current_bonus = float(upgrades.get("personal_slots_bonus", 0.0))
            element_symbol = "Ti"
            per_level_slots = 8.0
        elif category == "smuggle":
            current_bonus = float(upgrades.get("smuggle_slots_bonus", 0.0))
            element_symbol = "Pd"
            per_level_slots = 3.0
        else:
            raise ValueError("category must be 'personal' or 'smuggle'")
        start_level = int(max(0.0, current_bonus / per_level_slots))
        total_credits = 0.0
        total_elements = 0.0
        for idx in range(levels):
            stage = start_level + idx + 1
            total_credits += 1200.0 * (1.22 ** stage)
            total_elements += 10.0 + (stage * 3.4)
        return {
            "category": category,
            "levels": levels,
            "slots_delta": round(levels * per_level_slots, 3),
            "credits": round(total_credits, 3),
            "elements": [{"symbol": element_symbol, "amount": round(total_elements, 3)}],
        }

    def _upgrade_storage(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        category = payload.get("category")
        levels = payload.get("levels", 1)
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(category, str) or not category.strip():
            raise ValueError("category must be a non-empty string")
        if isinstance(levels, bool) or not isinstance(levels, int):
            raise ValueError("levels must be an integer")
        if levels <= 0 or levels > 30:
            raise ValueError("levels must be between 1 and 30")
        self._ensure_player_bootstrap(player_id.strip())
        cost = self._storage_upgrade_cost(
            player_id=player_id.strip(),
            category=category.strip().casefold(),
            levels=levels,
        )
        element_deltas = {
            str(row["symbol"]): -float(row["amount"])
            for row in cost["elements"]
            if isinstance(row, dict) and isinstance(row.get("symbol"), str)
        }
        resources = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=-float(cost["credits"]),
            voidcoin_delta=0.0,
            element_deltas=element_deltas,
        )
        slots_delta = float(cost["slots_delta"])
        if cost["category"] == "personal":
            upgrades = self.state_store.add_storage_upgrade(
                player_id=player_id.strip(),
                personal_slots_delta=slots_delta,
                smuggle_slots_delta=0.0,
            )
        else:
            upgrades = self.state_store.add_storage_upgrade(
                player_id=player_id.strip(),
                personal_slots_delta=0.0,
                smuggle_slots_delta=slots_delta,
            )
        storage = self._compute_storage_profile(player_id=player_id.strip())
        return {
            "player_id": player_id.strip(),
            "category": cost["category"],
            "levels": int(levels),
            "slots_delta": round(slots_delta, 3),
            "cost": cost,
            "upgrades": upgrades,
            "storage": storage,
            "wallet": resources["wallet"],
            "inventory_changes": resources["inventory"],
        }

    def _move_smuggled_assets(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        direction = payload.get("direction")
        asset_type = payload.get("asset_type")
        asset_id = payload.get("asset_id")
        quantity = payload.get("quantity", 1)
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(direction, str) or not direction.strip():
            raise ValueError("direction must be a non-empty string")
        if not isinstance(asset_type, str) or not asset_type.strip():
            raise ValueError("asset_type must be a non-empty string")
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise ValueError("asset_id must be a non-empty string")
        if isinstance(quantity, bool) or not isinstance(quantity, int):
            raise ValueError("quantity must be an integer")
        if quantity <= 0:
            raise ValueError("quantity must be > 0")

        player_id = player_id.strip()
        direction_value = direction.strip().casefold()
        asset_type_value = asset_type.strip()
        asset_id_value = asset_id.strip()
        self._ensure_player_bootstrap(player_id)

        if direction_value == "to_smuggle":
            rows = self.state_store.list_assets(
                player_id=player_id,
                asset_type=asset_type_value,
                limit=1200,
            )
            available = 0
            for row in rows:
                if isinstance(row, dict) and row.get("asset_id") == asset_id_value:
                    qty_raw = row.get("quantity")
                    if isinstance(qty_raw, int):
                        available = max(0, qty_raw)
                    break
            if available < quantity:
                raise ValueError("Not enough quantity in personal inventory")
            delta_slots = self._estimate_personal_slot_delta_for_asset_add(
                player_id=player_id,
                asset_type=asset_type_value,
                asset_id=asset_id_value,
                quantity=0,
            )
            _ = delta_slots  # keep explicit for readability in this pathway
            smuggled_rows = self.state_store.list_smuggled_assets(
                player_id=player_id,
                asset_type=asset_type_value,
                limit=1200,
            )
            current_smuggled = 0
            for row in smuggled_rows:
                if isinstance(row, dict) and row.get("asset_id") == asset_id_value:
                    qty_raw = row.get("quantity")
                    if isinstance(qty_raw, int):
                        current_smuggled = max(0, qty_raw)
                    break
            stack_size = max(1, self._asset_stack_size(asset_type_value))
            before_slots = math.ceil(current_smuggled / float(stack_size)) if current_smuggled > 0 else 0
            after_slots = math.ceil((current_smuggled + quantity) / float(stack_size))
            self._ensure_storage_capacity_for_reward(
                player_id=player_id,
                additional_personal_asset_slots=0.0,
                additional_instance_slots=0,
                additional_smuggle_asset_slots=float(max(0, after_slots - before_slots)),
            )
            self.state_store.adjust_asset_quantity(
                player_id=player_id,
                asset_type=asset_type_value,
                asset_id=asset_id_value,
                quantity_delta=-quantity,
            )
            self.state_store.adjust_smuggled_asset_quantity(
                player_id=player_id,
                asset_type=asset_type_value,
                asset_id=asset_id_value,
                quantity_delta=quantity,
            )
        elif direction_value == "to_personal":
            rows = self.state_store.list_smuggled_assets(
                player_id=player_id,
                asset_type=asset_type_value,
                limit=1200,
            )
            available = 0
            current_smuggled = 0
            for row in rows:
                if isinstance(row, dict) and row.get("asset_id") == asset_id_value:
                    qty_raw = row.get("quantity")
                    if isinstance(qty_raw, int):
                        available = max(0, qty_raw)
                        current_smuggled = available
                    break
            if available < quantity:
                raise ValueError("Not enough quantity in smuggle inventory")
            personal_delta = self._estimate_personal_slot_delta_for_asset_add(
                player_id=player_id,
                asset_type=asset_type_value,
                asset_id=asset_id_value,
                quantity=quantity,
            )
            stack_size = max(1, self._asset_stack_size(asset_type_value))
            before_smuggle_slots = math.ceil(current_smuggled / float(stack_size)) if current_smuggled > 0 else 0
            after_smuggle_slots = math.ceil((current_smuggled - quantity) / float(stack_size)) if (current_smuggled - quantity) > 0 else 0
            smuggle_delta = float(max(0, after_smuggle_slots - before_smuggle_slots))
            self._ensure_storage_capacity_for_reward(
                player_id=player_id,
                additional_personal_asset_slots=personal_delta,
                additional_instance_slots=0,
                additional_smuggle_asset_slots=smuggle_delta,
            )
            self.state_store.adjust_smuggled_asset_quantity(
                player_id=player_id,
                asset_type=asset_type_value,
                asset_id=asset_id_value,
                quantity_delta=-quantity,
            )
            self.state_store.adjust_asset_quantity(
                player_id=player_id,
                asset_type=asset_type_value,
                asset_id=asset_id_value,
                quantity_delta=quantity,
            )
        else:
            raise ValueError("direction must be 'to_smuggle' or 'to_personal'")

        return {
            "player_id": player_id,
            "direction": direction_value,
            "asset_type": asset_type_value,
            "asset_id": asset_id_value,
            "quantity": int(quantity),
            "storage": self._compute_storage_profile(player_id=player_id),
            "personal_assets": self.state_store.list_assets(player_id=player_id, limit=200),
            "smuggled_assets": self.state_store.list_smuggled_assets(player_id=player_id, limit=200),
        }

    def _trash_warning_payload(
        self,
        *,
        target_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        target = target_type.strip().casefold()
        if target == "crafted_instance":
            tier = payload.get("quality_tier")
            if isinstance(tier, str) and tier.strip().casefold() in WARNING_INSTANCE_QUALITY_TIERS:
                return {
                    "level": "warning",
                    "message": f"Instance quality '{tier}' is rare/high-value. Confirm trash to continue.",
                    "code": "warning.high_quality_instance",
                }
            return None
        if target == "asset_stack":
            tier_raw = payload.get("tier")
            if isinstance(tier_raw, int) and tier_raw >= 6:
                return {
                    "level": "warning",
                    "message": f"Tier {tier_raw} asset is rare/high-value. Confirm trash to continue.",
                    "code": "warning.high_tier_asset",
                }
            return None
        if target == "element":
            symbol = payload.get("symbol")
            amount = payload.get("amount")
            if isinstance(symbol, str) and symbol.strip() in WARNING_ELEMENT_SYMBOLS:
                return {
                    "level": "warning",
                    "message": f"Element {symbol.strip()} is rare/high-value. Confirm trash to continue.",
                    "code": "warning.rare_element",
                }
            if isinstance(amount, (int, float)) and not isinstance(amount, bool) and float(amount) >= 5000.0:
                return {
                    "level": "warning",
                    "message": "Large element disposal detected. Confirm trash to continue.",
                    "code": "warning.large_element_disposal",
                }
            return None
        return None

    def _trash_inventory_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        target_type = payload.get("target_type")
        confirm = payload.get("confirm", False)
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(target_type, str) or not target_type.strip():
            raise ValueError("target_type must be a non-empty string")
        if not isinstance(confirm, bool):
            raise ValueError("confirm must be boolean when provided")
        player_id = player_id.strip()
        target_value = target_type.strip().casefold()
        self._ensure_player_bootstrap(player_id)

        if target_value == "crafted_instance":
            instance_id = payload.get("instance_id")
            if not isinstance(instance_id, str) or not instance_id.strip():
                raise ValueError("instance_id must be a non-empty string")
            instance = self.state_store.get_crafted_instance(
                player_id=player_id,
                instance_id=instance_id.strip(),
            )
            warning = self._trash_warning_payload(target_type=target_value, payload=instance)
            if isinstance(warning, dict) and not confirm:
                return {
                    "player_id": player_id,
                    "target_type": target_value,
                    "confirm_required": True,
                    "warning": warning,
                    "preview": instance,
                }
            result = self.state_store.delete_crafted_instance(
                player_id=player_id,
                instance_id=instance_id.strip(),
            )
            return {
                "player_id": player_id,
                "target_type": target_value,
                "confirm_required": False,
                "result": result,
                "storage": self._compute_storage_profile(player_id=player_id),
            }

        if target_value == "asset_stack":
            asset_type = payload.get("asset_type")
            asset_id = payload.get("asset_id")
            quantity = payload.get("quantity", 1)
            if not isinstance(asset_type, str) or not asset_type.strip():
                raise ValueError("asset_type must be a non-empty string")
            if not isinstance(asset_id, str) or not asset_id.strip():
                raise ValueError("asset_id must be a non-empty string")
            if isinstance(quantity, bool) or not isinstance(quantity, int):
                raise ValueError("quantity must be an integer")
            if quantity <= 0:
                raise ValueError("quantity must be > 0")
            item = None
            try:
                _, item = self._catalog_lookup_item(item_id=asset_id.strip())
            except ValueError:
                item = None
            warning = self._trash_warning_payload(
                target_type=target_value,
                payload=item if isinstance(item, dict) else {},
            )
            if isinstance(warning, dict) and not confirm:
                return {
                    "player_id": player_id,
                    "target_type": target_value,
                    "confirm_required": True,
                    "warning": warning,
                    "preview": {
                        "asset_type": asset_type.strip(),
                        "asset_id": asset_id.strip(),
                        "quantity": int(quantity),
                    },
                }
            updated = self.state_store.adjust_asset_quantity(
                player_id=player_id,
                asset_type=asset_type.strip(),
                asset_id=asset_id.strip(),
                quantity_delta=-int(quantity),
            )
            return {
                "player_id": player_id,
                "target_type": target_value,
                "confirm_required": False,
                "result": updated,
                "storage": self._compute_storage_profile(player_id=player_id),
            }

        if target_value == "element":
            symbol = payload.get("symbol")
            amount = payload.get("amount")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError("symbol must be a non-empty string")
            if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                raise ValueError("amount must be numeric")
            drop = max(0.0, float(amount))
            if drop <= 0:
                raise ValueError("amount must be > 0")
            warning = self._trash_warning_payload(
                target_type=target_value,
                payload={"symbol": symbol.strip(), "amount": drop},
            )
            if isinstance(warning, dict) and not confirm:
                return {
                    "player_id": player_id,
                    "target_type": target_value,
                    "confirm_required": True,
                    "warning": warning,
                    "preview": {"symbol": symbol.strip(), "amount": round(drop, 3)},
                }
            result = self.state_store.adjust_inventory(
                player_id=player_id,
                symbol_deltas={symbol.strip(): -drop},
            )
            return {
                "player_id": player_id,
                "target_type": target_value,
                "confirm_required": False,
                "result": {
                    "symbol": symbol.strip(),
                    "remaining_amount": round(float(result.get(symbol.strip(), 0.0)), 3),
                },
                "storage": self._compute_storage_profile(player_id=player_id),
            }

        raise ValueError("target_type must be one of: crafted_instance, asset_stack, element")

    def _require_admin(self, admin_player_id: str) -> None:
        if not isinstance(admin_player_id, str) or not admin_player_id.strip():
            raise ValueError("admin_player_id must be a non-empty string")
        self._ensure_player_bootstrap(admin_player_id.strip())
        if not self._has_admin_privileges(admin_player_id.strip()):
            raise ValueError("Admin privileges required")

    def _admin_list_players(self, query: dict[str, list[str]]) -> dict[str, Any]:
        self._reject_unknown_query_keys(
            query,
            allowed={"player_id", "limit", "search", "include_admin", "status"},
        )
        admin_player_id = self._parse_required_query_string(query, "player_id")
        self._require_admin(admin_player_id=admin_player_id)
        limit = self._parse_market_limit(query)
        search = self._parse_optional_string(query, "search")
        include_admin = self._parse_bool_query(query, "include_admin", default=False)
        status = self._parse_optional_string(query, "status")
        players = self.state_store.list_player_profiles_admin(
            limit=limit,
            search=search,
            include_admin=include_admin,
            status=status or "all",
        )
        return {
            "admin_player_id": admin_player_id,
            "total": len(players),
            "items": players,
        }

    def _admin_kick_player(self, payload: dict[str, Any]) -> dict[str, Any]:
        admin_player_id = payload.get("admin_player_id")
        target_player_id = payload.get("target_player_id")
        reason = payload.get("reason")
        duration_hours = payload.get("duration_hours", 72.0)
        action = payload.get("action", "kick")
        if not isinstance(admin_player_id, str) or not admin_player_id.strip():
            raise ValueError("admin_player_id must be a non-empty string")
        if not isinstance(target_player_id, str) or not target_player_id.strip():
            raise ValueError("target_player_id must be a non-empty string")
        if not isinstance(reason, str) or len(reason.strip()) < 4:
            raise ValueError("reason must be at least 4 characters")
        if isinstance(duration_hours, bool) or not isinstance(duration_hours, (int, float)):
            raise ValueError("duration_hours must be numeric")
        if not isinstance(action, str) or not action.strip():
            raise ValueError("action must be a non-empty string")

        admin_id = admin_player_id.strip()
        target_id = target_player_id.strip()
        action_value = action.strip().casefold()
        if target_id == "admin":
            raise ValueError("Cannot moderate reserved admin player")
        self._require_admin(admin_player_id=admin_id)

        if action_value in {"kick", "suspend", "ban"}:
            status_value = "kicked" if action_value == "kick" else ("suspended" if action_value == "suspend" else "banned")
            moderation = self.state_store.set_player_moderation(
                player_id=target_id,
                status=status_value,
                reason=reason.strip(),
                imposed_by_player_id=admin_id,
                duration_hours=float(duration_hours),
            )
            revoked_sessions = self._revoke_player_sessions(target_id)
            action_log = self.state_store.log_admin_action(
                admin_player_id=admin_id,
                action_type=f"player_{action_value}",
                target_player_id=target_id,
                payload={
                    "reason": reason.strip(),
                    "duration_hours": float(duration_hours),
                    "revoked_sessions": revoked_sessions,
                    "moderation": moderation,
                },
            )
            return {
                "admin_player_id": admin_id,
                "target_player_id": target_id,
                "action": action_value,
                "moderation": moderation,
                "revoked_sessions": revoked_sessions,
                "action_log": action_log,
            }
        if action_value in {"unban", "clear"}:
            cleared = self.state_store.clear_player_moderation(player_id=target_id)
            action_log = self.state_store.log_admin_action(
                admin_player_id=admin_id,
                action_type="player_clear_moderation",
                target_player_id=target_id,
                payload={"reason": reason.strip(), "cleared": cleared},
            )
            return {
                "admin_player_id": admin_id,
                "target_player_id": target_id,
                "action": "clear",
                "cleared": cleared,
                "action_log": action_log,
            }
        raise ValueError("action must be one of: kick, suspend, ban, unban, clear")

    def _admin_force_jackpot_craft(self, payload: dict[str, Any]) -> dict[str, Any]:
        admin_player_id = payload.get("admin_player_id")
        target_player_id = payload.get("target_player_id")
        item_id = payload.get("item_id")
        quantity = payload.get("quantity", 1)
        jackpot_tier = payload.get("jackpot_tier", "mythic")
        if not isinstance(admin_player_id, str) or not admin_player_id.strip():
            raise ValueError("admin_player_id must be a non-empty string")
        if not isinstance(target_player_id, str) or not target_player_id.strip():
            raise ValueError("target_player_id must be a non-empty string")
        if not isinstance(item_id, str) or not item_id.strip():
            raise ValueError("item_id must be a non-empty string")
        if isinstance(quantity, bool) or not isinstance(quantity, int):
            raise ValueError("quantity must be an integer")
        if quantity <= 0 or quantity > 64:
            raise ValueError("quantity must be between 1 and 64")
        if not isinstance(jackpot_tier, str) or not jackpot_tier.strip():
            raise ValueError("jackpot_tier must be a non-empty string")
        admin_id = admin_player_id.strip()
        target_id = target_player_id.strip()
        self._require_admin(admin_player_id=admin_id)
        self._ensure_player_bootstrap(target_id, skip_auth=True)

        item_kind, item = self._catalog_lookup_item(item_id=item_id.strip())
        if item_kind not in {"module", "hull"}:
            raise ValueError("admin jackpot currently supports module.* or hull.* items")
        tier_value = jackpot_tier.strip().casefold()
        if tier_value in {"mythic", "prototype"}:
            quality_tier = "prototype"
            quality_score = 1.34
        elif tier_value in {"legendary", "elite"}:
            quality_tier = "elite"
            quality_score = 1.22
        elif tier_value in {"rare", "refined"}:
            quality_tier = "refined"
            quality_score = 1.1
        else:
            raise ValueError("jackpot_tier must be one of: rare, legendary, mythic")

        added_stack_slots = self._estimate_personal_slot_delta_for_asset_add(
            player_id=target_id,
            asset_type=item_kind,
            asset_id=item_id.strip(),
            quantity=quantity,
        )
        added_instances = min(quantity, 24)
        self._ensure_storage_capacity_for_reward(
            player_id=target_id,
            additional_personal_asset_slots=added_stack_slots,
            additional_instance_slots=added_instances,
            additional_smuggle_asset_slots=0.0,
            skip_auth=True,
        )

        self.state_store.add_asset(
            player_id=target_id,
            asset_type=item_kind,
            asset_id=item_id.strip(),
            quantity=quantity,
        )
        template = self._roll_quality_profile(
            item_kind=item_kind,
            item=item,
            rng=random.Random(stable_hash_int(admin_id, target_id, item_id.strip(), "admin_jackpot_seed")),
            player_id=target_id,
        )
        template["quality_tier"] = quality_tier
        template["quality_score"] = round(quality_score, 4)
        template["stat_multiplier"] = round(quality_score * 1.08, 4)
        template["jackpot_triggered"] = True
        template["jackpot_chance"] = 1.0
        template["admin_forced"] = True
        template["admin_forced_tier"] = tier_value

        instances: list[dict[str, Any]] = []
        for idx in range(min(quantity, 24)):
            payload_instance = dict(template)
            payload_instance["admin_forced_index"] = idx + 1
            stored = self.state_store.add_crafted_instance(
                player_id=target_id,
                asset_type=item_kind,
                asset_id=item_id.strip(),
                quality_payload=payload_instance,
            )
            instances.append(stored)
        action_log = self.state_store.log_admin_action(
            admin_player_id=admin_id,
            action_type="force_jackpot_craft",
            target_player_id=target_id,
            payload={
                "item_id": item_id.strip(),
                "item_kind": item_kind,
                "quantity": quantity,
                "jackpot_tier": tier_value,
            },
        )
        return {
            "admin_player_id": admin_id,
            "target_player_id": target_id,
            "item_id": item_id.strip(),
            "item_kind": item_kind,
            "quantity": quantity,
            "jackpot_tier": tier_value,
            "quality_tier": quality_tier,
            "quality_score": round(quality_score, 4),
            "instances_created": len(instances),
            "instances": instances,
            "assets": self.state_store.list_assets(player_id=target_id, asset_type=item_kind, limit=40),
            "storage": self._compute_storage_profile(player_id=target_id, skip_auth=True),
            "action_log": action_log,
        }

    def _covert_policy_payload(self, player_id: str | None = None) -> dict[str, Any]:
        player_cooldowns: list[dict[str, Any]] = []
        if isinstance(player_id, str) and player_id.strip():
            player_cooldowns = self.state_store.list_covert_cooldowns(player_id=player_id.strip())
        return {
            "version": "1.0",
            "ops": {
                "steal": {
                    "energy_cost": ENERGY_COST_COVERT_STEAL,
                    "base_cooldown_seconds": COVERT_STEAL_COOLDOWN_SECONDS,
                    "description": "Attempt to steal stack assets or element cargo from target inventory.",
                },
                "sabotage": {
                    "energy_cost": ENERGY_COST_COVERT_SABOTAGE,
                    "base_cooldown_seconds": COVERT_SABOTAGE_COOLDOWN_SECONDS,
                    "description": "Apply covert damage to fleet durability with optional module disruption.",
                },
                "hack": {
                    "energy_cost": ENERGY_COST_COVERT_HACK,
                    "base_cooldown_seconds": COVERT_HACK_COOLDOWN_SECONDS,
                    "description": "Execute data breach for intel extraction, energy disruption, and bounty credits.",
                },
            },
            "fairplay": {
                "high_level_vs_low_level_guardrail": "Attacks against much lower-level targets without high-risk visibility enabled are blocked from meaningful rewards/effects.",
                "visibility_opt_in_endpoint": "POST /api/profile/pvp-visibility",
                "non_pay_to_win": True,
            },
            "player_id": player_id.strip() if isinstance(player_id, str) and player_id.strip() else None,
            "player_cooldowns": player_cooldowns,
        }

    @staticmethod
    def _clamp(value: float, floor: float, ceil: float) -> float:
        return max(floor, min(ceil, value))

    @staticmethod
    def _sigmoid(value: float, scale: float = 1.0) -> float:
        denominator = max(1e-6, float(scale))
        return 1.0 / (1.0 + math.exp(-(float(value) / denominator)))

    def _covert_transfer_steal(
        self,
        *,
        actor_player_id: str,
        target_player_id: str,
        desired_quantity: int,
        reward_scale: float,
        rng: random.Random,
    ) -> dict[str, Any]:
        desired_qty = max(1, min(5, int(desired_quantity)))
        target_assets = self.state_store.list_assets(
            player_id=target_player_id,
            limit=500,
        )
        asset_candidates: list[dict[str, Any]] = []
        for row in target_assets:
            if not isinstance(row, dict):
                continue
            asset_type = row.get("asset_type")
            asset_id = row.get("asset_id")
            quantity_raw = row.get("quantity")
            if (
                isinstance(asset_type, str)
                and isinstance(asset_id, str)
                and isinstance(quantity_raw, int)
                and quantity_raw > 0
                and asset_type != "hull"
            ):
                asset_candidates.append(
                    {
                        "asset_type": asset_type,
                        "asset_id": asset_id,
                        "quantity": int(quantity_raw),
                    }
                )
        if asset_candidates:
            weighted = []
            for row in asset_candidates:
                weight = max(1, int(row["quantity"]))
                weighted.extend([row] * min(20, weight))
            selected = weighted[rng.randrange(0, len(weighted))]
            available = int(selected["quantity"])
            raw_roll = 1 + int(round((desired_qty * reward_scale) * rng.uniform(0.72, 1.36)))
            qty = max(1, min(available, raw_roll, desired_qty))
            delta_slots = self._estimate_personal_slot_delta_for_asset_add(
                player_id=actor_player_id,
                asset_type=str(selected["asset_type"]),
                asset_id=str(selected["asset_id"]),
                quantity=qty,
            )
            self._ensure_storage_capacity_for_reward(
                player_id=actor_player_id,
                additional_personal_asset_slots=float(delta_slots),
                additional_instance_slots=0,
                additional_smuggle_asset_slots=0.0,
            )
            target_after = self.state_store.adjust_asset_quantity(
                player_id=target_player_id,
                asset_type=str(selected["asset_type"]),
                asset_id=str(selected["asset_id"]),
                quantity_delta=-qty,
            )
            actor_after = self.state_store.adjust_asset_quantity(
                player_id=actor_player_id,
                asset_type=str(selected["asset_type"]),
                asset_id=str(selected["asset_id"]),
                quantity_delta=qty,
            )
            return {
                "kind": "asset",
                "asset_type": str(selected["asset_type"]),
                "asset_id": str(selected["asset_id"]),
                "quantity": int(qty),
                "target_remaining_quantity": int(target_after.get("quantity", 0)),
                "actor_quantity_after": int(actor_after.get("quantity", 0)),
            }

        target_inventory = self.state_store.list_inventory(player_id=target_player_id, limit=118)
        inv_candidates = [
            row
            for row in target_inventory
            if isinstance(row, dict)
            and isinstance(row.get("symbol"), str)
            and isinstance(row.get("amount"), (int, float))
            and float(row.get("amount")) > 1.0
        ]
        if not inv_candidates:
            return {"kind": "none", "reason": "target_empty"}
        weighted_symbols: list[dict[str, Any]] = []
        for row in inv_candidates:
            amount = max(1.0, float(row["amount"]))
            weighted_symbols.extend([row] * min(18, max(1, int(amount // 10))))
        selected_element = weighted_symbols[rng.randrange(0, len(weighted_symbols))]
        symbol = str(selected_element["symbol"])
        available_amount = max(0.0, float(selected_element["amount"]))
        roll_amount = (2.2 + (desired_qty * 2.1)) * reward_scale * rng.uniform(0.7, 1.4)
        amount = max(0.2, min(available_amount, round(roll_amount, 3)))
        self.state_store.apply_resource_delta(
            player_id=target_player_id,
            element_deltas={symbol: -amount},
        )
        actor_after = self.state_store.apply_resource_delta(
            player_id=actor_player_id,
            element_deltas={symbol: amount},
        )
        actor_inventory_after = actor_after.get("inventory", {})
        amount_after = (
            float(actor_inventory_after.get(symbol, 0.0))
            if isinstance(actor_inventory_after, dict)
            else 0.0
        )
        return {
            "kind": "element",
            "symbol": symbol,
            "amount": round(amount, 3),
            "actor_amount_after": round(amount_after, 3),
        }

    def _covert_sabotage_effect(
        self,
        *,
        actor_player_id: str,
        target_player_id: str,
        actor_stats: dict[str, float],
        target_level: int,
        reward_scale: float,
        rng: random.Random,
    ) -> dict[str, Any]:
        fleet_before = self._ensure_fleet_initialized(target_player_id)
        current_durability = float(fleet_before.get("hull_durability", 100.0))
        damage = (
            2.4
            + (actor_stats["attack"] / 175.0)
            + (target_level * 0.11)
            + rng.uniform(0.8, 4.4)
        ) * max(0.4, reward_scale)
        durability_loss = self._clamp(damage, 1.8, 24.0)
        fleet_after = self.state_store.update_fleet_state(
            player_id=target_player_id,
            hull_durability=max(0.0, current_durability - durability_loss),
        )
        disrupted_module: dict[str, Any] | None = None
        disruption_chance = self._clamp(0.18 + (0.22 * reward_scale), 0.18, 0.72)
        if rng.random() < disruption_chance:
            modules = self.state_store.list_assets(
                player_id=target_player_id,
                asset_type="module",
                limit=200,
            )
            module_candidates = [
                row
                for row in modules
                if isinstance(row, dict)
                and isinstance(row.get("asset_id"), str)
                and isinstance(row.get("quantity"), int)
                and int(row["quantity"]) > 0
            ]
            if module_candidates:
                chosen = module_candidates[rng.randrange(0, len(module_candidates))]
                asset_id = str(chosen["asset_id"])
                target_after = self.state_store.adjust_asset_quantity(
                    player_id=target_player_id,
                    asset_type="module",
                    asset_id=asset_id,
                    quantity_delta=-1,
                )
                disrupted_module = {
                    "asset_id": asset_id,
                    "quantity_lost": 1,
                    "target_remaining_quantity": int(target_after.get("quantity", 0)),
                }

        degraded_element: dict[str, Any] | None = None
        inv_rows = self.state_store.list_inventory(player_id=target_player_id, limit=25)
        element_rows = [
            row
            for row in inv_rows
            if isinstance(row, dict)
            and isinstance(row.get("symbol"), str)
            and isinstance(row.get("amount"), (int, float))
            and float(row.get("amount")) > 0.0
        ]
        if element_rows and rng.random() < 0.58:
            choice = element_rows[rng.randrange(0, len(element_rows))]
            symbol = str(choice["symbol"])
            available = max(0.0, float(choice["amount"]))
            loss = min(available, round((3.0 + (target_level * 0.35)) * rng.uniform(0.6, 1.8), 3))
            if loss > 0:
                self.state_store.apply_resource_delta(
                    player_id=target_player_id,
                    element_deltas={symbol: -loss},
                )
                degraded_element = {"symbol": symbol, "amount_lost": round(loss, 3)}

        return {
            "kind": "sabotage",
            "hull_durability_loss": round(durability_loss, 3),
            "fleet_before": {
                "hull_durability": round(current_durability, 3),
                "ship_level": fleet_before.get("ship_level"),
            },
            "fleet_after": {
                "hull_durability": round(float(fleet_after.get("hull_durability", 0.0)), 3),
                "ship_level": fleet_after.get("ship_level"),
            },
            "module_disruption": disrupted_module,
            "inventory_disruption": degraded_element,
        }

    def _covert_hack_effect(
        self,
        *,
        actor_player_id: str,
        target_player_id: str,
        target_level: int,
        reward_scale: float,
        rng: random.Random,
    ) -> dict[str, Any]:
        target_energy_before = self._get_player_action_energy(player_id=target_player_id)
        current_energy = max(0.0, float(target_energy_before.get("current_energy", 0.0)))
        drain = min(
            current_energy,
            (7.0 + (target_level * 0.32) + rng.uniform(0.8, 6.0)) * max(0.5, reward_scale),
        )
        target_energy_after = dict(target_energy_before)
        if drain > 0.001:
            target_energy_after = self._consume_player_action_energy(
                player_id=target_player_id,
                amount=drain,
                reason="covert_hack_disruption",
            )
        target_compute = self._player_compute_profile(player_id=target_player_id)
        bounty = (
            130.0
            + (float(target_compute.get("compute_power_per_hour", 0.0)) * 0.42)
            + (target_level * 10.0)
        ) * max(0.0, reward_scale) * rng.uniform(0.82, 1.26)
        bounty = round(max(0.0, bounty), 3)
        wallet_after = self.state_store.adjust_wallet(
            player_id=actor_player_id,
            credits_delta=bounty,
            voidcoin_delta=0.0,
        )
        target_assets = self.state_store.list_assets(player_id=target_player_id, limit=8)
        target_inventory = self.state_store.list_inventory(player_id=target_player_id, limit=6)
        intel = {
            "assets_sample": target_assets,
            "elements_sample": target_inventory,
            "discovered_worlds": self.state_store.count_discovered_worlds(player_id=target_player_id),
            "tech_unlock_count": len(self.state_store.list_unlocked_tech(player_id=target_player_id)),
        }
        return {
            "kind": "hack",
            "credits_bounty": bounty,
            "wallet_after": wallet_after,
            "target_energy_before": {
                "current_energy": round(current_energy, 3),
                "max_energy": target_energy_before.get("max_energy"),
            },
            "target_energy_after": {
                "current_energy": round(float(target_energy_after.get("current_energy", 0.0)), 3),
                "max_energy": target_energy_after.get("max_energy"),
            },
            "target_energy_drained": round(drain, 3),
            "intel": intel,
        }

    def _execute_covert_op(self, payload: dict[str, Any], op_type: str) -> dict[str, Any]:
        op_type_value = str(op_type).strip().casefold()
        if op_type_value not in {"steal", "sabotage", "hack"}:
            raise ValueError("op_type must be one of: steal, sabotage, hack")
        player_id = payload.get("player_id")
        target_player_id = payload.get("target_player_id")
        seed_raw = payload.get("seed")
        desired_qty_raw = payload.get("quantity", 1)
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(target_player_id, str) or not target_player_id.strip():
            raise ValueError("target_player_id must be a non-empty string")
        if isinstance(desired_qty_raw, bool) or not isinstance(desired_qty_raw, int):
            raise ValueError("quantity must be an integer when provided")
        desired_quantity = max(1, min(5, int(desired_qty_raw)))
        actor_id = player_id.strip()
        target_id = target_player_id.strip()
        if actor_id == target_id:
            raise ValueError("player_id and target_player_id must be different")
        self._ensure_player_bootstrap(actor_id)
        if not self.state_store.profile_exists(target_id):
            raise ValueError(f"Unknown target_player_id '{target_id}'")
        self._ensure_player_bootstrap(target_id, skip_auth=True)
        actor_is_admin = self._has_admin_privileges(actor_id)
        if target_id == "admin" and not actor_is_admin:
            raise ValueError("Target player is protected")

        cooldown_before = self.state_store.get_covert_cooldown(
            player_id=actor_id,
            op_type=op_type_value,
        )
        if (not actor_is_admin) and (not bool(cooldown_before.get("ready", True))):
            raise ValueError(
                "Covert op '{}' is on cooldown for {}s".format(
                    op_type_value,
                    int(cooldown_before.get("seconds_remaining", 0)),
                )
            )

        energy_cost = (
            ENERGY_COST_COVERT_STEAL
            if op_type_value == "steal"
            else (ENERGY_COST_COVERT_SABOTAGE if op_type_value == "sabotage" else ENERGY_COST_COVERT_HACK)
        )
        energy = self._consume_player_action_energy(
            player_id=actor_id,
            amount=energy_cost,
            reason=f"covert_{op_type_value}",
        )

        actor_stats = self._player_combat_stats(player_id=actor_id)
        target_stats = self._player_combat_stats(player_id=target_id)
        actor_level = self._combat_power_level(actor_stats)
        target_level = self._combat_power_level(target_stats)
        risk_profile = self._combat_risk_profile(
            player_stats=actor_stats,
            enemy_stats=target_stats,
        )
        reward_scaling = self._combat_reward_scaler(
            risk_profile=risk_profile,
            player_initiated_attack=True,
        )
        target_visibility = self.state_store.get_pvp_visibility_setting(player_id=target_id)
        actor_modifiers = self._player_identity_modifier_profile(player_id=actor_id).get("modifiers", {})
        target_modifiers = self._player_identity_modifier_profile(player_id=target_id).get("modifiers", {})
        actor_hacking_pct = (
            float(actor_modifiers.get("hacking_pct", 0.0))
            if isinstance(actor_modifiers, dict)
            and isinstance(actor_modifiers.get("hacking_pct"), (int, float))
            and not isinstance(actor_modifiers.get("hacking_pct"), bool)
            else 0.0
        )
        target_hacking_pct = (
            float(target_modifiers.get("hacking_pct", 0.0))
            if isinstance(target_modifiers, dict)
            and isinstance(target_modifiers.get("hacking_pct"), (int, float))
            and not isinstance(target_modifiers.get("hacking_pct"), bool)
            else 0.0
        )
        actor_compute = self._player_compute_profile(player_id=actor_id)
        target_compute = self._player_compute_profile(player_id=target_id)
        actor_compute_per_hour = float(actor_compute.get("compute_power_per_hour", 0.0))
        target_compute_per_hour = float(target_compute.get("compute_power_per_hour", 0.0))

        if op_type_value == "steal":
            vector = (
                (actor_stats["cloak"] * 0.74)
                + (actor_stats["scan"] * 0.42)
                + (actor_stats["energy"] * 0.17)
                + (actor_level * 3.8)
                + (actor_hacking_pct * 0.45)
            ) - (
                (target_stats["scan"] * 0.66)
                + (target_stats["cloak"] * 0.26)
                + (target_stats["defense"] * 0.21)
                + (target_level * 4.0)
                + (target_hacking_pct * 0.32)
            )
            success_probability = self._clamp(self._sigmoid(vector, scale=85.0), 0.06, 0.88)
        elif op_type_value == "sabotage":
            vector = (
                (actor_stats["attack"] * 0.53)
                + (actor_stats["scan"] * 0.21)
                + (actor_stats["energy"] * 0.19)
                + (actor_level * 3.9)
                + (actor_hacking_pct * 0.2)
            ) - (
                (target_stats["defense"] * 0.49)
                + (target_stats["hull"] * 0.11)
                + (target_stats["shield"] * 0.13)
                + (target_level * 4.1)
            )
            success_probability = self._clamp(self._sigmoid(vector, scale=92.0), 0.05, 0.84)
        else:
            vector = (
                (actor_compute_per_hour * 0.075)
                + (actor_stats["scan"] * 0.31)
                + (actor_level * 3.0)
                + (actor_hacking_pct * 0.9)
            ) - (
                (target_compute_per_hour * 0.072)
                + (target_stats["scan"] * 0.34)
                + (target_level * 3.15)
                + (target_hacking_pct * 0.85)
            )
            success_probability = self._clamp(self._sigmoid(vector, scale=88.0), 0.07, 0.9)

        level_gap_actor_minus_target = actor_level - target_level
        if level_gap_actor_minus_target > 0:
            success_probability *= max(0.08, 1.0 - min(0.82, level_gap_actor_minus_target * 0.07))
        elif level_gap_actor_minus_target < 0:
            success_probability *= min(1.38, 1.0 + (abs(level_gap_actor_minus_target) * 0.04))
        success_probability = self._clamp(success_probability, 0.04, 0.92)

        detection_probability = self._clamp(
            0.18
            + ((1.0 - success_probability) * 0.48)
            + ((target_stats["scan"] / max(1.0, actor_stats["scan"] + target_stats["scan"])) * 0.22),
            0.06,
            0.94,
        )
        blocked_by_fairplay = (
            (level_gap_actor_minus_target >= 4)
            and (not bool(target_visibility.get("allow_high_risk_visibility", False)))
            and (not actor_is_admin)
        )
        reward_scale = float(reward_scaling.get("reward_scale", 1.0))
        if blocked_by_fairplay:
            success_probability = min(success_probability, 0.04)
            reward_scale = 0.0
        elif level_gap_actor_minus_target > 0 and bool(reward_scaling.get("gank_penalty_active")):
            reward_scale *= 0.45
        elif level_gap_actor_minus_target < 0 and bool(reward_scaling.get("underdog_bonus_active")):
            reward_scale *= 1.12
        reward_scale = self._clamp(reward_scale, 0.0, 4.2)

        if isinstance(seed_raw, bool):
            raise ValueError("seed must be numeric or string when provided")
        if seed_raw is None:
            seed = int(stable_hash_int(actor_id, target_id, op_type_value, int(time.time())))
        elif isinstance(seed_raw, (int, float, str)):
            try:
                seed = int(seed_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("seed must be coercible to integer") from exc
        else:
            raise ValueError("seed must be numeric or string when provided")
        rng = random.Random(seed)
        success_roll = rng.random()
        detection_roll = rng.random()
        roll_success = success_roll < success_probability
        detected = detection_roll < detection_probability
        success = bool(roll_success and (not blocked_by_fairplay))
        status = "success" if success else ("blocked" if blocked_by_fairplay else "failed")

        outcome: dict[str, Any]
        if success:
            if op_type_value == "steal":
                outcome = self._covert_transfer_steal(
                    actor_player_id=actor_id,
                    target_player_id=target_id,
                    desired_quantity=desired_quantity,
                    reward_scale=max(0.25, reward_scale),
                    rng=rng,
                )
            elif op_type_value == "sabotage":
                outcome = self._covert_sabotage_effect(
                    actor_player_id=actor_id,
                    target_player_id=target_id,
                    actor_stats=actor_stats,
                    target_level=target_level,
                    reward_scale=max(0.35, reward_scale),
                    rng=rng,
                )
            else:
                outcome = self._covert_hack_effect(
                    actor_player_id=actor_id,
                    target_player_id=target_id,
                    target_level=target_level,
                    reward_scale=max(0.35, reward_scale),
                    rng=rng,
                )
        else:
            outcome = {
                "kind": op_type_value,
                "reason": (
                    "blocked_by_fairplay"
                    if blocked_by_fairplay
                    else "operation_failed"
                ),
            }

        penalty: dict[str, Any] | None = None
        if detected and not actor_is_admin:
            wallet_before = self.state_store.get_wallet(actor_id)
            available_credits = max(0.0, float(wallet_before.get("credits", 0.0)))
            base_fine = (
                180.0
                if op_type_value == "steal"
                else (420.0 if op_type_value == "sabotage" else 290.0)
            )
            fine = base_fine * (1.0 + max(0.0, level_gap_actor_minus_target) * 0.18)
            fine = min(available_credits, round(fine, 3))
            wallet_after = wallet_before
            if fine > 0:
                wallet_after = self.state_store.adjust_wallet(
                    player_id=actor_id,
                    credits_delta=-fine,
                    voidcoin_delta=0.0,
                )
            penalty = {
                "detected": True,
                "credits_fine": round(fine, 3),
                "wallet_after": wallet_after,
            }

        cooldown_base = (
            COVERT_STEAL_COOLDOWN_SECONDS
            if op_type_value == "steal"
            else (COVERT_SABOTAGE_COOLDOWN_SECONDS if op_type_value == "sabotage" else COVERT_HACK_COOLDOWN_SECONDS)
        )
        cooldown_scale = 1.0
        if status == "failed":
            cooldown_scale *= 1.25
        if status == "blocked":
            cooldown_scale *= 0.35
        if detected:
            cooldown_scale *= 1.28
        if op_type_value == "hack":
            cooldown_scale *= self._clamp(1.0 - (actor_hacking_pct * 0.0035), 0.74, 1.2)
        cooldown_seconds = 0.0 if actor_is_admin else (float(cooldown_base) * cooldown_scale)
        cooldown_after = self.state_store.set_covert_cooldown(
            player_id=actor_id,
            op_type=op_type_value,
            cooldown_seconds=cooldown_seconds,
        )

        log_row = self.state_store.log_covert_op(
            actor_player_id=actor_id,
            target_player_id=target_id,
            op_type=op_type_value,
            status=status,
            success_probability=success_probability,
            detection_probability=detection_probability,
            outcome={
                **outcome,
                "detected": bool(detected),
                "reward_scale": round(reward_scale, 4),
                "blocked_by_fairplay": bool(blocked_by_fairplay),
            },
        )
        return {
            "player_id": actor_id,
            "target_player_id": target_id,
            "op_type": op_type_value,
            "status": status,
            "success": bool(success),
            "detected": bool(detected),
            "blocked_by_fairplay": bool(blocked_by_fairplay),
            "energy_cost": round(float(energy_cost), 3),
            "energy": energy,
            "probabilities": {
                "success_probability": round(success_probability, 4),
                "detection_probability": round(detection_probability, 4),
            },
            "rolls": {
                "seed": int(seed),
                "success_roll": round(float(success_roll), 6),
                "detection_roll": round(float(detection_roll), 6),
            },
            "level_context": {
                "actor_level": int(actor_level),
                "target_level": int(target_level),
                "actor_minus_target": int(level_gap_actor_minus_target),
            },
            "risk_profile": risk_profile,
            "reward_scaling": reward_scaling,
            "outcome": outcome,
            "detected_penalty": penalty,
            "cooldown_before": cooldown_before,
            "cooldown_after": cooldown_after,
            "log": log_row,
        }

    def _life_support_rate_profile(self, player_id: str) -> dict[str, Any]:
        player_key = player_id.strip()
        fleet = self._ensure_fleet_initialized(player_id=player_key)
        crew_total_raw = fleet.get("crew_total", 0.0)
        crew_total = (
            max(0.0, float(crew_total_raw))
            if isinstance(crew_total_raw, (int, float)) and not isinstance(crew_total_raw, bool)
            else 0.0
        )

        demand_per_hour = {
            "AIR": crew_total * 0.032,
            "H2O": crew_total * 0.021,
            "FOOD": crew_total * 0.018,
        }
        production_per_hour = {"AIR": 0.0, "H2O": 0.0, "FOOD": 0.0}
        world_rows: list[dict[str, Any]] = []
        world_breakdown: list[dict[str, Any]] = []

        worlds = self.state_store.list_worlds_for_player(player_id=player_key)
        structure_index = self.seed_store.structure_index()
        total_population = 0.0

        body_biosphere_factor = {
            "planet": 1.0,
            "moon": 0.72,
            "gas_giant": 0.34,
            "asteroid": 0.22,
            "comet": 0.20,
            "star": 0.08,
        }

        for world in worlds:
            if not isinstance(world, dict):
                continue
            world_id = str(world.get("world_id", ""))
            body_class = str(world.get("body_class", "planet")).strip() or "planet"
            pop_raw = world.get("population_current", 0.0)
            population_current = (
                max(0.0, float(pop_raw))
                if isinstance(pop_raw, (int, float)) and not isinstance(pop_raw, bool)
                else 0.0
            )
            total_population += population_current
            world_rows.append(
                {
                    "world_id": world_id,
                    "population_current": population_current,
                    "world": dict(world),
                }
            )

            demand_per_hour["AIR"] += population_current * 0.00110
            demand_per_hour["H2O"] += population_current * 0.00072
            demand_per_hour["FOOD"] += population_current * 0.00058

            habitability_raw = (
                world.get("habitability_score")
                if world.get("habitability_score") is not None
                else world.get("habitability_index", 0.45)
            )
            if isinstance(habitability_raw, (int, float)) and not isinstance(habitability_raw, bool):
                habitability = max(0.0, min(1.0, float(habitability_raw)))
            else:
                habitability = 0.45

            body_factor = body_biosphere_factor.get(body_class, 0.45)
            biosphere_factor = max(0.05, (0.35 + (habitability * 0.95)) * body_factor)
            hydro_factor = max(0.05, (0.42 + (habitability * 0.85)) * max(0.34, body_factor))
            agro_factor = max(0.05, (0.38 + (habitability * 0.90)) * max(0.32, body_factor))

            world_air = population_current * 0.00082 * biosphere_factor
            world_water = population_current * 0.00055 * hydro_factor
            world_food = population_current * 0.00047 * agro_factor

            built = world.get("built_structures", [])
            if not isinstance(built, list):
                built = []
            for structure_id_raw in built:
                if not isinstance(structure_id_raw, str):
                    continue
                structure = structure_index.get(structure_id_raw)
                if not isinstance(structure, dict):
                    continue
                structure_id = structure_id_raw.casefold()
                modifiers = structure.get("modifiers", {})
                if not isinstance(modifiers, dict):
                    modifiers = {}
                tier_raw = structure.get("tier", 1)
                tier = int(tier_raw) if isinstance(tier_raw, int) else 1
                scale = 1.0 + (max(0, tier - 1) * 0.08)

                crew_support_raw = modifiers.get("crew_support_pct", 0.0)
                crew_support = (
                    float(crew_support_raw)
                    if isinstance(crew_support_raw, (int, float))
                    and not isinstance(crew_support_raw, bool)
                    else 0.0
                )
                pop_cap_raw = modifiers.get("population_capacity", 0.0)
                pop_capacity = (
                    float(pop_cap_raw)
                    if isinstance(pop_cap_raw, (int, float))
                    and not isinstance(pop_cap_raw, bool)
                    else 0.0
                )
                growth_raw = modifiers.get("population_growth_pct", 0.0)
                growth_pct = (
                    float(growth_raw)
                    if isinstance(growth_raw, (int, float))
                    and not isinstance(growth_raw, bool)
                    else 0.0
                )
                mining_raw = modifiers.get("mining_yield_pct", 0.0)
                mining_yield = (
                    float(mining_raw)
                    if isinstance(mining_raw, (int, float))
                    and not isinstance(mining_raw, bool)
                    else 0.0
                )

                if "life_support" in structure_id or "biosphere" in structure_id:
                    world_air += (4.5 + (crew_support * 0.22) + (pop_capacity * 0.0042)) * scale
                    world_water += (3.7 + (crew_support * 0.16) + (pop_capacity * 0.0031)) * scale
                    world_food += (3.2 + (crew_support * 0.13) + (pop_capacity * 0.0026)) * scale
                if (
                    "hydroponic" in structure_id
                    or "nutrient" in structure_id
                    or "biorefinery" in structure_id
                    or "arcology" in structure_id
                ):
                    world_food += (6.8 + (growth_pct * 0.35) + (pop_capacity * 0.0035)) * scale
                    world_air += (2.4 + (growth_pct * 0.12)) * scale
                    world_water += 1.4 * scale
                if (
                    "water" in structure_id
                    or "reclamation" in structure_id
                    or "ammonia" in structure_id
                    or "ice" in structure_id
                ):
                    world_water += (7.1 + (crew_support * 0.16) + (mining_yield * 0.12)) * scale
                if "emergency" in structure_id or "bunker" in structure_id:
                    world_air += 2.2 * scale
                    world_water += 2.2 * scale
                    world_food += 1.7 * scale

                world_air += crew_support * 0.045
                world_water += crew_support * 0.034
                world_food += crew_support * 0.029

            world_air = max(0.0, world_air)
            world_water = max(0.0, world_water)
            world_food = max(0.0, world_food)
            production_per_hour["AIR"] += world_air
            production_per_hour["H2O"] += world_water
            production_per_hour["FOOD"] += world_food
            world_breakdown.append(
                {
                    "world_id": world_id,
                    "body_class": body_class,
                    "population_current": round(population_current, 3),
                    "production_per_hour": {
                        "AIR": round(world_air, 4),
                        "H2O": round(world_water, 4),
                        "FOOD": round(world_food, 4),
                    },
                }
            )

        module_assets = self.state_store.list_assets(
            player_id=player_key,
            asset_type="module",
            limit=900,
        )
        module_index = self.seed_store.module_index()
        module_output = {"AIR": 0.0, "H2O": 0.0, "FOOD": 0.0}
        for row in module_assets:
            if not isinstance(row, dict):
                continue
            module_id = row.get("asset_id")
            quantity_raw = row.get("quantity", 0)
            if not isinstance(module_id, str):
                continue
            if isinstance(quantity_raw, bool) or not isinstance(quantity_raw, int):
                continue
            quantity = max(0, int(quantity_raw))
            if quantity <= 0:
                continue
            module = module_index.get(module_id)
            if not isinstance(module, dict):
                continue
            tier_raw = module.get("tier", 1)
            tier = int(tier_raw) if isinstance(tier_raw, int) else 1
            tier_scale = 1.0 + (max(0, tier - 1) * 0.12)
            effective_qty = min(2.0, float(quantity))
            module_key = module_id.casefold()
            bonuses = module.get("stat_bonuses", {})
            if not isinstance(bonuses, dict):
                bonuses = {}
            crew_cap_raw = bonuses.get("crew_capacity", 0.0)
            crew_cap = (
                float(crew_cap_raw)
                if isinstance(crew_cap_raw, (int, float)) and not isinstance(crew_cap_raw, bool)
                else 0.0
            )
            passenger_cap_raw = bonuses.get("passenger_capacity", 0.0)
            passenger_cap = (
                float(passenger_cap_raw)
                if isinstance(passenger_cap_raw, (int, float))
                and not isinstance(passenger_cap_raw, bool)
                else 0.0
            )
            capacity_factor = max(0.0, (crew_cap + (passenger_cap * 0.6)) / 240.0)
            module_output["AIR"] += capacity_factor * 0.65 * effective_qty * tier_scale
            module_output["H2O"] += capacity_factor * 0.52 * effective_qty * tier_scale
            module_output["FOOD"] += capacity_factor * 0.46 * effective_qty * tier_scale

            if "life_support" in module_key or "biosphere" in module_key:
                module_output["AIR"] += 3.2 * effective_qty * tier_scale
                module_output["H2O"] += 2.4 * effective_qty * tier_scale
                module_output["FOOD"] += 2.0 * effective_qty * tier_scale
            if "hydroponic" in module_key or "bioreactor" in module_key:
                module_output["FOOD"] += 3.6 * effective_qty * tier_scale
                module_output["AIR"] += 1.8 * effective_qty * tier_scale
                module_output["H2O"] += 1.2 * effective_qty * tier_scale
            if "water_reclaimer" in module_key or (
                "water" in module_key and "weapon" not in module_key
            ):
                module_output["H2O"] += 3.8 * effective_qty * tier_scale
            if "nutrient" in module_key or "seedbank" in module_key:
                module_output["FOOD"] += 2.8 * effective_qty * tier_scale

        for symbol in LIFE_SUPPORT_SYMBOLS:
            module_output[symbol] = min(64.0, max(0.0, module_output[symbol]))
            production_per_hour[symbol] += module_output[symbol]

        surplus_per_hour = {
            symbol: round(production_per_hour[symbol] - demand_per_hour[symbol], 6)
            for symbol in LIFE_SUPPORT_SYMBOLS
        }
        return {
            "crew_total": round(crew_total, 6),
            "population_total": round(total_population, 6),
            "demand_per_hour": {
                symbol: round(max(0.0, demand_per_hour[symbol]), 6)
                for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "production_per_hour": {
                symbol: round(max(0.0, production_per_hour[symbol]), 6)
                for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "surplus_per_hour": surplus_per_hour,
            "module_output_per_hour": {
                symbol: round(module_output[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "world_output_preview": world_breakdown[:20],
            "world_rows": world_rows,
        }

    def _apply_life_support_runtime(
        self,
        player_id: str,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        player_key = player_id.strip()
        if not player_key:
            raise ValueError("player_id must be a non-empty string")

        now_epoch = int(time.time())
        now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_epoch))
        state_before = self.state_store.get_life_support_state(player_id=player_key)

        # Keep admin testing frictionless; admin account is meant to act as a true god-mode test actor.
        if player_key == "admin":
            state_after = self.state_store.set_life_support_state(
                player_id=player_key,
                last_tick_utc=now_utc,
                deficit_air=0.0,
                deficit_water=0.0,
                deficit_food=0.0,
                shortage_stress=0.0,
            )
            inventory = self.state_store.get_inventory_amounts(
                player_id=player_key,
                symbols=list(LIFE_SUPPORT_SYMBOLS),
            )
            return {
                "player_id": player_key,
                "applied": False,
                "bypass_reason": "admin_god_mode",
                "elapsed_hours": 0.0,
                "effective_hours": 0.0,
                "backlog_hours_remaining": 0.0,
                "demand_per_hour": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "production_per_hour": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "consumed_units": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "produced_units": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "deficit_units": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "inventory_after": {
                    symbol: round(float(inventory.get(symbol, 0.0)), 6)
                    for symbol in LIFE_SUPPORT_SYMBOLS
                },
                "shortage_ratio": 0.0,
                "penalties": {
                    "crew_losses": 0.0,
                    "population_losses": 0.0,
                    "credits_penalty": 0.0,
                },
                "state_before": state_before,
                "state_after": state_after,
            }

        last_tick_utc_raw = state_before.get("last_tick_utc", now_utc)
        if isinstance(last_tick_utc_raw, str):
            last_tick_utc = last_tick_utc_raw
        else:
            last_tick_utc = now_utc
        try:
            last_epoch = int(calendar.timegm(time.strptime(last_tick_utc, "%Y-%m-%dT%H:%M:%SZ")))
        except Exception:
            last_epoch = now_epoch
        elapsed_hours = max(0.0, (now_epoch - last_epoch) / 3600.0)
        effective_hours = min(float(LIFE_SUPPORT_MAX_TICK_HOURS), elapsed_hours)
        backlog_hours_remaining = max(0.0, elapsed_hours - effective_hours)

        profile = self._life_support_rate_profile(player_id=player_key)
        demand_per_hour = {
            symbol: max(0.0, float(profile["demand_per_hour"].get(symbol, 0.0)))
            for symbol in LIFE_SUPPORT_SYMBOLS
        }
        production_per_hour = {
            symbol: max(0.0, float(profile["production_per_hour"].get(symbol, 0.0)))
            for symbol in LIFE_SUPPORT_SYMBOLS
        }

        if (effective_hours <= 1e-9) and (not force):
            inventory_now = self.state_store.get_inventory_amounts(
                player_id=player_key,
                symbols=list(LIFE_SUPPORT_SYMBOLS),
            )
            return {
                "player_id": player_key,
                "applied": False,
                "elapsed_hours": round(elapsed_hours, 6),
                "effective_hours": round(effective_hours, 6),
                "backlog_hours_remaining": round(backlog_hours_remaining, 6),
                "demand_per_hour": {
                    symbol: round(demand_per_hour[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
                },
                "production_per_hour": {
                    symbol: round(production_per_hour[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
                },
                "consumed_units": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "produced_units": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "deficit_units": {symbol: 0.0 for symbol in LIFE_SUPPORT_SYMBOLS},
                "inventory_after": {
                    symbol: round(float(inventory_now.get(symbol, 0.0)), 6)
                    for symbol in LIFE_SUPPORT_SYMBOLS
                },
                "shortage_ratio": 0.0,
                "penalties": {
                    "crew_losses": 0.0,
                    "population_losses": 0.0,
                    "credits_penalty": 0.0,
                },
                "state_before": state_before,
                "state_after": state_before,
            }

        inventory_before = self.state_store.get_inventory_amounts(
            player_id=player_key,
            symbols=list(LIFE_SUPPORT_SYMBOLS),
        )

        required_units = {
            symbol: demand_per_hour[symbol] * effective_hours for symbol in LIFE_SUPPORT_SYMBOLS
        }
        produced_units = {
            symbol: production_per_hour[symbol] * effective_hours for symbol in LIFE_SUPPORT_SYMBOLS
        }
        consumed_units: dict[str, float] = {}
        deficit_units: dict[str, float] = {}
        net_deltas: dict[str, float] = {}
        inventory_after_calc: dict[str, float] = {}
        for symbol in LIFE_SUPPORT_SYMBOLS:
            before_amount = max(0.0, float(inventory_before.get(symbol, 0.0)))
            required = max(0.0, required_units[symbol])
            produced = max(0.0, produced_units[symbol])
            available_after_production = before_amount + produced
            consumed = min(required, available_after_production)
            after_amount = max(0.0, available_after_production - consumed)
            deficit = max(0.0, required - consumed)
            consumed_units[symbol] = consumed
            deficit_units[symbol] = deficit
            inventory_after_calc[symbol] = after_amount
            net_deltas[symbol] = after_amount - before_amount

        if any(abs(value) > 1e-9 for value in net_deltas.values()):
            self.state_store.apply_resource_delta(
                player_id=player_key,
                element_deltas=net_deltas,
            )

        weighted_need = (
            (required_units["AIR"] * 1.35)
            + (required_units["H2O"] * 1.00)
            + (required_units["FOOD"] * 1.20)
        )
        weighted_deficit = (
            (deficit_units["AIR"] * 1.35)
            + (deficit_units["H2O"] * 1.00)
            + (deficit_units["FOOD"] * 1.20)
        )
        shortage_ratio = (
            min(1.0, weighted_deficit / weighted_need)
            if weighted_need > 1e-9
            else 0.0
        )

        crew_losses = 0.0
        population_losses = 0.0
        credits_penalty = 0.0
        if shortage_ratio > 1e-9 and effective_hours > 1e-9:
            crew_total = max(0.0, float(profile.get("crew_total", 0.0)))
            total_population = max(0.0, float(profile.get("population_total", 0.0)))
            crew_losses = min(
                crew_total * 0.24,
                crew_total * shortage_ratio * effective_hours * 0.0024,
            )
            if crew_losses > 1e-6:
                self.state_store.apply_fleet_combat_losses(
                    player_id=player_key,
                    hull_durability_loss=0.0,
                    crew_casualties=crew_losses,
                    cargo_loss_ratio=0.0,
                )

            population_losses = min(
                total_population * 0.18,
                total_population * shortage_ratio * effective_hours * 0.00035,
            )
            if population_losses > 1e-6:
                distributed = 0.0
                rows = profile.get("world_rows", [])
                if isinstance(rows, list):
                    world_total_for_distribution = sum(
                        float(row.get("population_current", 0.0))
                        for row in rows
                        if isinstance(row, dict)
                        and isinstance(row.get("population_current"), (int, float))
                        and not isinstance(row.get("population_current"), bool)
                        and float(row.get("population_current")) > 0.0
                    )
                    for idx, row in enumerate(rows):
                        if not isinstance(row, dict):
                            continue
                        world_payload = row.get("world")
                        if not isinstance(world_payload, dict):
                            continue
                        current_pop = float(row.get("population_current", 0.0))
                        if current_pop <= 0.0 or world_total_for_distribution <= 1e-9:
                            continue
                        remaining = max(0.0, population_losses - distributed)
                        if remaining <= 1e-9:
                            break
                        if idx == (len(rows) - 1):
                            loss_share = remaining
                        else:
                            loss_share = population_losses * (current_pop / world_total_for_distribution)
                        applied_loss = min(current_pop, max(0.0, loss_share))
                        if applied_loss <= 1e-9:
                            continue
                        new_pop = max(0, int(round(current_pop - applied_loss)))
                        current_pop_int = int(round(current_pop))
                        if new_pop != current_pop_int:
                            world_next = dict(world_payload)
                            world_next["population_current"] = new_pop
                            growth_raw = world_next.get("population_growth_per_day_pct", 0.0)
                            if isinstance(growth_raw, (int, float)) and not isinstance(growth_raw, bool):
                                growth_now = max(0.0, float(growth_raw))
                                growth_multiplier = max(0.45, 1.0 - (shortage_ratio * 0.18))
                                world_next["population_growth_per_day_pct"] = round(
                                    growth_now * growth_multiplier,
                                    4,
                                )
                            self.state_store.update_world_payload(
                                player_id=player_key,
                                world=world_next,
                            )
                        distributed += applied_loss

            base_penalty = (
                (max(0.0, float(profile.get("crew_total", 0.0))) * 0.45)
                + (max(0.0, float(profile.get("population_total", 0.0))) * 0.0065)
            )
            requested_penalty = base_penalty * shortage_ratio * effective_hours * 0.22
            wallet = self.state_store.get_wallet(player_key)
            credits_available = max(0.0, float(wallet.get("credits", 0.0)))
            credits_penalty = min(credits_available, max(0.0, requested_penalty))
            if credits_penalty > 1e-6:
                self.state_store.adjust_wallet(
                    player_id=player_key,
                    credits_delta=-credits_penalty,
                    voidcoin_delta=0.0,
                )

        previous_deficits = {
            "AIR": max(0.0, float(state_before.get("deficit_air", 0.0))),
            "H2O": max(0.0, float(state_before.get("deficit_water", 0.0))),
            "FOOD": max(0.0, float(state_before.get("deficit_food", 0.0))),
        }
        next_deficits: dict[str, float] = {}
        for symbol in LIFE_SUPPORT_SYMBOLS:
            surplus_units = max(0.0, produced_units[symbol] - required_units[symbol])
            next_deficits[symbol] = max(
                0.0,
                previous_deficits[symbol] + deficit_units[symbol] - (surplus_units * 0.22),
            )

        shortage_stress_before = max(0.0, float(state_before.get("shortage_stress", 0.0)))
        shortage_stress_after = shortage_stress_before + (shortage_ratio * min(14.0, effective_hours * 0.75))
        if shortage_ratio <= 1e-9:
            shortage_stress_after = max(
                0.0,
                shortage_stress_after - min(12.0, effective_hours * 0.85),
            )
        shortage_stress_after = max(0.0, min(100.0, shortage_stress_after))

        applied_seconds = int(round(max(0.0, effective_hours) * 3600.0))
        next_tick_epoch = last_epoch + applied_seconds
        next_tick_epoch = min(next_tick_epoch, now_epoch)
        next_tick_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(next_tick_epoch))
        state_after = self.state_store.set_life_support_state(
            player_id=player_key,
            last_tick_utc=next_tick_utc,
            deficit_air=next_deficits["AIR"],
            deficit_water=next_deficits["H2O"],
            deficit_food=next_deficits["FOOD"],
            shortage_stress=shortage_stress_after,
        )
        inventory_after = self.state_store.get_inventory_amounts(
            player_id=player_key,
            symbols=list(LIFE_SUPPORT_SYMBOLS),
        )
        wallet_after = self.state_store.get_wallet(player_key)

        return {
            "player_id": player_key,
            "applied": True,
            "elapsed_hours": round(elapsed_hours, 6),
            "effective_hours": round(effective_hours, 6),
            "backlog_hours_remaining": round(backlog_hours_remaining, 6),
            "demand_per_hour": {
                symbol: round(demand_per_hour[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "production_per_hour": {
                symbol: round(production_per_hour[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "required_units": {
                symbol: round(required_units[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "produced_units": {
                symbol: round(produced_units[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "consumed_units": {
                symbol: round(consumed_units[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "deficit_units": {
                symbol: round(deficit_units[symbol], 6) for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "inventory_before": {
                symbol: round(float(inventory_before.get(symbol, 0.0)), 6)
                for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "inventory_after": {
                symbol: round(float(inventory_after.get(symbol, 0.0)), 6)
                for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "shortage_ratio": round(shortage_ratio, 6),
            "shortage_stress_before": round(shortage_stress_before, 6),
            "shortage_stress_after": round(shortage_stress_after, 6),
            "penalties": {
                "crew_losses": round(crew_losses, 6),
                "population_losses": round(population_losses, 6),
                "credits_penalty": round(credits_penalty, 6),
            },
            "crew_total": round(float(profile.get("crew_total", 0.0)), 6),
            "population_total": round(float(profile.get("population_total", 0.0)), 6),
            "module_output_per_hour": profile.get("module_output_per_hour", {}),
            "world_output_preview": profile.get("world_output_preview", []),
            "wallet_after": wallet_after,
            "state_before": state_before,
            "state_after": state_after,
        }

    def _life_support_status(self, player_id: str, force_tick: bool = False) -> dict[str, Any]:
        player_key = player_id.strip()
        if not player_key:
            raise ValueError("player_id must be a non-empty string")
        tick = self._apply_life_support_runtime(player_id=player_key, force=bool(force_tick))
        state = self.state_store.get_life_support_state(player_key)
        inventory = self.state_store.get_inventory_amounts(
            player_id=player_key,
            symbols=list(LIFE_SUPPORT_SYMBOLS),
        )
        rates = self._life_support_rate_profile(player_id=player_key)
        return {
            "player_id": player_key,
            "symbols": list(LIFE_SUPPORT_SYMBOLS),
            "state": state,
            "inventory": {
                symbol: round(float(inventory.get(symbol, 0.0)), 6)
                for symbol in LIFE_SUPPORT_SYMBOLS
            },
            "demand_per_hour": rates.get("demand_per_hour", {}),
            "production_per_hour": rates.get("production_per_hour", {}),
            "surplus_per_hour": rates.get("surplus_per_hour", {}),
            "tick": tick,
        }

    def _life_support_market_row(
        self,
        *,
        symbol: str,
        bucket: int,
        holdings: float,
        shortage_stress: float,
        demand_pressure: float,
        region: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        specs = {
            "AIR": {"name": "Compressed Atmosphere", "base_price": 4.8, "volatility": 0.08, "idx": 1},
            "H2O": {"name": "Reclaimed Water", "base_price": 6.7, "volatility": 0.10, "idx": 2},
            "FOOD": {"name": "Nutrient Rations", "base_price": 9.2, "volatility": 0.12, "idx": 3},
        }
        spec = specs.get(symbol, {"name": symbol, "base_price": 5.0, "volatility": 0.1, "idx": 0})
        idx = int(spec["idx"])
        seed = ((bucket * 2654435761) ^ (idx * 1013904223)) & 0xFFFFFFFF
        rng = random.Random(seed)
        pressure = max(0.0, min(1.0, demand_pressure))
        stress_factor = max(0.0, min(1.0, float(shortage_stress) / 100.0))
        base_price = float(spec["base_price"]) * (1.0 + (pressure * 0.42) + (stress_factor * 0.26))
        volatility = min(0.26, float(spec["volatility"]) + (pressure * 0.08) + (stress_factor * 0.05))
        wave = math.sin((bucket / 5.0) + (idx * 0.83)) * volatility
        jitter = rng.uniform(-volatility, volatility)
        mid_price = max(0.2, base_price * (1.0 + wave + jitter))
        spread = 0.025 + (volatility * 0.95)

        liquidity_multiplier = 1.0
        risk_premium_pct = 0.0
        spread_multiplier = 1.0
        region_id = None
        if isinstance(region, dict):
            region_id = region.get("id")
            raw_liquidity = region.get("liquidity_multiplier", 1.0)
            raw_risk = region.get("risk_premium_pct", 0.0)
            raw_spread = region.get("spread_multiplier", 1.0)
            if isinstance(raw_liquidity, (int, float)) and not isinstance(raw_liquidity, bool):
                liquidity_multiplier = max(0.2, min(3.0, float(raw_liquidity)))
            if isinstance(raw_risk, (int, float)) and not isinstance(raw_risk, bool):
                risk_premium_pct = max(-30.0, min(120.0, float(raw_risk)))
            if isinstance(raw_spread, (int, float)) and not isinstance(raw_spread, bool):
                spread_multiplier = max(0.5, min(3.0, float(raw_spread)))

        mid_price = max(0.01, mid_price * (1.0 + (risk_premium_pct / 100.0)))
        mid_price = max(
            0.01,
            mid_price * (1.0 + ((1.0 / max(0.2, liquidity_multiplier)) - 1.0) * 0.28),
        )
        spread *= spread_multiplier
        ask_credits = max(0.01, mid_price * (1.0 + (spread / 2.0)))
        bid_credits = max(0.01, mid_price * (1.0 - (spread / 2.0)))
        credits_per_voidcoin = self._voidcoin_rate_credits(bucket=bucket)

        return {
            "symbol": symbol,
            "name": spec["name"],
            "atomic_number": None,
            "group_block": "commodity",
            "standard_state": "mixed",
            "rarity_tier": "industrial",
            "demand_score": round(max(0.0, min(1.0, 0.35 + pressure + (stress_factor * 0.55))), 4),
            "volatility": round(volatility, 4),
            "mid_credits": round(mid_price, 4),
            "bid_credits": round(bid_credits, 4),
            "ask_credits": round(ask_credits, 4),
            "bid_voidcoin": round(bid_credits / credits_per_voidcoin, 8),
            "ask_voidcoin": round(ask_credits / credits_per_voidcoin, 8),
            "holdings": round(max(0.0, float(holdings)), 3),
            "region_id": region_id,
            "regional_risk_premium_pct": round(risk_premium_pct, 3),
            "market_tick": bucket,
            "market_class": "commodity",
            "commodity": True,
        }

    def _action_energy_modifiers(self, player_id: str) -> dict[str, float]:
        module_assets = self.state_store.list_assets(
            player_id=player_id,
            asset_type="module",
            limit=800,
        )
        module_index = self.seed_store.module_index()
        max_bonus = 0.0
        regen_bonus = 0.0
        for row in module_assets:
            if not isinstance(row, dict):
                continue
            module_id = row.get("asset_id")
            qty_raw = row.get("quantity", 0)
            if not isinstance(module_id, str):
                continue
            if isinstance(qty_raw, bool) or not isinstance(qty_raw, int):
                continue
            quantity = max(0, qty_raw)
            if quantity <= 0:
                continue
            module = module_index.get(module_id)
            if not isinstance(module, dict):
                continue
            bonuses = module.get("stat_bonuses", {})
            if not isinstance(bonuses, dict):
                continue
            tier_raw = module.get("tier", 1)
            tier = int(tier_raw) if isinstance(tier_raw, int) else 1
            effective_qty = min(2.0, float(quantity))
            tier_scale = 1.0 + min(0.5, tier * 0.045)
            raw_max = bonuses.get("action_energy_max")
            if isinstance(raw_max, (int, float)) and not isinstance(raw_max, bool):
                max_bonus += float(raw_max) * effective_qty * tier_scale
            raw_regen = bonuses.get("action_energy_regen")
            if isinstance(raw_regen, (int, float)) and not isinstance(raw_regen, bool):
                regen_bonus += float(raw_regen) * effective_qty * tier_scale
        fleet = self._ensure_fleet_initialized(player_id=player_id)
        active_hull_id = fleet.get("active_hull_id")
        hull = None
        if isinstance(active_hull_id, str):
            hull = self.seed_store.hull_index().get(active_hull_id)
        if isinstance(hull, dict):
            tier_raw = hull.get("tier", 1)
            tier = int(tier_raw) if isinstance(tier_raw, int) else 1
            max_bonus += max(0.0, tier * 6.5)
            regen_bonus += max(0.0, (tier - 1) * 0.42)
        return {
            "max_energy_bonus": round(min(420.0, max_bonus), 4),
            "regen_bonus_per_hour": round(min(58.0, regen_bonus), 4),
        }

    def _get_player_action_energy(self, player_id: str) -> dict[str, Any]:
        modifiers = self._action_energy_modifiers(player_id=player_id)
        payload = self.state_store.get_action_energy(
            player_id=player_id,
            max_energy_bonus=modifiers["max_energy_bonus"],
            regen_bonus_per_hour=modifiers["regen_bonus_per_hour"],
        )
        payload["modifiers"] = modifiers
        return payload

    def _consume_player_action_energy(self, player_id: str, amount: float, reason: str) -> dict[str, Any]:
        modifiers = self._action_energy_modifiers(player_id=player_id)
        payload = self.state_store.consume_action_energy(
            player_id=player_id,
            amount=amount,
            reason=reason,
            max_energy_bonus=modifiers["max_energy_bonus"],
            regen_bonus_per_hour=modifiers["regen_bonus_per_hour"],
        )
        payload["modifiers"] = modifiers
        return payload

    def _grant_admin_god_mode(self, player_id: str) -> None:
        symbols = [
            str(row.get("symbol"))
            for row in self.seed_store.elements
            if isinstance(row, dict) and isinstance(row.get("symbol"), str)
        ]
        for symbol in LIFE_SUPPORT_SYMBOLS:
            if symbol not in symbols:
                symbols.append(symbol)
        floors = {symbol: ADMIN_GOD_ELEMENT_FLOOR for symbol in symbols}
        self.state_store.set_wallet_balances(
            player_id=player_id,
            credits=ADMIN_GOD_CREDITS,
            voidcoin=ADMIN_GOD_VOIDCOIN,
        )
        self.state_store.set_action_energy(
            player_id=player_id,
            current_energy=10_000_000.0,
            max_energy=10_000_000.0,
            regen_per_hour=100_000.0,
        )
        self.state_store.set_inventory_floor(player_id=player_id, symbol_floors=floors)
        self.state_store.add_storage_upgrade(
            player_id=player_id,
            personal_slots_delta=5000.0,
            smuggle_slots_delta=1200.0,
        )
        for tech_id in [
            node.get("id")
            for node in self.seed_store.tech_tree
            if isinstance(node, dict) and isinstance(node.get("id"), str)
        ]:
            if isinstance(tech_id, str):
                self.state_store.unlock_tech(player_id=player_id, tech_id=tech_id)
        for consumable in self.seed_store.consumables:
            if not isinstance(consumable, dict):
                continue
            consumable_id = consumable.get("id")
            if isinstance(consumable_id, str):
                self.state_store.add_asset(
                    player_id=player_id,
                    asset_type="consumable",
                    asset_id=consumable_id,
                    quantity=250,
                )
        for recipe in self.seed_store.reverse_engineering_recipes:
            if not isinstance(recipe, dict):
                continue
            blueprint_id = recipe.get("unlock_blueprint_id")
            if isinstance(blueprint_id, str):
                self.state_store.add_asset(
                    player_id=player_id,
                    asset_type="blueprint",
                    asset_id=blueprint_id,
                    quantity=50,
                )

    def _fairplay_policy_payload(self) -> dict[str, Any]:
        return {
            "policy_version": "1.0",
            "monetization_model": "non_pay_to_win",
            "principles": [
                "No direct stat purchases with real money.",
                "Power progression is earned through gameplay loops.",
                "Cosmetic monetization only in competitive contexts.",
                "Matchmaking and combat outcomes are independent from spending."
            ],
            "allowed_store_categories": [
                "cosmetic_skins",
                "ui_themes",
                "profile_badges"
            ],
            "disallowed_store_categories": [
                "direct_stat_boosts",
                "paid_win_probability_modifiers",
                "exclusive_combat_only_power_items"
            ],
        }

    def _player_faction_bonus_profile(self, player_id: str) -> dict[str, Any]:
        if not isinstance(player_id, str) or not player_id.strip():
            return {"faction_id": None, "faction_name": None, "bonuses": {}}
        affiliation = self.state_store.get_player_faction_affiliation(player_id=player_id.strip())
        if not isinstance(affiliation, dict):
            return {"faction_id": None, "faction_name": None, "bonuses": {}}
        faction_id = affiliation.get("faction_id")
        if not isinstance(faction_id, str):
            return {"faction_id": None, "faction_name": None, "bonuses": {}}
        faction = self.seed_store.faction_index().get(faction_id)
        if not isinstance(faction, dict):
            return {"faction_id": faction_id, "faction_name": None, "bonuses": {}}
        raw_bonus = faction.get("alignment_bonus", {})
        bonuses: dict[str, float] = {}
        if isinstance(raw_bonus, dict):
            for key, raw in raw_bonus.items():
                if not isinstance(key, str):
                    continue
                if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                    continue
                value = float(raw)
                if not math.isfinite(value):
                    continue
                bonuses[key] = value
        return {
            "faction_id": faction_id,
            "faction_name": faction.get("name") if isinstance(faction.get("name"), str) else None,
            "bonuses": bonuses,
        }

    def _player_identity_variance_profile(
        self,
        player_id: str,
        race_id: str | None = None,
        profession_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(player_id, str) or not player_id.strip():
            return {}
        profile = self.state_store.get_profile(player_id=player_id.strip())
        if not isinstance(race_id, str):
            race_id = profile.get("race_id") if isinstance(profile.get("race_id"), str) else None
        if not isinstance(profession_id, str):
            profession_id = (
                profile.get("profession_id")
                if isinstance(profile.get("profession_id"), str)
                else None
            )
        memory = profile.get("player_memory", {})
        if not isinstance(memory, dict):
            memory = {}
        existing = memory.get("identity_variance")
        if (
            isinstance(existing, dict)
            and existing.get("schema_version") == 1
            and existing.get("race_id") == race_id
            and existing.get("profession_id") == profession_id
        ):
            return existing

        rng = random.Random(
            stable_hash_int(
                player_id.strip(),
                race_id or "",
                profession_id or "",
                "identity_variance_v1",
            )
        )
        focus_pool = ["scan", "speed", "defense", "attack", "energy", "cloak", "cargo"]
        preferred_focus = focus_pool[rng.randrange(0, len(focus_pool))]

        def _clamped_gauss(mu: float, sigma: float, floor: float, ceil: float) -> float:
            return max(floor, min(ceil, rng.gauss(mu, sigma)))

        generated = {
            "schema_version": 1,
            "race_id": race_id,
            "profession_id": profession_id,
            "craft_quality_mean_shift_pct": round(_clamped_gauss(0.0, 1.7, -4.5, 4.5), 4),
            "craft_quality_sigma_shift_pct": round(_clamped_gauss(0.0, 7.0, -18.0, 18.0), 4),
            "craft_jackpot_shift_pct": round(_clamped_gauss(0.0, 10.0, -25.0, 25.0), 4),
            "preferred_focus": preferred_focus,
        }
        self.state_store.update_profile_memory(
            player_id=player_id.strip(),
            player_memory={"identity_variance": generated},
            merge=True,
        )
        return generated

    def _player_identity_modifier_profile(self, player_id: str) -> dict[str, Any]:
        if not isinstance(player_id, str) or not player_id.strip():
            return {"race_id": None, "profession_id": None, "modifiers": {}, "variance": {}}
        profile = self.state_store.get_profile(player_id=player_id.strip())
        race_id = profile.get("race_id") if isinstance(profile.get("race_id"), str) else None
        profession_id = (
            profile.get("profession_id")
            if isinstance(profile.get("profession_id"), str)
            else None
        )
        race = self.seed_store.race_index().get(race_id or "")
        profession = self.seed_store.profession_index().get(profession_id or "")
        modifiers: dict[str, float] = {}

        def _merge_modifier_payload(raw_payload: Any) -> None:
            if not isinstance(raw_payload, dict):
                return
            for key, raw in raw_payload.items():
                if not isinstance(key, str):
                    continue
                if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                    continue
                modifiers[key] = modifiers.get(key, 0.0) + float(raw)

        if isinstance(race, dict):
            _merge_modifier_payload(race.get("stat_modifiers"))
        if isinstance(profession, dict):
            _merge_modifier_payload(profession.get("passive_effect"))
        variance = self._player_identity_variance_profile(
            player_id=player_id.strip(),
            race_id=race_id,
            profession_id=profession_id,
        )
        return {
            "race_id": race_id,
            "profession_id": profession_id,
            "race_name": race.get("name") if isinstance(race, dict) else None,
            "profession_name": profession.get("name") if isinstance(profession, dict) else None,
            "modifiers": modifiers,
            "variance": variance if isinstance(variance, dict) else {},
        }

    def _player_compute_profile(self, player_id: str) -> dict[str, Any]:
        if self._has_admin_privileges(player_id):
            return {
                "player_id": player_id,
                "compute_power_per_hour": 1_000_000_000.0,
                "components": {
                    "base": 1_000_000_000.0,
                    "world_bonus": 0.0,
                    "structure_bonus": 0.0,
                    "module_bonus": 0.0,
                    "population_bonus": 0.0,
                    "research_yield_bonus_pct": 0.0,
                },
                "god_mode": True,
                "research_jobs": {
                    "active": self.state_store.list_research_jobs(
                        player_id=player_id,
                        status="active",
                        limit=80,
                    ),
                    "completed": self.state_store.list_research_jobs(
                        player_id=player_id,
                        status="completed",
                        limit=80,
                    ),
                    "claimed": self.state_store.list_research_jobs(
                        player_id=player_id,
                        status="claimed",
                        limit=80,
                    ),
                },
            }

        base_compute_per_hour = 110.0
        faction_profile = self._player_faction_bonus_profile(player_id=player_id)
        faction_bonuses = faction_profile.get("bonuses", {})
        identity_profile = self._player_identity_modifier_profile(player_id=player_id)
        identity_modifiers = (
            identity_profile.get("modifiers", {})
            if isinstance(identity_profile.get("modifiers"), dict)
            else {}
        )
        faction_research_bonus_pct = (
            float(faction_bonuses.get("research_pct", 0.0))
            if isinstance(faction_bonuses, dict)
            and isinstance(faction_bonuses.get("research_pct"), (int, float))
            and not isinstance(faction_bonuses.get("research_pct"), bool)
            else 0.0
        )
        identity_research_bonus_pct = 0.0
        for key in ("research_pct", "research_production_pct"):
            raw = identity_modifiers.get(key)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                identity_research_bonus_pct += float(raw)
        identity_compute_bonus_pct = 0.0
        raw_compute_pct = identity_modifiers.get("compute_pct")
        if isinstance(raw_compute_pct, (int, float)) and not isinstance(raw_compute_pct, bool):
            identity_compute_bonus_pct += float(raw_compute_pct)
        raw_hacking_pct = identity_modifiers.get("hacking_pct")
        if isinstance(raw_hacking_pct, (int, float)) and not isinstance(raw_hacking_pct, bool):
            identity_compute_bonus_pct += float(raw_hacking_pct) * 0.22
        worlds = self.state_store.list_worlds_for_player(player_id=player_id)
        structures_index = self.seed_store.structure_index()
        research_bonus_pct = 0.0
        structure_count = 0
        total_population = 0.0
        for world in worlds:
            if not isinstance(world, dict):
                continue
            pop_current = world.get("population_current")
            if isinstance(pop_current, (int, float)) and not isinstance(pop_current, bool):
                total_population += max(0.0, float(pop_current))
            for structure_id in world.get("built_structures", []):
                if not isinstance(structure_id, str):
                    continue
                structure = structures_index.get(structure_id)
                if not isinstance(structure, dict):
                    continue
                structure_count += 1
                modifiers = structure.get("modifiers", {})
                if isinstance(modifiers, dict):
                    raw = modifiers.get("research_yield_pct", 0.0)
                    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                        research_bonus_pct += float(raw)

        assets = self.state_store.list_assets(player_id=player_id, asset_type=None, limit=400)
        module_index = self.seed_store.module_index()
        module_compute = 0.0
        for asset in assets:
            if asset.get("asset_type") != "module":
                continue
            asset_id = asset.get("asset_id")
            qty = int(asset.get("quantity", 0))
            if not isinstance(asset_id, str) or qty <= 0:
                continue
            module = module_index.get(asset_id)
            if not isinstance(module, dict):
                continue
            family = str(module.get("family", ""))
            tier = int(module.get("tier", 1))
            if family in {"special", "scanner", "relay", "reactor"}:
                module_compute += qty * (5.0 + (tier * 1.2))

        world_bonus = len(worlds) * 15.0
        structure_bonus = structure_count * 4.0
        population_bonus = min(400.0, total_population / 6000.0)
        subtotal = (
            base_compute_per_hour
            + world_bonus
            + structure_bonus
            + module_compute
            + population_bonus
        )
        total_research_bonus_pct = (
            research_bonus_pct + faction_research_bonus_pct + identity_research_bonus_pct
        )
        total_compute_bonus_pct = total_research_bonus_pct + identity_compute_bonus_pct
        total_compute = subtotal * (1.0 + (total_compute_bonus_pct / 100.0))

        active_jobs = self.state_store.list_research_jobs(
            player_id=player_id,
            status="active",
            limit=40,
        )
        completed_jobs = self.state_store.list_research_jobs(
            player_id=player_id,
            status="completed",
            limit=40,
        )
        claimed_jobs = self.state_store.list_research_jobs(
            player_id=player_id,
            status="claimed",
            limit=40,
        )

        return {
            "player_id": player_id,
            "compute_power_per_hour": round(total_compute, 3),
            "components": {
                "base": round(base_compute_per_hour, 3),
                "world_bonus": round(world_bonus, 3),
                "structure_bonus": round(structure_bonus, 3),
                "module_bonus": round(module_compute, 3),
                "population_bonus": round(population_bonus, 3),
                "research_yield_bonus_pct": round(research_bonus_pct, 3),
                "faction_research_bonus_pct": round(faction_research_bonus_pct, 3),
                "identity_research_bonus_pct": round(identity_research_bonus_pct, 3),
                "identity_compute_bonus_pct": round(identity_compute_bonus_pct, 3),
                "total_research_bonus_pct": round(total_research_bonus_pct, 3),
                "total_compute_bonus_pct": round(total_compute_bonus_pct, 3),
            },
            "faction_profile": faction_profile,
            "identity_profile": identity_profile,
            "research_jobs": {
                "active": active_jobs,
                "completed": completed_jobs,
                "claimed": claimed_jobs[:10],
            },
        }

    def _start_research_job(
        self,
        player_id: str,
        tech_id: str,
        substitution_id: str | None = None,
    ) -> dict[str, Any]:
        quote = self._crafting_quote(
            player_id=player_id,
            item_id=tech_id,
            quantity=1,
            world_id=None,
            substitution_id=substitution_id,
        )
        if quote["item_kind"] != "tech":
            raise ValueError("research/start only supports tech ids")
        if not quote["can_craft"]:
            reasons: list[str] = []
            if not quote["can_afford_credits"]:
                reasons.append("insufficient credits")
            if quote["missing_elements"]:
                reasons.append("insufficient elements")
            if quote["requirements"]["missing_tech"]:
                reasons.append("missing prerequisite tech")
            if quote["requirements"]["notes"]:
                reasons.extend(quote["requirements"]["notes"])
            if quote.get("storage_notes"):
                reasons.extend(
                    [str(note) for note in quote["storage_notes"] if isinstance(note, str)]
                )
            raise ValueError(f"Cannot start research for {tech_id}: " + "; ".join(reasons))

        tech_item = self.seed_store.tech_index().get(tech_id)
        if not isinstance(tech_item, dict):
            raise ValueError(f"Unknown tech id '{tech_id}'")
        rp_cost = int(tech_item.get("rp_cost", 100))
        tier = int(tech_item.get("tier", 1))
        baseline_seconds = int(tech_item.get("research_time_seconds", max(900, rp_cost // 2)))

        compute = self._player_compute_profile(player_id=player_id)
        compute_per_hour = float(compute["compute_power_per_hour"])
        compute_scalar = 0.12 + min(0.22, tier * 0.015)
        required_compute = float(rp_cost) * compute_scalar
        eta_from_compute = int(round((required_compute / max(1.0, compute_per_hour)) * 3600.0))
        duration_seconds = max(baseline_seconds, eta_from_compute)

        cost = quote["cost"]
        element_deltas = {
            row["symbol"]: -float(row["amount"])
            for row in cost["elements"]
        }
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id,
            credits_delta=-float(cost["credits"]),
            voidcoin_delta=0.0,
            element_deltas=element_deltas,
        )

        job = self.state_store.start_research_job(
            player_id=player_id,
            tech_id=tech_id,
            required_compute=required_compute,
            compute_power_per_hour=compute_per_hour,
            duration_seconds=duration_seconds,
            cost_payload=cost,
            substitution_id=substitution_id,
        )
        return {
            "player_id": player_id,
            "job": job,
            "wallet": resource_state["wallet"],
            "inventory_changes": resource_state["inventory"],
            "quote": quote,
            "compute_profile": compute,
        }

    def _claim_research_job(self, player_id: str, job_id: str) -> dict[str, Any]:
        job = self.state_store.claim_research_job(player_id=player_id, job_id=job_id)
        unlocked = self.state_store.list_unlocked_tech(player_id=player_id)
        return {
            "player_id": player_id,
            "job": job,
            "unlocked_tech_total": len(unlocked),
            "unlocked_latest": job["tech_id"] if job["status"] == "claimed" else None,
        }

    def _combat_effective_score(self, stats: dict[str, float]) -> float:
        attack_vec = (
            (stats["attack"] * 1.22)
            + (stats["scan"] * 0.25)
            + (stats["energy"] * 0.22)
            + (stats["cloak"] * 0.11)
        )
        defense_vec = (
            (stats["defense"] * 1.06)
            + (stats["hull"] * 0.47)
            + (stats["shield"] * 0.39)
        )
        return max(1.0, attack_vec + defense_vec)

    def _combat_power_level(self, stats: dict[str, float]) -> int:
        score = self._combat_effective_score(stats)
        # Soft mapping from effective score to a user-facing combat level band.
        level = int(round((max(1.0, score) ** 0.5) * 1.2))
        return max(1, min(120, level))

    def _combat_risk_profile(
        self,
        player_stats: dict[str, float],
        enemy_stats: dict[str, float],
    ) -> dict[str, Any]:
        odds = self._estimate_combat_odds(
            {
                "battle_id": f"risk.{uuid.uuid4().hex[:8]}",
                "attacker": {"name": "Player", "stats": player_stats},
                "defender": {"name": "Enemy", "stats": enemy_stats},
            }
        )
        player_score = self._combat_effective_score(player_stats)
        enemy_score = self._combat_effective_score(enemy_stats)
        player_level = self._combat_power_level(player_stats)
        enemy_level = self._combat_power_level(enemy_stats)
        level_gap = int(enemy_level) - int(player_level)
        win_probability = float(odds.get("attacker_win_probability", 0.5))
        return {
            "player_score": round(player_score, 4),
            "enemy_score": round(enemy_score, 4),
            "threat_ratio": round(enemy_score / max(1.0, player_score), 4),
            "power_ratio": round(player_score / max(1.0, enemy_score), 4),
            "player_level": int(player_level),
            "enemy_level": int(enemy_level),
            "level_gap": int(level_gap),
            "player_win_probability": round(win_probability, 4),
            "odds_recommendation": odds.get("recommendation"),
        }

    def _combat_reward_scaler(
        self,
        risk_profile: dict[str, Any],
        player_initiated_attack: bool,
    ) -> dict[str, Any]:
        level_gap = int(risk_profile.get("level_gap", 0))
        win_probability = float(risk_profile.get("player_win_probability", 0.5))
        reward_scale = 1.0
        gank_penalty_active = False
        underdog_bonus_active = False

        # Discourage farming weak targets: if player has >50% odds and higher level,
        # rewards drop sharply with mismatch.
        if level_gap <= -1 and win_probability > 0.5:
            gank_penalty_active = True
            dominance = (abs(level_gap) * 0.22) + max(0.0, win_probability - 0.5) * 1.35
            penalty = min(0.96, dominance)
            reward_scale *= max(0.04, 1.0 - penalty)

        # Reward intentional underdog engagements (higher risk => higher payout).
        if level_gap >= 1 and win_probability < 0.5:
            underdog_bonus_active = True
            bonus = (level_gap * 0.22) + ((0.5 - win_probability) * 2.3)
            if player_initiated_attack:
                bonus *= 1.2
            reward_scale *= 1.0 + min(2.9, bonus)

        if not gank_penalty_active and not underdog_bonus_active:
            reward_scale *= max(
                0.82,
                min(1.18, 1.0 + ((0.5 - win_probability) * 0.35)),
            )

        reward_scale = max(0.04, min(3.75, reward_scale))
        return {
            "reward_scale": round(reward_scale, 4),
            "gank_penalty_active": bool(gank_penalty_active),
            "underdog_bonus_active": bool(underdog_bonus_active),
            "player_initiated_attack": bool(player_initiated_attack),
            "win_probability_threshold": 0.5,
        }

    def _estimate_combat_odds(self, payload: dict[str, Any]) -> dict[str, Any]:
        attacker = payload["attacker"]
        defender = payload["defender"]
        a_stats = attacker["stats"]
        d_stats = defender["stats"]
        a_score = self._combat_effective_score(a_stats)
        d_score = self._combat_effective_score(d_stats)
        a_profiles = attacker.get("profiles", {})
        d_profiles = defender.get("profiles", {})
        if isinstance(a_profiles, dict) and isinstance(d_profiles, dict):
            a_damage = a_profiles.get("damage_profile", {})
            a_resist = a_profiles.get("resistance_profile", {})
            d_damage = d_profiles.get("damage_profile", {})
            d_resist = d_profiles.get("resistance_profile", {})
            if (
                isinstance(a_damage, dict)
                and isinstance(a_resist, dict)
                and isinstance(d_damage, dict)
                and isinstance(d_resist, dict)
            ):
                a_matchup = 0.0
                d_matchup = 0.0
                for dtype in DAMAGE_TYPES:
                    a_matchup += float(a_damage.get(dtype, 0.0)) * float(d_resist.get(dtype, 0.0))
                    d_matchup += float(d_damage.get(dtype, 0.0)) * float(a_resist.get(dtype, 0.0))
                a_mod = 1.0 - max(0.0, min(0.75, a_matchup))
                d_mod = 1.0 - max(0.0, min(0.75, d_matchup))
                a_score *= 0.74 + (0.52 * a_mod)
                d_score *= 0.74 + (0.52 * d_mod)
        spread = (a_score - d_score) / max(1.0, (a_score + d_score) * 0.18)
        attacker_win_prob = 1.0 / (1.0 + math.exp(-spread))
        defender_win_prob = 1.0 - attacker_win_prob
        likely_rounds = int(
            max(2, min(MAX_COMBAT_ROUNDS, round(5 + (abs(spread) * 3.2))))
        )
        return {
            "battle_id": payload.get("battle_id"),
            "attacker_name": attacker["name"],
            "defender_name": defender["name"],
            "attacker_score": round(a_score, 3),
            "defender_score": round(d_score, 3),
            "attacker_win_probability": round(attacker_win_prob, 4),
            "defender_win_probability": round(defender_win_prob, 4),
            "likely_rounds": likely_rounds,
            "recommendation": (
                "favorable" if attacker_win_prob >= 0.62 else "risky" if attacker_win_prob >= 0.43 else "avoid"
            ),
        }

    def _item_level_stat_multiplier(self, level: int, tier: int) -> float:
        bounded_level = max(1, min(40, int(level)))
        bounded_tier = max(1, min(12, int(tier)))
        growth = ((bounded_level - 1) ** 0.82) * (0.011 + ((bounded_tier - 1) * 0.0007))
        return max(1.0, min(1.75, 1.0 + growth))

    def _item_level_upgrade_cost(
        self,
        current_level: int,
        target_level: int,
        tier: int,
    ) -> dict[str, Any]:
        if target_level <= current_level:
            return {"credits": 0.0, "elements": []}
        tier_scale = max(1.0, 1.0 + ((max(1, tier) - 1) * 0.35))
        credits = 0.0
        elements: dict[str, float] = {"Fe": 0.0, "Si": 0.0, "Cu": 0.0}
        for level in range(max(1, current_level), target_level):
            step = level + 1
            base = (280.0 + (step * 110.0)) * tier_scale
            credits += base * 6.2
            elements["Fe"] += base * 0.12
            elements["Si"] += base * 0.06
            elements["Cu"] += base * 0.045
        rows = [
            {"symbol": symbol, "amount": round(amount, 3)}
            for symbol, amount in elements.items()
            if amount > 0
        ]
        return {"credits": round(credits, 4), "elements": rows}

    def _round_volume_m3(self, value: float) -> float:
        if abs(value) >= 1.0:
            return round(value, 1)
        return round(value, 3)

    def _module_equipment_volume_m3(
        self,
        module: dict[str, Any],
        family: str,
        deck_cost: float,
        module_level: int,
        quantity: int,
    ) -> tuple[float, str]:
        space_usage = module.get("space_usage", {})
        if isinstance(space_usage, dict):
            explicit = space_usage.get("equipment_volume_m3")
            if isinstance(explicit, (int, float)) and not isinstance(explicit, bool):
                volume = max(0.01, float(explicit)) * max(1, quantity)
                return volume, "explicit_module_space_usage"

        tier_raw = module.get("tier", 1)
        tier = int(tier_raw) if isinstance(tier_raw, int) else 1
        miniaturization = max(0.52, 1.0 - ((max(1, tier) - 1) * 0.028))
        family_scale = {
            "weapon_ballistic": 1.12,
            "weapon_missile": 1.16,
            "weapon_laser": 0.92,
            "weapon_plasma": 0.98,
            "weapon_railgun": 1.08,
            "shield": 1.04,
            "armor": 1.20,
            "reactor": 1.18,
            "relay": 0.72,
            "engine": 1.22,
            "scanner": 0.70,
            "jammer": 0.66,
            "utility": 0.94,
            "special": 0.84,
        }.get(family, 1.0)
        module_id = str(module.get("id", ""))
        if "quantum" in module_id or "photonic" in module_id:
            family_scale *= 0.58
        if "habitat" in module_id:
            family_scale *= 1.35
        if "microhangar" in module_id:
            family_scale *= 1.42
        if "nanite" in module_id:
            family_scale *= 0.72
        level_expansion = 1.0 + (max(0, module_level - 1) * 0.004)
        per_unit = max(
            0.04,
            float(deck_cost) * EQUIPMENT_M3_PER_DECK_POINT * family_scale * miniaturization * level_expansion,
        )
        return per_unit * max(1, quantity), "derived_from_deck_cost"

    def _compute_ship_space_model(
        self,
        *,
        base_stats: dict[str, Any],
        support_metrics: dict[str, float],
        normalized_modules: list[dict[str, Any]],
        module_index: dict[str, dict[str, Any]],
        deck_limit: float,
        deck_used: float,
        crew_assigned_total: float,
        crew_assigned_elite: float,
        passenger_assigned_total: float,
        cargo_load_tons: float,
    ) -> dict[str, Any]:
        equipment_capacity_raw = base_stats.get("equipment_volume_m3")
        if isinstance(equipment_capacity_raw, (int, float)) and not isinstance(
            equipment_capacity_raw, bool
        ):
            equipment_capacity_m3 = max(4.0, float(equipment_capacity_raw))
        else:
            equipment_capacity_m3 = max(4.0, deck_limit * EQUIPMENT_M3_PER_DECK_POINT)

        module_space_usage: list[dict[str, Any]] = []
        modules_equipment_used_m3 = 0.0
        for row in normalized_modules:
            module_id = row.get("id")
            if not isinstance(module_id, str):
                continue
            module = module_index.get(module_id)
            if not isinstance(module, dict):
                continue
            quantity_raw = row.get("quantity", 1)
            quantity = (
                int(quantity_raw)
                if isinstance(quantity_raw, int) and not isinstance(quantity_raw, bool)
                else 1
            )
            module_level_raw = row.get("level", 1)
            module_level = (
                int(module_level_raw)
                if isinstance(module_level_raw, int) and not isinstance(module_level_raw, bool)
                else 1
            )
            family = str(row.get("family", module.get("family", "utility")))
            deck_cost_raw = module.get("deck_cost", 1)
            deck_cost = (
                float(deck_cost_raw)
                if isinstance(deck_cost_raw, (int, float)) and not isinstance(deck_cost_raw, bool)
                else 1.0
            )
            equipment_volume_m3, source = self._module_equipment_volume_m3(
                module=module,
                family=family,
                deck_cost=deck_cost,
                module_level=module_level,
                quantity=quantity,
            )
            modules_equipment_used_m3 += equipment_volume_m3
            module_space_usage.append(
                {
                    "module_id": module_id,
                    "name": row.get("name", module_id),
                    "family": family,
                    "quantity": quantity,
                    "space_units": round(deck_cost * quantity, 3),
                    "equipment_volume_m3": self._round_volume_m3(equipment_volume_m3),
                    "volume_source": source,
                }
            )

        equipment_used_m3 = modules_equipment_used_m3
        equipment_free_m3 = equipment_capacity_m3 - equipment_used_m3
        equipment_utilization_ratio = equipment_used_m3 / max(0.001, equipment_capacity_m3)

        crew_capacity = max(0.0, float(support_metrics.get("crew_capacity", 0.0)))
        passenger_capacity = max(0.0, float(support_metrics.get("passenger_capacity", 0.0)))
        crew_required_min_raw = base_stats.get("crew_min", 0)
        crew_required_min = (
            float(crew_required_min_raw)
            if isinstance(crew_required_min_raw, (int, float)) and not isinstance(crew_required_min_raw, bool)
            else 0.0
        )
        habitable_capacity_raw = base_stats.get("habitable_volume_m3")
        if isinstance(habitable_capacity_raw, (int, float)) and not isinstance(
            habitable_capacity_raw, bool
        ):
            habitable_capacity_m3 = max(2.0, float(habitable_capacity_raw))
        else:
            habitable_capacity_m3 = max(
                6.0,
                (crew_capacity * HABITABLE_M3_PER_CREW)
                + (passenger_capacity * HABITABLE_M3_PER_PASSENGER)
                + max(8.0, float(base_stats.get("deck", 1)) * 1.8),
            )

        cargo_capacity_tons = max(0.0, float(support_metrics.get("cargo_capacity_tons", 0.0)))
        cargo_load_tons = max(0.0, float(cargo_load_tons))
        cargo_used_m3 = cargo_load_tons * CARGO_M3_PER_TON
        crew_used_m3 = (
            max(0.0, float(crew_assigned_total)) * HABITABLE_M3_PER_CREW
        ) + (
            max(0.0, float(passenger_assigned_total)) * HABITABLE_M3_PER_PASSENGER
        )
        habitable_used_m3 = crew_used_m3 + (cargo_used_m3 * 0.02)
        habitable_free_m3 = habitable_capacity_m3 - habitable_used_m3
        habitable_utilization_ratio = habitable_used_m3 / max(0.001, habitable_capacity_m3)

        return {
            "space_units": {
                "used": round(deck_used, 3),
                "total": round(deck_limit, 3),
                "free": round(max(0.0, deck_limit - deck_used), 3),
                "utilization_ratio": round(deck_used / max(0.001, deck_limit), 4),
            },
            "equipment_capacity_m3": self._round_volume_m3(equipment_capacity_m3),
            "equipment_used_m3": self._round_volume_m3(equipment_used_m3),
            "equipment_free_m3": self._round_volume_m3(equipment_free_m3),
            "equipment_utilization_ratio": round(equipment_utilization_ratio, 4),
            "habitable_capacity_m3": self._round_volume_m3(habitable_capacity_m3),
            "habitable_used_m3": self._round_volume_m3(habitable_used_m3),
            "habitable_free_m3": self._round_volume_m3(habitable_free_m3),
            "habitable_utilization_ratio": round(habitable_utilization_ratio, 4),
            "crew_required_min": round(crew_required_min, 3),
            "crew_assigned_total": round(max(0.0, float(crew_assigned_total)), 3),
            "crew_assigned_elite": round(max(0.0, float(crew_assigned_elite)), 3),
            "crew_capacity": round(crew_capacity, 3),
            "crew_occupancy_ratio": round(
                max(0.0, float(crew_assigned_total)) / max(1.0, crew_capacity),
                4,
            ),
            "crew_shortfall": round(max(0.0, crew_required_min - float(crew_assigned_total)), 3),
            "passenger_assigned_total": round(max(0.0, float(passenger_assigned_total)), 3),
            "passenger_capacity": round(passenger_capacity, 3),
            "cargo_load_tons": round(cargo_load_tons, 3),
            "cargo_capacity_tons": round(cargo_capacity_tons, 3),
            "module_space_usage": module_space_usage,
        }

    def _simulate_fitting(self, payload: dict[str, Any]) -> dict[str, Any]:
        hull_id = payload.get("hull_id")
        modules = payload.get("modules")
        player_id = payload.get("player_id")
        enforce_owned = bool(payload.get("enforce_owned", False))
        enemy = payload.get("enemy")
        hull_level = payload.get("hull_level", 1)
        runs = payload.get("runs", 80)
        crew_assigned_total_raw = payload.get("crew_assigned_total")
        crew_assigned_elite_raw = payload.get("crew_assigned_elite")
        passenger_assigned_total_raw = payload.get("passenger_assigned_total")
        cargo_load_tons_raw = payload.get("cargo_load_tons", 0.0)
        seed_raw = payload.get("seed", int(time.time()))
        try:
            seed = int(seed_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("seed must be coercible to integer") from exc
        if isinstance(hull_level, bool) or not isinstance(hull_level, int):
            raise ValueError("hull_level must be an integer")
        if hull_level <= 0 or hull_level > 40:
            raise ValueError("hull_level must be between 1 and 40")
        if isinstance(runs, bool) or not isinstance(runs, int):
            raise ValueError("runs must be an integer")
        if runs <= 0 or runs > 300:
            raise ValueError("runs must be between 1 and 300")
        if crew_assigned_total_raw is not None and (
            isinstance(crew_assigned_total_raw, bool)
            or not isinstance(crew_assigned_total_raw, (int, float))
        ):
            raise ValueError("crew_assigned_total must be numeric when provided")
        if crew_assigned_elite_raw is not None and (
            isinstance(crew_assigned_elite_raw, bool)
            or not isinstance(crew_assigned_elite_raw, (int, float))
        ):
            raise ValueError("crew_assigned_elite must be numeric when provided")
        if passenger_assigned_total_raw is not None and (
            isinstance(passenger_assigned_total_raw, bool)
            or not isinstance(passenger_assigned_total_raw, (int, float))
        ):
            raise ValueError("passenger_assigned_total must be numeric when provided")
        if isinstance(cargo_load_tons_raw, bool) or not isinstance(cargo_load_tons_raw, (int, float)):
            raise ValueError("cargo_load_tons must be numeric")
        if not isinstance(hull_id, str) or not hull_id.strip():
            raise ValueError("hull_id must be a non-empty string")
        if not isinstance(modules, list):
            raise ValueError("modules must be an array")
        if player_id is not None and not isinstance(player_id, str):
            raise ValueError("player_id must be a string when provided")

        hull = self.seed_store.hull_index().get(hull_id.strip())
        if not isinstance(hull, dict):
            raise ValueError(f"Unknown hull_id '{hull_id}'")
        base_stats = hull.get("base_stats", {})
        if not isinstance(base_stats, dict):
            raise ValueError(f"Hull '{hull_id}' does not define base_stats")
        module_index = self.seed_store.module_index()

        slot_map = {
            "weapon_ballistic": "weapon",
            "weapon_laser": "weapon",
            "weapon_missile": "weapon",
            "weapon_plasma": "weapon",
            "weapon_railgun": "weapon",
            "armor": "defense",
            "shield": "defense",
            "jammer": "defense",
            "reactor": "power",
            "engine": "utility",
            "scanner": "utility",
            "relay": "utility",
            "utility": "utility",
            "special": "special",
        }
        support_bonus_key_map: dict[str, str] = {
            "cargo": "cargo",
            "cargo_capacity_tons": "cargo_capacity_tons",
            "crew_capacity": "crew_capacity",
            "passenger_capacity": "passenger_capacity",
            "heat_dissipation": "heat_dissipation",
            "heat_dissipation_pct": "heat_dissipation_pct",
            "sensor_lock": "sensor_lock",
            "missile_guidance": "missile_guidance",
            "ecm_resistance": "ecm_resistance",
            "compute": "compute",
            "market_efficiency": "market_efficiency",
            "mining_yield": "mining_yield",
            "fighter_bay": "fighter_bay",
            "launch_tube": "launch_tube",
            "repair_rate_pct": "repair_rate_pct",
            "stealth_signature_pct": "stealth_signature_pct",
            "corrosion_resistance": "corrosion_resistance",
            "thrust_kn": "thrust_kn",
            "action_energy_max": "action_energy_max",
            "action_energy_regen": "action_energy_regen",
        }
        damage_map_by_family: dict[str, dict[str, float]] = {
            "weapon_laser": {"thermal": 0.72, "ion": 0.28},
            "weapon_plasma": {"plasma": 0.68, "thermal": 0.32},
            "weapon_missile": {"explosive": 0.72, "kinetic": 0.28},
            "weapon_railgun": {"kinetic": 0.88, "ion": 0.12},
            "weapon_ballistic": {"kinetic": 0.82, "explosive": 0.18},
        }
        weapon_attack_tuning: dict[str, float] = {
            "weapon_laser": 1.0,
            "weapon_plasma": 0.95,
            "weapon_missile": 1.04,
            "weapon_railgun": 0.92,
            "weapon_ballistic": 1.26,
        }
        resistance_add_by_family: dict[str, dict[str, float]] = {
            "shield": {"thermal": 0.045, "plasma": 0.04, "ion": 0.035, "explosive": 0.01},
            "armor": {"kinetic": 0.05, "explosive": 0.04, "thermal": 0.015},
            "jammer": {"explosive": 0.03, "ion": 0.03, "kinetic": 0.01},
            "utility": {"kinetic": 0.006, "thermal": 0.006, "explosive": 0.006, "plasma": 0.006, "ion": 0.006},
            "special": {"plasma": 0.02, "ion": 0.02, "thermal": 0.01},
        }
        slot_limits_raw = hull.get("module_slots", {})
        slot_limits = {
            key: int(value)
            for key, value in slot_limits_raw.items()
            if isinstance(key, str) and isinstance(value, int)
        } if isinstance(slot_limits_raw, dict) else {}
        slot_usage: dict[str, int] = {}
        hull_tier_raw = hull.get("tier", 1)
        hull_tier = int(hull_tier_raw) if isinstance(hull_tier_raw, int) else 1
        hull_level_mult = self._item_level_stat_multiplier(
            level=hull_level,
            tier=hull_tier,
        )
        deck_limit = float(base_stats.get("deck", 0)) * (1.0 + ((hull_level - 1) * 0.01))
        deck_used = 0.0
        power_draw = 0.0
        power_generation = (
            float(base_stats.get("energy", 0.0)) * (1.0 + ((hull_level - 1) * 0.015))
        )
        merged_stats: dict[str, float] = {}
        for key in STAT_KEYS:
            raw = base_stats.get(key, 0.0)
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                merged_stats[key] = float(raw) * hull_level_mult
            else:
                merged_stats[key] = 0.0
        damage_weights: dict[str, float] = {dtype: 0.0 for dtype in DAMAGE_TYPES}
        base_resistance = 0.055 + min(0.12, hull_tier * 0.0085)
        resistance_values: dict[str, float] = {
            dtype: base_resistance for dtype in DAMAGE_TYPES
        }
        support_metrics: dict[str, float] = {
            "cargo": merged_stats.get("cargo", float(base_stats.get("cargo", 0.0)) * hull_level_mult),
            "cargo_capacity_tons": float(base_stats.get("cargo_capacity_tons", 0.0)) * hull_level_mult,
            "crew_capacity": float(base_stats.get("crew_capacity", 0.0)) * hull_level_mult,
            "passenger_capacity": float(base_stats.get("passenger_capacity", 0.0)) * hull_level_mult,
            "heat_dissipation": 12.0 + (float(base_stats.get("thrust_kn", 0.0)) * 0.004) + (hull_tier * 1.8),
            "heat_dissipation_pct": 0.0,
            "sensor_lock": 0.0,
            "missile_guidance": 0.0,
            "ecm_resistance": 0.0,
            "compute": 0.0,
            "market_efficiency": 0.0,
            "mining_yield": 0.0,
            "fighter_bay": 0.0,
            "launch_tube": 0.0,
            "repair_rate_pct": 0.0,
            "stealth_signature_pct": 0.0,
            "corrosion_resistance": 0.0,
            "thrust_kn": float(base_stats.get("thrust_kn", 0.0)),
            "action_energy_max": 0.0,
            "action_energy_regen": 0.0,
        }
        family_counts: dict[str, int] = {}
        heat_generation_per_second = 0.0
        antimatter_module_count = 0

        normalized_modules: list[dict[str, Any]] = []
        violations: list[str] = []
        for idx, row in enumerate(modules):
            module_id: str | None = None
            quantity = 1
            module_level = 1
            instance_id = None
            if isinstance(row, str):
                module_id = row
            elif isinstance(row, dict):
                candidate_id = row.get("id", row.get("module_id"))
                if isinstance(candidate_id, str):
                    module_id = candidate_id
                qty_raw = row.get("quantity", 1)
                if isinstance(qty_raw, bool) or not isinstance(qty_raw, int):
                    raise ValueError(f"modules[{idx}].quantity must be an integer")
                if qty_raw <= 0:
                    raise ValueError(f"modules[{idx}].quantity must be > 0")
                quantity = qty_raw
                level_raw = row.get("level", 1)
                if isinstance(level_raw, bool) or not isinstance(level_raw, int):
                    raise ValueError(f"modules[{idx}].level must be an integer")
                if level_raw <= 0 or level_raw > 40:
                    raise ValueError(f"modules[{idx}].level must be between 1 and 40")
                module_level = level_raw
                raw_instance = row.get("instance_id")
                if raw_instance is not None and not isinstance(raw_instance, str):
                    raise ValueError(f"modules[{idx}].instance_id must be a string when provided")
                instance_id = raw_instance.strip() if isinstance(raw_instance, str) else None
            else:
                raise ValueError(f"modules[{idx}] must be a string or object")

            if not isinstance(module_id, str) or not module_id.strip():
                raise ValueError(f"modules[{idx}] does not include a valid module id")
            module = module_index.get(module_id.strip())
            if not isinstance(module, dict):
                raise ValueError(f"Unknown module id '{module_id}'")
            if instance_id:
                if not isinstance(player_id, str) or not player_id.strip():
                    raise ValueError(
                        f"modules[{idx}].instance_id requires player_id in the request"
                    )
                instance = self.state_store.get_crafted_instance(
                    player_id=player_id.strip(),
                    instance_id=instance_id,
                )
                if instance.get("asset_type") != "module" or instance.get("asset_id") != module_id.strip():
                    raise ValueError(
                        f"modules[{idx}].instance_id does not match module '{module_id.strip()}'"
                    )
                payload_level = instance.get("item_level")
                if isinstance(payload_level, int):
                    module_level = payload_level
            family = str(module.get("family", "utility"))
            family_counts[family] = family_counts.get(family, 0) + quantity
            if "antimatter" in module_id.casefold():
                antimatter_module_count += quantity
            slot_key = slot_map.get(family, "utility")
            slot_usage[slot_key] = slot_usage.get(slot_key, 0) + quantity
            tier_raw = module.get("tier", 1)
            tier = int(tier_raw) if isinstance(tier_raw, int) else 1
            level_mult = self._item_level_stat_multiplier(level=module_level, tier=tier)
            deck_cost = module.get("deck_cost", 0)
            if isinstance(deck_cost, (int, float)) and not isinstance(deck_cost, bool):
                deck_used += float(deck_cost) * quantity
            draw = module.get("power_draw", 0)
            gen = module.get("power_generation", 0)
            if isinstance(draw, (int, float)) and not isinstance(draw, bool):
                power_draw += float(draw) * quantity * (1.0 + ((module_level - 1) * 0.015))
            if isinstance(gen, (int, float)) and not isinstance(gen, bool):
                power_generation += float(gen) * quantity * (1.0 + ((module_level - 1) * 0.012))
            heat_cycle = module.get("heat_per_cycle", 0)
            cycle_seconds = module.get("cycle_seconds", 1.0)
            if (
                isinstance(heat_cycle, (int, float))
                and not isinstance(heat_cycle, bool)
                and isinstance(cycle_seconds, (int, float))
                and not isinstance(cycle_seconds, bool)
                and float(cycle_seconds) > 0
            ):
                heat_generation_per_second += (
                    (float(heat_cycle) / max(0.2, float(cycle_seconds)))
                    * quantity
                    * (1.0 + ((module_level - 1) * 0.014))
                )
            bonuses = module.get("stat_bonuses", {})
            attack_bonus = 0.0
            if isinstance(bonuses, dict):
                for key in STAT_KEYS:
                    raw = bonuses.get(key)
                    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                        stat_value = float(raw)
                        if key == "attack":
                            attack_bonus = stat_value
                            if family.startswith("weapon_"):
                                # Compress low-tier weapon spread so same-level loadouts are not near-deterministic.
                                stat_value = (max(0.0, stat_value) * 0.30) + 8.0
                                stat_value *= weapon_attack_tuning.get(family, 1.0)
                                attack_bonus = stat_value
                        merged_stats[key] = merged_stats.get(key, 0.0) + (
                            stat_value * quantity * level_mult
                        )
                for bonus_key, support_key in support_bonus_key_map.items():
                    raw_support = bonuses.get(bonus_key)
                    if isinstance(raw_support, (int, float)) and not isinstance(raw_support, bool):
                        support_metrics[support_key] = support_metrics.get(support_key, 0.0) + (
                            float(raw_support) * quantity * level_mult
                        )
            family_damage = damage_map_by_family.get(family)
            if isinstance(family_damage, dict):
                weapon_weight = max(1.0, attack_bonus if attack_bonus > 0 else (tier * 2.2))
                weapon_weight *= quantity * max(1.0, (0.82 + (0.18 * level_mult)))
                for dtype, frac in family_damage.items():
                    damage_weights[dtype] = damage_weights.get(dtype, 0.0) + (weapon_weight * frac)
            family_resist = resistance_add_by_family.get(family)
            if isinstance(family_resist, dict):
                resist_scale = quantity * (1.0 + ((module_level - 1) * 0.018))
                for dtype, value in family_resist.items():
                    resistance_values[dtype] = resistance_values.get(dtype, base_resistance) + (
                        value * resist_scale
                    )
            normalized_modules.append(
                {
                    "id": module_id.strip(),
                    "name": module.get("name", module_id.strip()),
                    "family": family,
                    "slot": slot_key,
                    "quantity": quantity,
                    "level": module_level,
                    "instance_id": instance_id,
                    "stat_multiplier_from_level": round(level_mult, 4),
                }
            )

        crew_required_min_raw = base_stats.get("crew_min", 0)
        crew_required_min = (
            float(crew_required_min_raw)
            if isinstance(crew_required_min_raw, (int, float)) and not isinstance(crew_required_min_raw, bool)
            else 0.0
        )
        crew_capacity = max(0.0, float(support_metrics.get("crew_capacity", 0.0)))
        passenger_capacity = max(0.0, float(support_metrics.get("passenger_capacity", 0.0)))
        crew_assigned_total = (
            float(crew_assigned_total_raw)
            if isinstance(crew_assigned_total_raw, (int, float)) and not isinstance(crew_assigned_total_raw, bool)
            else max(crew_required_min, min(crew_capacity, crew_required_min))
        )
        crew_assigned_elite = (
            float(crew_assigned_elite_raw)
            if isinstance(crew_assigned_elite_raw, (int, float)) and not isinstance(crew_assigned_elite_raw, bool)
            else min(crew_assigned_total, max(0.0, round(crew_assigned_total * 0.12, 3)))
        )
        passenger_assigned_total = (
            float(passenger_assigned_total_raw)
            if isinstance(passenger_assigned_total_raw, (int, float))
            and not isinstance(passenger_assigned_total_raw, bool)
            else 0.0
        )
        cargo_load_tons = max(0.0, float(cargo_load_tons_raw))

        for slot_key, used in slot_usage.items():
            allowed = int(slot_limits.get(slot_key, 0))
            if used > allowed:
                violations.append(f"slot '{slot_key}' exceeded ({used}/{allowed})")
        if deck_limit > 0 and deck_used > deck_limit + 1e-9:
            violations.append(
                "deck usage exceeded ({:.1f}/{:.1f})".format(deck_used, deck_limit)
            )
        if power_draw > power_generation + 1e-9:
            violations.append(
                "power deficit ({:.2f} draw > {:.2f} generation)".format(
                    power_draw,
                    power_generation,
                )
            )

        if enforce_owned:
            if not isinstance(player_id, str) or not player_id.strip():
                raise ValueError("player_id is required when enforce_owned is true")
            self._ensure_player_bootstrap(player_id.strip())
            owned_hulls = self.state_store.list_assets(
                player_id=player_id.strip(),
                asset_type="hull",
                limit=160,
            )
            if not any(
                isinstance(row, dict)
                and row.get("asset_id") == hull_id.strip()
                and int(row.get("quantity", 0)) > 0
                for row in owned_hulls
            ):
                violations.append(f"player does not own hull '{hull_id.strip()}'")
            owned_modules = self.state_store.list_assets(
                player_id=player_id.strip(),
                asset_type="module",
                limit=600,
            )
            owned_lookup = {
                str(row.get("asset_id")): int(row.get("quantity", 0))
                for row in owned_modules
                if isinstance(row, dict) and isinstance(row.get("asset_id"), str)
            }
            required: dict[str, int] = {}
            for row in normalized_modules:
                required[row["id"]] = required.get(row["id"], 0) + int(row["quantity"])
            for module_id, qty_needed in required.items():
                qty_owned = int(owned_lookup.get(module_id, 0))
                if qty_owned < qty_needed:
                    violations.append(
                        f"player does not own enough '{module_id}' ({qty_owned}/{qty_needed})"
                    )

        if sum(damage_weights.values()) <= 1e-9:
            fallback = {"kinetic": 0.4, "thermal": 0.2, "explosive": 0.15, "plasma": 0.15, "ion": 0.1}
            for dtype, frac in fallback.items():
                damage_weights[dtype] = frac * max(1.0, merged_stats.get("attack", 1.0))
        damage_total = sum(damage_weights.values())
        damage_profile = {
            dtype: (damage_weights[dtype] / damage_total) if damage_total > 1e-9 else (1.0 / len(DAMAGE_TYPES))
            for dtype in DAMAGE_TYPES
        }
        resistance_profile = {
            dtype: max(0.0, min(0.8, resistance_values[dtype]))
            for dtype in DAMAGE_TYPES
        }

        enemy_stats = {
            "attack": 180.0,
            "defense": 175.0,
            "hull": 640.0,
            "shield": 260.0,
            "energy": 390.0,
            "scan": 72.0,
            "cloak": 42.0,
        }
        enemy_profiles = {
            "damage_profile": self._normalize_damage_profile(None, "enemy.profiles.damage_profile"),
            "resistance_profile": self._normalize_resistance_profile(None, "enemy.profiles.resistance_profile"),
        }
        enemy_name = "Simulated Opponent"
        if isinstance(enemy, dict):
            raw_stats = enemy.get("stats")
            if isinstance(enemy.get("name"), str) and enemy["name"].strip():
                enemy_name = enemy["name"].strip()
            if isinstance(raw_stats, dict):
                for key in STAT_KEYS:
                    raw = raw_stats.get(key, enemy_stats[key])
                    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                        raise ValueError(f"enemy.stats.{key} must be numeric")
                    enemy_stats[key] = max(1.0, float(raw))
            elif isinstance(enemy.get("hull_id"), str):
                ref_hull = self.seed_store.hull_index().get(enemy["hull_id"])
                if isinstance(ref_hull, dict) and isinstance(ref_hull.get("base_stats"), dict):
                    enemy_name = str(ref_hull.get("name", enemy_name))
                    ref_base = ref_hull["base_stats"]
                    for key in STAT_KEYS:
                        raw = ref_base.get(key)
                        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                            enemy_stats[key] = max(1.0, float(raw))
            raw_profiles = enemy.get("profiles")
            if raw_profiles is not None:
                if not isinstance(raw_profiles, dict):
                    raise ValueError("enemy.profiles must be an object when provided")
                enemy_profiles = {
                    "damage_profile": self._normalize_damage_profile(
                        raw_profiles.get("damage_profile"),
                        "enemy.profiles.damage_profile",
                    ),
                    "resistance_profile": self._normalize_resistance_profile(
                        raw_profiles.get("resistance_profile"),
                        "enemy.profiles.resistance_profile",
                    ),
                }

        power_net = power_generation - power_draw
        power_deficit_ratio = 0.0
        if power_net < 0:
            power_deficit_ratio = min(0.65, abs(power_net) / max(1.0, power_generation))
            merged_stats["attack"] *= max(0.42, 1.0 - (power_deficit_ratio * 0.55))
            merged_stats["defense"] *= max(0.52, 1.0 - (power_deficit_ratio * 0.42))
            merged_stats["shield"] *= max(0.38, 1.0 - (power_deficit_ratio * 0.7))
            merged_stats["energy"] *= max(0.35, 1.0 - (power_deficit_ratio * 0.72))

        heat_dissipation_per_second = max(
            0.1,
            support_metrics["heat_dissipation"]
            * (1.0 + (support_metrics["heat_dissipation_pct"] / 100.0)),
        )
        heat_margin_per_second = heat_dissipation_per_second - heat_generation_per_second
        thermal_load_ratio = heat_generation_per_second / max(0.1, heat_dissipation_per_second)
        thermal_state = "stable"
        if thermal_load_ratio >= 1.30:
            thermal_state = "critical"
        elif thermal_load_ratio >= 1.08:
            thermal_state = "overheated"
        elif thermal_load_ratio >= 0.78:
            thermal_state = "warm"
        if thermal_load_ratio > 1.0:
            overheat_ratio = min(0.55, (thermal_load_ratio - 1.0) * 0.58)
            merged_stats["attack"] *= max(0.4, 1.0 - (overheat_ratio * 0.72))
            merged_stats["defense"] *= max(0.45, 1.0 - (overheat_ratio * 0.62))
            merged_stats["shield"] *= max(0.36, 1.0 - (overheat_ratio * 0.78))
            merged_stats["energy"] *= max(0.32, 1.0 - (overheat_ratio * 0.82))
            if thermal_load_ratio >= 1.25:
                violations.append(
                    "thermal overload ({:.2f} gen > {:.2f} dissipation)".format(
                        heat_generation_per_second,
                        heat_dissipation_per_second,
                    )
                )

        weapon_module_count = sum(
            count
            for family, count in family_counts.items()
            if isinstance(family, str) and family.startswith("weapon_")
        )
        ship_effects: list[dict[str, Any]] = []
        if heat_margin_per_second >= 20.0:
            ship_effects.append(
                {
                    "id": "effect.thermal_headroom",
                    "type": "buff",
                    "magnitude": round(min(60.0, heat_margin_per_second), 3),
                    "detail": "Heat dissipation exceeds generation; sustained fire is safer.",
                }
            )
        if support_metrics["fighter_bay"] >= 8.0:
            ship_effects.append(
                {
                    "id": "effect.carrier_wing_control",
                    "type": "buff",
                    "magnitude": round(support_metrics["fighter_bay"], 3),
                    "detail": "Microhangar systems enable deployable support craft.",
                }
            )
        if support_metrics["missile_guidance"] >= 18.0:
            ship_effects.append(
                {
                    "id": "effect.precision_salvo",
                    "type": "buff",
                    "magnitude": round(support_metrics["missile_guidance"], 3),
                    "detail": "Guidance stack improves missile hit quality.",
                }
            )
        if support_metrics["sensor_lock"] >= 20.0:
            ship_effects.append(
                {
                    "id": "effect.target_fusion",
                    "type": "buff",
                    "magnitude": round(support_metrics["sensor_lock"], 3),
                    "detail": "Target-fusion pipeline improves lock retention and firing solutions.",
                }
            )
        if support_metrics["compute"] >= 20.0:
            ship_effects.append(
                {
                    "id": "effect.battle_predictor",
                    "type": "buff",
                    "magnitude": round(support_metrics["compute"], 3),
                    "detail": "Onboard compute improves tactical adaptation and timing windows.",
                }
            )
        if support_metrics["action_energy_max"] >= 20.0:
            ship_effects.append(
                {
                    "id": "effect.endurance_reservoir",
                    "type": "buff",
                    "magnitude": round(support_metrics["action_energy_max"], 3),
                    "detail": "Expanded action-energy reservoirs increase gameplay stamina for scans, missions, and engagements.",
                }
            )
        if support_metrics["action_energy_regen"] >= 2.0:
            ship_effects.append(
                {
                    "id": "effect.endurance_recovery",
                    "type": "buff",
                    "magnitude": round(support_metrics["action_energy_regen"], 3),
                    "detail": "Recovery systems accelerate action-energy regeneration over time.",
                }
            )
        if support_metrics["market_efficiency"] >= 12.0:
            ship_effects.append(
                {
                    "id": "effect.market_optimizer",
                    "type": "buff",
                    "magnitude": round(support_metrics["market_efficiency"], 3),
                    "detail": "Trade suite improves quote execution and route profitability.",
                }
            )
        if support_metrics["mining_yield"] >= 10.0:
            ship_effects.append(
                {
                    "id": "effect.extraction_uplift",
                    "type": "buff",
                    "magnitude": round(support_metrics["mining_yield"], 3),
                    "detail": "Mining refinement package increases extractable output per operation.",
                }
            )
        if thermal_load_ratio > 1.0:
            ship_effects.append(
                {
                    "id": "effect.thermal_saturation",
                    "type": "debuff",
                    "magnitude": round(thermal_load_ratio, 3),
                    "detail": "Thermal load exceeds dissipation; combat stats degrade under sustained operation.",
                }
            )
        if power_deficit_ratio > 0.0:
            ship_effects.append(
                {
                    "id": "effect.power_starved",
                    "type": "debuff",
                    "magnitude": round(power_deficit_ratio, 3),
                    "detail": "Power deficit reduces shield stability and offensive throughput.",
                }
            )
        if support_metrics["stealth_signature_pct"] > 8.0:
            ship_effects.append(
                {
                    "id": "effect.signature_bloom",
                    "type": "debuff",
                    "magnitude": round(support_metrics["stealth_signature_pct"], 3),
                    "detail": "Emissions and active hardware increase detectability while operating.",
                }
            )
        if antimatter_module_count > 0:
            ship_effects.append(
                {
                    "id": "effect.antimatter_containment_risk",
                    "type": "debuff",
                    "magnitude": round(float(antimatter_module_count), 3),
                    "detail": "Antimatter modules increase catastrophic failure risk when heavily damaged.",
                }
            )
        if weapon_module_count >= 4 and family_counts.get("jammer", 0) <= 0:
            ship_effects.append(
                {
                    "id": "effect.emissions_trace",
                    "type": "debuff",
                    "magnitude": round(float(weapon_module_count), 3),
                    "detail": "Heavy weapon profile without jamming support leaves a stronger tracking signature.",
                }
            )

        mass_tons = float(base_stats.get("mass_tons", 1.0))
        maneuver_index = support_metrics["thrust_kn"] / max(1.0, mass_tons)
        role_scores: dict[str, float] = {
            "combat": (
                (merged_stats["attack"] * 1.22)
                + (merged_stats["defense"] * 1.05)
                + (merged_stats["hull"] * 0.34)
                + (merged_stats["shield"] * 0.30)
            ),
            "carrier": (
                (support_metrics["fighter_bay"] * 38.0)
                + (support_metrics["launch_tube"] * 22.0)
                + (merged_stats["defense"] * 0.68)
                + (merged_stats["energy"] * 0.45)
            ),
            "cargo": (
                (support_metrics["cargo_capacity_tons"] * 0.12)
                + (support_metrics["cargo"] * 4.4)
                + (support_metrics["market_efficiency"] * 36.0)
            ),
            "exploration": (
                (merged_stats["scan"] * 3.2)
                + (merged_stats["cloak"] * 2.4)
                + (support_metrics["sensor_lock"] * 1.2)
                + (support_metrics["action_energy_regen"] * 18.0)
                + (maneuver_index * 420.0)
            ),
            "mining": (
                (support_metrics["mining_yield"] * 62.0)
                + (support_metrics["cargo_capacity_tons"] * 0.07)
                + (support_metrics["heat_dissipation"] * 2.8)
            ),
            "market": (
                (support_metrics["market_efficiency"] * 78.0)
                + (support_metrics["compute"] * 9.0)
                + (support_metrics["cargo"] * 1.7)
            ),
            "science": (
                (support_metrics["compute"] * 55.0)
                + (merged_stats["scan"] * 1.9)
                + (support_metrics["sensor_lock"] * 1.1)
                + (support_metrics["action_energy_max"] * 1.2)
            ),
            "transport": (
                (support_metrics["crew_capacity"] * 0.32)
                + (support_metrics["passenger_capacity"] * 0.56)
                + (merged_stats["defense"] * 0.8)
            ),
            "interceptor": (
                (maneuver_index * 640.0)
                + (merged_stats["attack"] * 0.9)
                + (support_metrics["sensor_lock"] * 0.8)
            ),
        }
        sorted_roles = sorted(role_scores.items(), key=lambda row: row[1], reverse=True)
        primary_role = sorted_roles[0][0] if sorted_roles else "combat"
        secondary_roles = [row[0] for row in sorted_roles[1:4]]

        ship_space = self._compute_ship_space_model(
            base_stats=base_stats,
            support_metrics=support_metrics,
            normalized_modules=normalized_modules,
            module_index=module_index,
            deck_limit=deck_limit,
            deck_used=deck_used,
            crew_assigned_total=crew_assigned_total,
            crew_assigned_elite=crew_assigned_elite,
            passenger_assigned_total=passenger_assigned_total,
            cargo_load_tons=cargo_load_tons,
        )
        if float(ship_space["equipment_utilization_ratio"]) > 1.0 + 1e-9:
            violations.append(
                "equipment volume exceeded ({:.1f}/{:.1f} m3)".format(
                    float(ship_space["equipment_used_m3"]),
                    float(ship_space["equipment_capacity_m3"]),
                )
            )
        if float(ship_space["habitable_utilization_ratio"]) > 1.0 + 1e-9:
            violations.append(
                "habitable volume exceeded ({:.1f}/{:.1f} m3)".format(
                    float(ship_space["habitable_used_m3"]),
                    float(ship_space["habitable_capacity_m3"]),
                )
            )
        if crew_assigned_total + 1e-9 < crew_required_min:
            violations.append(
                "crew assigned below minimum ({:.1f}/{:.1f})".format(
                    crew_assigned_total,
                    crew_required_min,
                )
            )
        if crew_assigned_total > crew_capacity + 1e-9:
            violations.append(
                "crew assigned exceeds capacity ({:.1f}/{:.1f})".format(
                    crew_assigned_total,
                    crew_capacity,
                )
            )
        if passenger_assigned_total > passenger_capacity + 1e-9:
            violations.append(
                "passengers exceed capacity ({:.1f}/{:.1f})".format(
                    passenger_assigned_total,
                    passenger_capacity,
                )
            )
        cargo_capacity_tons = float(ship_space.get("cargo_capacity_tons", 0.0))
        if cargo_load_tons > cargo_capacity_tons + 1e-9:
            violations.append(
                "cargo load exceeds capacity ({:.1f}/{:.1f} tons)".format(
                    cargo_load_tons,
                    cargo_capacity_tons,
                )
            )

        can_fit = len(violations) == 0
        normalized_payload = self._normalize_combat_payload(
            {
                "attacker": {
                    "name": "Fitting Candidate",
                    "stats": merged_stats,
                    "profiles": {
                        "damage_profile": damage_profile,
                        "resistance_profile": resistance_profile,
                    },
                },
                "defender": {
                    "name": enemy_name,
                    "stats": enemy_stats,
                    "profiles": enemy_profiles,
                },
                "context": {"mode": "pvp", "max_rounds": 9, "seed": seed},
            }
        )
        odds = self._estimate_combat_odds(normalized_payload)

        simulations: list[dict[str, Any]] = []
        win = 0
        loss = 0
        draw = 0
        rounds_total = 0
        ttk_rounds_total = 0
        ttk_samples = 0
        for idx in range(runs):
            sim_payload = {
                "battle_id": f"fit.{uuid.uuid4().hex[:10]}",
                "attacker": normalized_payload["attacker"],
                "defender": normalized_payload["defender"],
                "context": dict(normalized_payload["context"]),
            }
            sim_payload["context"]["seed"] = seed + idx
            result = self._simulate_combat(sim_payload)
            if idx < 3:
                simulations.append(result)
            rounds_total += int(result["rounds_fought"])
            if result["winner"] == "attacker":
                win += 1
            elif result["winner"] == "defender":
                loss += 1
            else:
                draw += 1
            if bool(result["summary"]["defender_disabled"]):
                ttk_rounds_total += int(result["rounds_fought"])
                ttk_samples += 1

        return {
            "fit_id": str(uuid.uuid4()),
            "hull_id": hull_id.strip(),
            "hull_name": hull.get("name", hull_id.strip()),
            "hull_level": hull_level,
            "hull_level_stat_multiplier": round(hull_level_mult, 4),
            "can_fit": can_fit,
            "violations": violations,
            "slot_usage": slot_usage,
            "slot_limits": slot_limits,
            "deck_used": round(deck_used, 3),
            "deck_limit": round(deck_limit, 3),
            "power_draw": round(power_draw, 3),
            "power_generation": round(power_generation, 3),
            "power_net": round(power_net, 3),
            "thermal": {
                "generation_per_second": round(heat_generation_per_second, 4),
                "dissipation_per_second": round(heat_dissipation_per_second, 4),
                "margin_per_second": round(heat_margin_per_second, 4),
                "load_ratio": round(thermal_load_ratio, 4),
                "state": thermal_state,
            },
            "merged_stats": {k: round(float(v), 3) for k, v in merged_stats.items()},
            "support_metrics": {
                key: round(float(value), 3)
                for key, value in support_metrics.items()
            },
            "ship_space": ship_space,
            "profiles": {
                "damage_profile": {k: round(v, 4) for k, v in damage_profile.items()},
                "resistance_profile": {k: round(v, 4) for k, v in resistance_profile.items()},
            },
            "ship_effects": ship_effects,
            "family_counts": family_counts,
            "role_projection": {
                "primary_role": primary_role,
                "secondary_roles": secondary_roles,
                "scores": {key: round(float(value), 3) for key, value in role_scores.items()},
            },
            "modules": normalized_modules,
            "combat_score": round(self._combat_effective_score(merged_stats), 3),
            "odds": odds,
            "simulation_runs": runs,
            "simulation_summary": {
                "attacker_win_rate": round(win / runs, 4),
                "defender_win_rate": round(loss / runs, 4),
                "draw_rate": round(draw / runs, 4),
                "avg_rounds": round(rounds_total / runs, 3),
                "avg_ttk_rounds_when_win": round(ttk_rounds_total / max(1, ttk_samples), 3),
            },
            "sample_simulations": simulations,
        }

    def _ship_level_stat_multiplier(self, ship_level: int) -> float:
        level_value = max(1, int(ship_level))
        if level_value <= 1:
            return 1.0
        # Soft-scaling growth stays meaningful at high levels without hard-capping progression.
        return 1.0 + (0.06 * math.log1p(level_value - 1)) + (0.0018 * math.sqrt(level_value - 1))

    def _module_instance_stat_multiplier(self, row: dict[str, Any]) -> float:
        raw = row.get("stat_multiplier", 1.0)
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return float(raw)
        return 1.0

    def _apply_module_instance_combat_bonuses(
        self,
        stats: dict[str, float],
        module_instances: list[dict[str, Any]],
    ) -> None:
        synergy_counts: dict[str, int] = {}
        for row in module_instances:
            stat_multiplier = self._module_instance_stat_multiplier(row)
            payload = row if isinstance(row, dict) else {}
            raw_rolls = payload.get("rolled_stats_preview")
            roll_stats = raw_rolls if isinstance(raw_rolls, dict) else {}
            raw_affix = payload.get("affix_stat_bonuses")
            affix_stats = raw_affix if isinstance(raw_affix, dict) else {}
            for source_stats, scale in ((roll_stats, 0.16), (affix_stats, 0.22)):
                for stat_key, raw in source_stats.items():
                    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                        continue
                    value = float(raw) * stat_multiplier
                    if stat_key == "attack":
                        stats["attack"] += value * scale
                    elif stat_key == "defense":
                        stats["defense"] += value * scale
                    elif stat_key == "hull":
                        stats["hull"] += value * scale
                    elif stat_key == "shield":
                        stats["shield"] += value * scale
                    elif stat_key == "energy":
                        stats["energy"] += value * scale
                    elif stat_key == "scan":
                        stats["scan"] += value * scale
                    elif stat_key == "cloak":
                        stats["cloak"] += value * scale
                    elif stat_key == "critical_chance_pct":
                        stats["attack"] += value * 0.72
                    elif stat_key == "sensor_lock":
                        stats["scan"] += value * 0.62
                    elif stat_key == "missile_guidance":
                        stats["attack"] += value * 0.48
                    elif stat_key == "shield_recharge_pct":
                        stats["shield"] += value * 0.68
                    elif stat_key == "heat_dissipation":
                        stats["defense"] += value * 0.36
                    elif stat_key == "heat_dissipation_pct":
                        stats["defense"] += value * 1.45
                    elif stat_key == "repair_rate_pct":
                        stats["hull"] += value * 0.58
                    elif stat_key == "thrust_kn":
                        stats["defense"] += value * 0.26
                        stats["scan"] += value * 0.18
            tags_raw = payload.get("synergy_tags")
            tags = (
                [tag for tag in tags_raw if isinstance(tag, str) and tag.strip()]
                if isinstance(tags_raw, list)
                else []
            )
            for tag in tags:
                synergy_counts[tag] = synergy_counts.get(tag, 0) + 1

        for tag, count in synergy_counts.items():
            if count <= 1:
                continue
            stack = min(5, count - 1)
            bonus_scale = 0.008 * stack
            if "scan" in tag or "sensor" in tag:
                stats["scan"] *= 1.0 + bonus_scale
            elif "stealth" in tag or "cloak" in tag:
                stats["cloak"] *= 1.0 + bonus_scale
            elif "engine" in tag or "velocity" in tag:
                stats["defense"] *= 1.0 + (bonus_scale * 0.9)
                stats["energy"] *= 1.0 + (bonus_scale * 0.6)
            elif "shield" in tag or "barrier" in tag:
                stats["shield"] *= 1.0 + bonus_scale
                stats["defense"] *= 1.0 + (bonus_scale * 0.5)
            elif "reactor" in tag or "power" in tag:
                stats["energy"] *= 1.0 + bonus_scale
            else:
                stats["attack"] *= 1.0 + bonus_scale

    def _resolve_profile_memory_combat_loadout(
        self,
        player_id: str,
        fallback_hull_id: str,
    ) -> dict[str, Any] | None:
        profile = self.state_store.get_profile(player_id=player_id)
        player_memory = profile.get("player_memory", {})
        if not isinstance(player_memory, dict):
            return None

        candidates: list[tuple[str, dict[str, Any]]] = []

        def append_candidate(source_key: str, raw: Any) -> None:
            if isinstance(raw, dict):
                candidates.append((source_key, raw))

        append_candidate("profile_memory.combat_loadout", player_memory.get("combat_loadout"))
        append_candidate("profile_memory.equipped_loadout", player_memory.get("equipped_loadout"))
        append_candidate("profile_memory.active_loadout", player_memory.get("active_loadout"))
        combat_block = player_memory.get("combat")
        if isinstance(combat_block, dict):
            append_candidate("profile_memory.combat.loadout", combat_block.get("loadout"))
        fleet_block = player_memory.get("fleet")
        if isinstance(fleet_block, dict):
            append_candidate("profile_memory.fleet.loadout", fleet_block.get("loadout"))
        append_candidate("profile_memory.loadout", player_memory.get("loadout"))

        if not candidates:
            return None

        module_instances = self.state_store.list_crafted_instances(
            player_id=player_id,
            asset_type="module",
            limit=800,
        )
        instance_lookup: dict[str, dict[str, Any]] = {}
        modules_by_asset: dict[str, list[dict[str, Any]]] = {}
        for row in module_instances:
            if not isinstance(row, dict):
                continue
            instance_id = row.get("instance_id")
            asset_id = row.get("asset_id")
            if isinstance(instance_id, str) and instance_id.strip():
                instance_lookup[instance_id.strip()] = row
            if isinstance(asset_id, str) and asset_id.strip():
                modules_by_asset.setdefault(asset_id.strip(), []).append(row)
        for rows in modules_by_asset.values():
            rows.sort(key=self._module_instance_stat_multiplier, reverse=True)

        owned_modules = self.state_store.list_assets(
            player_id=player_id,
            asset_type="module",
            limit=1200,
        )
        owned_module_qty: dict[str, int] = {}
        for row in owned_modules:
            asset_id = row.get("asset_id")
            quantity = row.get("quantity")
            if not isinstance(asset_id, str):
                continue
            if isinstance(quantity, bool) or not isinstance(quantity, int):
                continue
            owned_module_qty[asset_id.strip()] = max(0, int(quantity))
        module_index = self.seed_store.module_index()

        def append_by_module_id(
            module_id: str,
            quantity: int,
            selected: list[dict[str, Any]],
            selected_synthetic: list[dict[str, Any]],
            used_instance_ids: set[str],
            claimed_asset_qty: dict[str, int],
        ) -> None:
            module_key = module_id.strip()
            if not module_key or quantity <= 0:
                return
            added = 0
            for row in modules_by_asset.get(module_key, []):
                if added >= quantity:
                    break
                instance_id = row.get("instance_id")
                if not isinstance(instance_id, str) or not instance_id.strip():
                    continue
                instance_key = instance_id.strip()
                if instance_key in used_instance_ids:
                    continue
                used_instance_ids.add(instance_key)
                selected.append(row)
                added += 1
            remaining = max(0, quantity - added)
            if remaining <= 0:
                return
            owned = max(0, int(owned_module_qty.get(module_key, 0)))
            claimed = max(0, int(claimed_asset_qty.get(module_key, 0)))
            available_for_synthetic = max(0, owned - claimed)
            synthetic_to_add = min(remaining, available_for_synthetic)
            if synthetic_to_add <= 0:
                return
            module_seed = module_index.get(module_key, {})
            stat_bonuses = (
                module_seed.get("stat_bonuses", {})
                if isinstance(module_seed, dict) and isinstance(module_seed.get("stat_bonuses"), dict)
                else {}
            )
            template = self._module_synergy_template(module_seed) if isinstance(module_seed, dict) else {}
            tags_raw = template.get("tags") if isinstance(template, dict) else []
            tags = [tag for tag in tags_raw if isinstance(tag, str)]
            for _ in range(synthetic_to_add):
                selected_synthetic.append(
                    {
                        "asset_id": module_key,
                        "stat_multiplier": 1.0,
                        "rolled_stats_preview": stat_bonuses,
                        "affix_stat_bonuses": {},
                        "synergy_tags": tags,
                    }
                )
            claimed_asset_qty[module_key] = claimed + synthetic_to_add

        for source_key, candidate in candidates:
            hull_id = None
            for key in ("hull_id", "active_hull_id"):
                raw = candidate.get(key)
                if isinstance(raw, str) and raw.strip():
                    hull_id = raw.strip()
                    break
            if hull_id is None:
                raw_hull = candidate.get("hull")
                if isinstance(raw_hull, dict):
                    for key in ("id", "hull_id", "asset_id"):
                        raw = raw_hull.get(key)
                        if isinstance(raw, str) and raw.strip():
                            hull_id = raw.strip()
                            break
            if hull_id is None and fallback_hull_id.strip():
                hull_id = fallback_hull_id.strip()
            if hull_id is None:
                continue

            selected_instances: list[dict[str, Any]] = []
            selected_synthetic: list[dict[str, Any]] = []
            used_instance_ids: set[str] = set()
            claimed_asset_qty: dict[str, int] = {}
            explicit_selection = False
            requested_rows = 0

            for key in (
                "module_instance_ids",
                "equipped_module_instance_ids",
                "equipped_instances",
                "instance_ids",
            ):
                if key not in candidate:
                    continue
                raw_ids = candidate.get(key)
                if not isinstance(raw_ids, list):
                    continue
                explicit_selection = True
                for raw_instance in raw_ids[:80]:
                    requested_rows += 1
                    if not isinstance(raw_instance, str) or not raw_instance.strip():
                        continue
                    instance_key = raw_instance.strip()
                    payload = instance_lookup.get(instance_key)
                    if not isinstance(payload, dict):
                        continue
                    if instance_key in used_instance_ids:
                        continue
                    used_instance_ids.add(instance_key)
                    selected_instances.append(payload)

            for key in ("modules", "equipped_modules", "module_loadout"):
                if key not in candidate:
                    continue
                raw_modules = candidate.get(key)
                if not isinstance(raw_modules, list):
                    continue
                explicit_selection = True
                for row in raw_modules[:80]:
                    module_id = None
                    quantity = 1
                    instance_id = None
                    if isinstance(row, str):
                        requested_rows += 1
                        if row.strip():
                            module_id = row.strip()
                    elif isinstance(row, dict):
                        requested_rows += 1
                        for id_key in ("id", "module_id", "asset_id"):
                            candidate_id = row.get(id_key)
                            if isinstance(candidate_id, str) and candidate_id.strip():
                                module_id = candidate_id.strip()
                                break
                        qty_raw = row.get("quantity", 1)
                        if isinstance(qty_raw, (int, float)) and not isinstance(qty_raw, bool):
                            quantity = max(1, min(8, int(qty_raw)))
                        raw_instance = row.get("instance_id")
                        if isinstance(raw_instance, str) and raw_instance.strip():
                            instance_id = raw_instance.strip()
                    if instance_id is not None:
                        payload = instance_lookup.get(instance_id)
                        if not isinstance(payload, dict):
                            continue
                        payload_module_id = payload.get("asset_id")
                        if (
                            module_id is not None
                            and isinstance(payload_module_id, str)
                            and payload_module_id.strip() != module_id
                        ):
                            continue
                        if instance_id in used_instance_ids:
                            continue
                        used_instance_ids.add(instance_id)
                        selected_instances.append(payload)
                        continue
                    if module_id is None:
                        continue
                    append_by_module_id(
                        module_id=module_id,
                        quantity=quantity,
                        selected=selected_instances,
                        selected_synthetic=selected_synthetic,
                        used_instance_ids=used_instance_ids,
                        claimed_asset_qty=claimed_asset_qty,
                    )

            for key in ("module_ids", "equipped_module_ids"):
                if key not in candidate:
                    continue
                raw_module_ids = candidate.get(key)
                if not isinstance(raw_module_ids, list):
                    continue
                explicit_selection = True
                for raw_module_id in raw_module_ids[:80]:
                    requested_rows += 1
                    if not isinstance(raw_module_id, str) or not raw_module_id.strip():
                        continue
                    append_by_module_id(
                        module_id=raw_module_id.strip(),
                        quantity=1,
                        selected=selected_instances,
                        selected_synthetic=selected_synthetic,
                        used_instance_ids=used_instance_ids,
                        claimed_asset_qty=claimed_asset_qty,
                    )

            if not explicit_selection:
                continue
            selected_modules = (selected_instances + selected_synthetic)[:14]
            if requested_rows > 0 and not selected_modules:
                continue
            return {
                "resolved": True,
                "source": f"persisted_loadout.{source_key}",
                "hull_id": hull_id,
                "module_instances": selected_modules,
            }
        return None

    def _resolve_authenticated_player_combat_profile(self, player_id: str) -> dict[str, Any]:
        fleet = self._ensure_fleet_initialized(player_id=player_id)
        fallback_hull_id = str(fleet.get("active_hull_id", "")).strip()
        loadout_resolution = self._resolve_profile_memory_combat_loadout(
            player_id=player_id,
            fallback_hull_id=fallback_hull_id,
        )
        stats = self._player_combat_stats(
            player_id=player_id,
            loadout_resolution=loadout_resolution,
            fleet=fleet,
        )
        if isinstance(loadout_resolution, dict) and bool(loadout_resolution.get("resolved")):
            source = str(loadout_resolution.get("source", "persisted_loadout.profile_memory"))
            module_rows = loadout_resolution.get("module_instances", [])
            module_count = len(module_rows) if isinstance(module_rows, list) else 0
            hull_id = loadout_resolution.get("hull_id")
        else:
            source = "legacy.inventory_projection"
            module_count = 0
            hull_id = fallback_hull_id if fallback_hull_id else None
        return {
            "stats": stats,
            "source": source,
            "loadout": {
                "hull_id": hull_id if isinstance(hull_id, str) and hull_id.strip() else None,
                "module_count": int(module_count),
            },
        }

    def _player_combat_stats(
        self,
        player_id: str,
        loadout_resolution: dict[str, Any] | None = None,
        fleet: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        default_stats = {
            "attack": 220.0,
            "defense": 210.0,
            "hull": 760.0,
            "shield": 330.0,
            "energy": 460.0,
            "scan": 88.0,
            "cloak": 56.0,
        }
        if not isinstance(fleet, dict):
            fleet = self._ensure_fleet_initialized(player_id=player_id)
        active_hull_id = fleet.get("active_hull_id")
        ship_level_raw = fleet.get("ship_level", 1)
        ship_level = int(ship_level_raw) if isinstance(ship_level_raw, int) else 1
        ship_level_mult = self._ship_level_stat_multiplier(ship_level)
        hull_assets = self.state_store.list_assets(player_id=player_id, asset_type="hull", limit=120)
        hull_index = self.seed_store.hull_index()
        best_hull: dict[str, Any] | None = None
        best_tier = -1
        loadout_resolved = isinstance(loadout_resolution, dict) and bool(loadout_resolution.get("resolved"))
        if loadout_resolved:
            resolved_hull_id = loadout_resolution.get("hull_id")
            if isinstance(resolved_hull_id, str) and resolved_hull_id.strip():
                resolved_hull = hull_index.get(resolved_hull_id.strip())
                if isinstance(resolved_hull, dict):
                    best_hull = resolved_hull
                    tier_raw = resolved_hull.get("tier", 1)
                    best_tier = int(tier_raw) if isinstance(tier_raw, int) else 1
        if best_hull is None:
            if isinstance(active_hull_id, str) and active_hull_id.strip():
                active_hull = hull_index.get(active_hull_id.strip())
                if isinstance(active_hull, dict):
                    best_hull = active_hull
                    tier_raw = active_hull.get("tier", 1)
                    best_tier = int(tier_raw) if isinstance(tier_raw, int) else 1
            for row in hull_assets:
                hull_id = row.get("asset_id")
                if not isinstance(hull_id, str):
                    continue
                hull = hull_index.get(hull_id)
                if not isinstance(hull, dict):
                    continue
                tier = int(hull.get("tier", 1))
                if tier > best_tier:
                    best_tier = tier
                    best_hull = hull
        if isinstance(best_hull, dict):
            base = best_hull.get("base_stats", {})
            if isinstance(base, dict):
                for key in ("attack", "defense", "hull", "shield", "energy", "scan", "cloak"):
                    raw = base.get(key)
                    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                        default_stats[key] = float(raw)

        identity_profile = self._player_identity_modifier_profile(player_id=player_id)
        identity_modifiers = (
            identity_profile.get("modifiers", {})
            if isinstance(identity_profile.get("modifiers"), dict)
            else {}
        )
        modifier_key_by_stat = {
            "attack": "attack_pct",
            "defense": "defense_pct",
            "hull": "hull_pct",
            "shield": "shield_pct",
            "energy": "energy_pct",
            "scan": "scan_pct",
            "cloak": "cloak_pct",
        }
        for key in ("attack", "defense", "hull", "shield", "energy", "scan", "cloak"):
            default_stats[key] *= ship_level_mult
            mod_key = modifier_key_by_stat[key]
            mod_value = identity_modifiers.get(mod_key)
            if isinstance(mod_value, (int, float)) and not isinstance(mod_value, bool):
                default_stats[key] *= (1.0 + (float(mod_value) / 100.0))

        selected_modules: list[dict[str, Any]] = []
        if loadout_resolved:
            candidate_rows = loadout_resolution.get("module_instances", [])
            if isinstance(candidate_rows, list):
                selected_modules = [row for row in candidate_rows if isinstance(row, dict)][:14]
        else:
            module_instances = self.state_store.list_crafted_instances(
                player_id=player_id,
                asset_type="module",
                limit=220,
            )
            if module_instances:
                module_instances.sort(key=self._module_instance_stat_multiplier, reverse=True)
                selected_modules = module_instances[:14]
        if selected_modules:
            self._apply_module_instance_combat_bonuses(
                stats=default_stats,
                module_instances=selected_modules,
            )
        return default_stats

    def _combat_contacts(self, player_id: str, count: int, seed: int) -> dict[str, Any]:
        player_profile = self._resolve_authenticated_player_combat_profile(
            player_id=player_id,
        )
        player_stats = (
            player_profile.get("stats", {})
            if isinstance(player_profile.get("stats"), dict)
            else {}
        )
        if not player_stats:
            player_stats = self._player_combat_stats(player_id=player_id)
        player_stats_source = str(
            player_profile.get("source", "legacy.inventory_projection")
        )
        player_loadout = (
            player_profile.get("loadout", {})
            if isinstance(player_profile.get("loadout"), dict)
            else {}
        )
        rng = random.Random(seed)
        hulls = [row for row in self.seed_store.ship_hulls if isinstance(row, dict)]
        if not hulls:
            raise ValueError("No hull seed data available for contacts")
        contacts: list[dict[str, Any]] = []
        player_score = self._combat_effective_score(player_stats)
        player_level = self._combat_power_level(player_stats)
        for _ in range(count):
            hull = hulls[rng.randrange(0, len(hulls))]
            base = hull.get("base_stats", {})
            if not isinstance(base, dict):
                continue
            threat = rng.uniform(0.72, 1.34)
            hostile = rng.random() < 0.62
            stats = {}
            for key in ("attack", "defense", "hull", "shield", "energy", "scan", "cloak"):
                raw = base.get(key, player_stats[key])
                value = float(raw) if isinstance(raw, (int, float)) and not isinstance(raw, bool) else player_stats[key]
                stats[key] = max(1.0, value * threat)
            odds = self._estimate_combat_odds(
                {
                    "battle_id": f"odds.{uuid.uuid4().hex[:8]}",
                    "attacker": {"name": "Player", "stats": player_stats},
                    "defender": {"name": hull.get("name", "Unknown Contact"), "stats": stats},
                    "context": {"mode": "pvp", "max_rounds": DEFAULT_COMBAT_ROUNDS, "seed": seed},
                }
            )
            hostile_score = self._combat_effective_score(stats)
            enemy_level = self._combat_power_level(stats)
            contacts.append(
                {
                    "contact_id": f"contact.{uuid.uuid4().hex[:10]}",
                    "name": hull.get("name", "Unknown Contact"),
                    "hull_id": hull.get("id"),
                    "hostile": hostile,
                    "will_attack_on_sight": bool(hostile and hostile_score >= player_score * 0.84),
                    "threat_ratio": round(hostile_score / max(1.0, player_score), 3),
                    "combat_level": int(enemy_level),
                    "player_level": int(player_level),
                    "level_gap": int(enemy_level - player_level),
                    "stats": {k: round(float(v), 2) for k, v in stats.items()},
                    "odds": odds,
                }
            )
        contacts.sort(
            key=lambda row: (not bool(row["hostile"]), -float(row["threat_ratio"]))
        )
        for contact in contacts:
            if not isinstance(contact, dict):
                continue
            encounter_id = self._issue_encounter(player_id=player_id, contact=contact)
            contact["encounter_id"] = encounter_id
        return {
            "player_id": player_id,
            "seed": seed,
            "player_stats": {k: round(v, 2) for k, v in player_stats.items()},
            "player_stats_source": player_stats_source,
            "player_loadout": player_loadout,
            "player_level": int(player_level),
            "total": len(contacts),
            "items": contacts,
        }

    def _engage_contact(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        contact = payload.get("contact")
        encounter_id = payload.get("encounter_id")
        if encounter_id is None and isinstance(contact, dict):
            encounter_id = contact.get("encounter_id")
        action = payload.get("action", "fight")
        context = payload.get("context", {})
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(encounter_id, str) or not encounter_id.strip():
            raise ValueError("encounter_id is required")
        if action not in {"fight", "flee"}:
            raise ValueError("action must be one of: fight, flee")
        if context is None:
            context = {}
        if not isinstance(context, dict):
            raise ValueError("context must be an object when provided")

        self._ensure_player_bootstrap(player_id.strip())
        contact = self._resolve_encounter(
            player_id=player_id.strip(),
            encounter_id=encounter_id.strip(),
        )
        fleet_before = self._ensure_fleet_initialized(player_id.strip())

        player_profile = self._resolve_authenticated_player_combat_profile(
            player_id=player_id.strip(),
        )
        player_stats = (
            player_profile.get("stats", {})
            if isinstance(player_profile.get("stats"), dict)
            else {}
        )
        if not player_stats:
            player_stats = self._player_combat_stats(player_id=player_id.strip())
        player_stats_source = str(
            player_profile.get("source", "legacy.inventory_projection")
        )
        player_loadout = (
            player_profile.get("loadout", {})
            if isinstance(player_profile.get("loadout"), dict)
            else {}
        )
        enemy_stats_raw = contact.get("stats")
        if not isinstance(enemy_stats_raw, dict):
            raise ValueError("contact.stats must be an object")
        enemy_stats = {}
        for key in STAT_KEYS:
            raw = enemy_stats_raw.get(key, player_stats[key])
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ValueError(f"contact.stats.{key} must be numeric")
            enemy_stats[key] = float(raw)
        initiated_raw = payload.get("player_initiated_attack", context.get("player_initiated_attack", True))
        if not isinstance(initiated_raw, bool):
            raise ValueError("player_initiated_attack must be boolean when provided")
        player_initiated_attack = bool(initiated_raw)
        risk_profile = self._combat_risk_profile(
            player_stats=player_stats,
            enemy_stats=enemy_stats,
        )
        reward_scaling = self._combat_reward_scaler(
            risk_profile=risk_profile,
            player_initiated_attack=player_initiated_attack,
        )

        if action == "flee":
            flee_score = (
                (player_stats["cloak"] * 0.9)
                + (player_stats["scan"] * 0.35)
                + (player_stats["energy"] * 0.15)
                + (float(fleet_before.get("hull_durability", 100.0)) * 0.25)
            )
            pursuit_score = (
                (enemy_stats["scan"] * 0.65)
                + (enemy_stats["attack"] * 0.2)
                + (enemy_stats["energy"] * 0.15)
            )
            flee_probability = 1.0 / (1.0 + math.exp(-(flee_score - pursuit_score) / 55.0))
            escaped = random.random() < flee_probability
            battle_metrics = self.state_store.increment_battle_metrics(
                player_id=player_id.strip(),
                fled=1,
            )
            return {
                "player_id": player_id.strip(),
                "action": "flee",
                "encounter_id": encounter_id.strip(),
                "escaped": escaped,
                "flee_probability": round(flee_probability, 4),
                "contact_id": contact.get("contact_id"),
                "contact_name": contact.get("name"),
                "result": "escaped" if escaped else "failed_to_escape",
                "fleet_before": fleet_before,
                "player_stats_source": player_stats_source,
                "player_loadout": player_loadout,
                "engagement_balance": {
                    "risk_profile": risk_profile,
                    "reward_scaling": reward_scaling,
                },
                "battle_metrics": battle_metrics,
            }

        battle_context = {
            "mode": "pvp",
            "max_rounds": 8,
            "seed": int(stable_hash_int(player_id.strip(), encounter_id.strip(), "engage")),
            "counterfire_enabled": True,
        }
        for key in ("mode", "max_rounds", "seed", "damage_cap", "tactical_commands"):
            if key in context:
                battle_context[key] = context[key]
        normalized = self._normalize_combat_payload(
            {
                "attacker": {"name": "Player", "stats": player_stats},
                "defender": {
                    "name": str(contact.get("name", "Hostile")),
                    "stats": enemy_stats,
                },
                "context": battle_context,
            }
        )
        battle = self._simulate_combat(normalized)
        fleet_after = fleet_before
        defeat_consequences = None
        victory_rewards = None
        if bool(battle["summary"]["attacker_disabled"]) or battle["winner"] == "defender":
            defeat_consequences = self._apply_defeat_consequences(
                player_id=player_id.strip(),
                player_stats=player_stats,
                battle=battle,
            )
            fleet_after = defeat_consequences["fleet_after"]
            battle_metrics = self.state_store.increment_battle_metrics(
                player_id=player_id.strip(),
                lost=1,
            )
        else:
            incoming = float(battle["damage_totals"]["defender_to_attacker"])
            total_capacity = max(1.0, player_stats["hull"] + player_stats["shield"])
            wear_loss = min(4.0, max(0.0, (incoming / total_capacity) * 5.0))
            if wear_loss > 0:
                fleet_after = self.state_store.update_fleet_state(
                    player_id=player_id.strip(),
                    hull_durability=max(
                        0.0,
                        float(fleet_before.get("hull_durability", 100.0)) - wear_loss,
                    ),
                )
            if battle["winner"] == "attacker":
                victory_rewards = self._apply_victory_rewards(
                    player_id=player_id.strip(),
                    player_stats=player_stats,
                    enemy_stats=enemy_stats,
                    battle=battle,
                    risk_profile=risk_profile,
                    reward_scaling=reward_scaling,
                )
                battle_metrics = self.state_store.increment_battle_metrics(
                    player_id=player_id.strip(),
                    won=1,
                )
            else:
                battle_metrics = self.state_store.increment_battle_metrics(
                    player_id=player_id.strip(),
                    lost=1,
                )
        return {
            "player_id": player_id.strip(),
            "action": "fight",
            "encounter_id": encounter_id.strip(),
            "contact_id": contact.get("contact_id"),
            "contact_name": contact.get("name"),
            "battle": battle,
            "fleet_before": fleet_before,
            "fleet_after": fleet_after,
            "player_stats_source": player_stats_source,
            "player_loadout": player_loadout,
            "defeat_consequences": defeat_consequences,
            "victory_rewards": victory_rewards,
            "engagement_balance": {
                "risk_profile": risk_profile,
                "reward_scaling": reward_scaling,
            },
            "battle_metrics": battle_metrics,
        }

    def _apply_defeat_consequences(
        self,
        player_id: str,
        player_stats: dict[str, float],
        battle: dict[str, Any],
    ) -> dict[str, Any]:
        fleet_before = self._ensure_fleet_initialized(player_id=player_id)
        seed_raw = battle.get("seed")
        seed = int(seed_raw) if isinstance(seed_raw, (int, float)) and not isinstance(seed_raw, bool) else int(
            stable_hash_int(player_id, "defeat")
        )
        rng = random.Random(seed ^ stable_hash_int(player_id, "defeat"))
        damage_ratio = min(
            1.0,
            float(battle["damage_totals"]["defender_to_attacker"])
            / max(1.0, player_stats["hull"] + player_stats["shield"]),
        )
        severity = min(
            1.0,
            max(
                0.12,
                (0.68 * damage_ratio)
                + (0.32 * (1.0 if battle["summary"]["attacker_disabled"] else 0.0)),
            ),
        )
        durability_loss = max(4.0, min(70.0, 8.0 + (52.0 * severity)))
        cargo_loss_ratio = max(0.08, min(0.85, 0.12 + (0.58 * severity)))
        crew_exposure_ratio = max(0.10, min(0.95, 0.16 + (0.66 * severity)))

        crew_total = float(fleet_before.get("crew_total", 0.0))
        crew_elite = float(fleet_before.get("crew_elite", 0.0))
        crew_exposed = crew_total * crew_exposure_ratio
        remaining_hull_ratio = min(
            1.0,
            float(battle["remaining"]["attacker"]["hull"]) / max(1.0, player_stats["hull"]),
        )
        elite_ratio = crew_elite / max(1.0, crew_total)
        lifeboat_chance = max(
            0.05,
            min(
                0.88,
                0.18
                + (player_stats["scan"] / 1500.0)
                + (player_stats["cloak"] / 1700.0)
                + (0.22 * remaining_hull_ratio)
                + (0.18 * elite_ratio),
            ),
        )
        crew_rescued = crew_exposed * lifeboat_chance
        crew_casualties = min(crew_total, max(0.0, crew_exposed - crew_rescued))

        cargo = fleet_before.get("cargo", {})
        element_deltas: dict[str, float] = {}
        cargo_losses: list[dict[str, Any]] = []
        cargo_symbols: list[str] = []
        if isinstance(cargo, dict):
            for symbol in cargo.keys():
                if isinstance(symbol, str):
                    cargo_symbols.append(symbol)
        inventory = self.state_store.get_inventory_amounts(
            player_id=player_id,
            symbols=cargo_symbols,
        )
        if isinstance(cargo, dict):
            for symbol, amount in cargo.items():
                if not isinstance(symbol, str):
                    continue
                if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                    continue
                if float(amount) <= 0:
                    continue
                planned_loss = float(amount) * cargo_loss_ratio * rng.uniform(0.88, 1.14)
                available = float(inventory.get(symbol, 0.0))
                actual_loss = min(available, max(0.0, planned_loss))
                if actual_loss <= 0:
                    continue
                element_deltas[symbol] = element_deltas.get(symbol, 0.0) - actual_loss
                cargo_losses.append(
                    {
                        "symbol": symbol,
                        "planned": round(planned_loss, 3),
                        "actual": round(actual_loss, 3),
                    }
                )
        inventory_after: dict[str, float] = {}
        if element_deltas:
            resource_state = self.state_store.apply_resource_delta(
                player_id=player_id,
                element_deltas=element_deltas,
            )
            inventory_after = resource_state["inventory"]

        fleet_after = self.state_store.apply_fleet_combat_losses(
            player_id=player_id,
            hull_durability_loss=durability_loss,
            crew_casualties=crew_casualties,
            cargo_loss_ratio=cargo_loss_ratio,
        )

        module_loss: dict[str, Any] | None = None
        if severity >= 0.45 and rng.random() < min(0.55, 0.2 + (0.35 * severity)):
            modules = self.state_store.list_assets(
                player_id=player_id,
                asset_type="module",
                limit=120,
            )
            candidates = [
                row
                for row in modules
                if isinstance(row, dict)
                and isinstance(row.get("asset_id"), str)
                and isinstance(row.get("quantity"), int)
                and int(row["quantity"]) > 0
            ]
            if candidates:
                selected = candidates[rng.randrange(0, len(candidates))]
                asset_id = str(selected["asset_id"])
                updated = self.state_store.adjust_asset_quantity(
                    player_id=player_id,
                    asset_type="module",
                    asset_id=asset_id,
                    quantity_delta=-1,
                )
                module_loss = {
                    "asset_id": asset_id,
                    "quantity_lost": 1,
                    "remaining_quantity": int(updated.get("quantity", 0)),
                }

        return {
            "severity": round(severity, 4),
            "hull_durability_loss": round(durability_loss, 4),
            "cargo_loss_ratio": round(cargo_loss_ratio, 4),
            "cargo_losses": cargo_losses,
            "crew_exposed": round(crew_exposed, 4),
            "crew_rescued": round(crew_rescued, 4),
            "crew_casualties": round(crew_casualties, 4),
            "lifeboat_chance": round(lifeboat_chance, 4),
            "module_loss": module_loss,
            "wallet_impact": {"credits_delta": 0.0, "voidcoin_delta": 0.0},
            "inventory_after_loss": {
                symbol: round(float(value), 3) for symbol, value in inventory_after.items()
            },
            "fleet_before": fleet_before,
            "fleet_after": fleet_after,
        }

    def _apply_victory_rewards(
        self,
        player_id: str,
        player_stats: dict[str, float],
        enemy_stats: dict[str, float],
        battle: dict[str, Any],
        risk_profile: dict[str, Any],
        reward_scaling: dict[str, Any],
    ) -> dict[str, Any]:
        seed_raw = battle.get("seed")
        seed = int(seed_raw) if isinstance(seed_raw, (int, float)) and not isinstance(seed_raw, bool) else int(
            stable_hash_int(player_id, "victory_rewards")
        )
        rng = random.Random(seed ^ stable_hash_int(player_id, "victory_rewards"))
        reward_scale = float(reward_scaling.get("reward_scale", 1.0))
        enemy_score = self._combat_effective_score(enemy_stats)
        enemy_capacity = max(1.0, enemy_stats["hull"] + enemy_stats["shield"])
        inflicted_ratio = min(
            1.15,
            float(battle["damage_totals"]["attacker_to_defender"]) / enemy_capacity,
        )
        remaining_hull_ratio = min(
            1.0,
            float(battle["remaining"]["attacker"]["hull"]) / max(1.0, player_stats["hull"]),
        )

        base_credits = 90.0 + (enemy_score * 0.9)
        credits_awarded = (
            base_credits
            * (0.66 + (0.54 * min(1.0, inflicted_ratio)))
            * (0.72 + (0.28 * remaining_hull_ratio))
            * reward_scale
            * rng.uniform(0.92, 1.08)
        )
        if bool(reward_scaling.get("gank_penalty_active")):
            credits_awarded *= 0.42
        credits_awarded = max(0.0, round(credits_awarded, 4))

        salvage_base = max(
            0.0,
            (enemy_score / 95.0)
            * (0.4 + (0.6 * min(1.0, inflicted_ratio)))
            * (max(0.12, reward_scale) ** 0.9),
        )
        element_weights: dict[str, float] = {
            "Fe": 1.0,
            "Ni": 0.58,
            "Cu": 0.46,
            "Si": 0.42,
            "C": 0.28,
        }
        if bool(reward_scaling.get("underdog_bonus_active")):
            element_weights["Ti"] = 0.26
            element_weights["W"] = 0.18
        element_deltas: dict[str, float] = {}
        for symbol, weight in element_weights.items():
            amount = salvage_base * weight * rng.uniform(0.64, 1.34)
            if amount >= 0.01:
                element_deltas[symbol] = round(amount, 3)

        voidcoin_awarded = 0.0
        if bool(reward_scaling.get("underdog_bonus_active")):
            level_gap = max(0, int(risk_profile.get("level_gap", 0)))
            win_probability = float(risk_profile.get("player_win_probability", 0.5))
            voidcoin_awarded = (
                0.01
                + (level_gap * 0.008)
                + (max(0.0, 0.5 - win_probability) * 0.05)
            ) * rng.uniform(0.9, 1.1)
            voidcoin_awarded = max(0.0, min(0.35, round(voidcoin_awarded, 6)))

        combat_xp_awarded = max(
            12.0,
            (enemy_score / 18.0) * max(0.35, reward_scale) * rng.uniform(0.88, 1.22),
        )
        progress_after = self.state_store.grant_combat_xp(
            player_id=player_id,
            xp_delta=combat_xp_awarded,
        )
        ship_xp_awarded = max(
            4.0,
            combat_xp_awarded
            * (0.42 + (0.38 * min(1.0, inflicted_ratio)))
            * rng.uniform(0.9, 1.15),
        )
        if bool(reward_scaling.get("gank_penalty_active")):
            ship_xp_awarded *= 0.55
        ship_progress_after = self.state_store.grant_fleet_xp(
            player_id=player_id,
            xp_delta=ship_xp_awarded,
        )

        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id,
            credits_delta=credits_awarded,
            voidcoin_delta=voidcoin_awarded,
            element_deltas=element_deltas,
        )
        return {
            "reward_scaling": reward_scaling,
            "risk_profile": risk_profile,
            "credits_awarded": credits_awarded,
            "voidcoin_awarded": voidcoin_awarded,
            "combat_xp_awarded": round(combat_xp_awarded, 3),
            "ship_xp_awarded": round(ship_xp_awarded, 3),
            "combat_progress_after": progress_after,
            "ship_progress_after": ship_progress_after,
            "salvage_elements_awarded": element_deltas,
            "wallet_after": resource_state["wallet"],
            "inventory_after": resource_state["inventory"],
        }

    def _auto_resolve_hostile(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        prefer_flee = bool(payload.get("prefer_flee", False))
        context = payload.get("context", {})
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if context is None:
            context = {}
        if not isinstance(context, dict):
            raise ValueError("context must be an object when provided")
        self._ensure_player_bootstrap(player_id.strip())
        if DETERMINISTIC_MODE:
            encounter_seed = int(stable_hash_int(player_id.strip(), "auto_resolve"))
        else:
            encounter_seed = int(time.time())
        contacts = self._combat_contacts(player_id=player_id.strip(), count=6, seed=encounter_seed)
        hostile = next(
            (
                item
                for item in contacts["items"]
                if isinstance(item, dict) and bool(item.get("will_attack_on_sight"))
            ),
            None,
        )
        if hostile is None:
            return {
                "player_id": player_id.strip(),
                "auto_encounter": False,
                "result": "no_hostile_contact",
            }
        if prefer_flee:
            flee_attempt = self._engage_contact(
                {
                    "player_id": player_id.strip(),
                    "contact": hostile,
                    "action": "flee",
                    "context": context,
                }
            )
            if flee_attempt.get("escaped"):
                return {
                    "player_id": player_id.strip(),
                    "auto_encounter": True,
                    "resolution": flee_attempt,
                }
            forced = self._engage_contact(
                {
                    "player_id": player_id.strip(),
                    "contact": hostile,
                    "action": "fight",
                    "player_initiated_attack": False,
                    "context": context,
                }
            )
            return {
                "player_id": player_id.strip(),
                "auto_encounter": True,
                "resolution": {
                    "result": "failed_escape_forced_combat",
                    "flee_attempt": flee_attempt,
                    "forced_engagement": forced,
                },
            }
        resolved = self._engage_contact(
            {
                "player_id": player_id.strip(),
                "contact": hostile,
                "action": "fight",
                "player_initiated_attack": False,
                "context": context,
            }
        )
        return {"player_id": player_id.strip(), "auto_encounter": True, "resolution": resolved}

    def _market_epoch_bucket(self) -> int:
        return int(time.time() // 300)

    def _voidcoin_rate_credits(self, bucket: int) -> float:
        # Credits per 1 voidcoin. A mild wave keeps the exchange dynamic.
        base = 940.0 + (85.0 * math.sin(bucket / 9.0))
        noise = 20.0 * math.sin(bucket / 3.7 + 1.9)
        return max(120.0, base + noise)

    def _element_demand_scores(self) -> dict[str, float]:
        demand_totals: dict[str, float] = {}

        def add_symbol(symbol: Any, amount: Any) -> None:
            if not isinstance(symbol, str):
                return
            if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                return
            numeric = float(amount)
            if not math.isfinite(numeric) or numeric <= 0:
                return
            demand_totals[symbol] = demand_totals.get(symbol, 0.0) + numeric

        def accumulate_cost(cost: dict[str, Any] | None, weight: float = 1.0) -> None:
            if not isinstance(cost, dict):
                return
            if not math.isfinite(weight) or weight <= 0:
                return
            elements = cost.get("elements")
            if not isinstance(elements, list):
                return
            for row in elements:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                amount = row.get("amount")
                if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                    continue
                add_symbol(symbol=symbol, amount=float(amount) * weight)

        for row in self.seed_store.modules:
            if isinstance(row, dict):
                accumulate_cost(row.get("build_cost"))
        for row in self.seed_store.ship_hulls:
            if isinstance(row, dict):
                accumulate_cost(row.get("build_cost"))
        for row in self.seed_store.structures:
            if isinstance(row, dict):
                accumulate_cost(row.get("build_cost"))
        for row in self.seed_store.tech_tree:
            if isinstance(row, dict):
                accumulate_cost(self._tech_research_cost(row))
        for row in self.seed_store.crafting_substitutions:
            if isinstance(row, dict):
                accumulate_cost(row.get("override_cost"), weight=0.65)

        # Material recipes also drive macro demand (metamaterials, alloys, ceramics, etc.).
        for material in self.seed_store.materials:
            if not isinstance(material, dict):
                continue
            composition = material.get("composition")
            if not isinstance(composition, list):
                continue
            economy_weight_raw = material.get("economy_weight", 1.0)
            if isinstance(economy_weight_raw, bool) or not isinstance(
                economy_weight_raw, (int, float)
            ):
                economy_weight = 1.0
            else:
                economy_weight = max(0.1, min(3.0, float(economy_weight_raw)))

            used_in = material.get("used_in")
            usage_count = (
                sum(1 for item in used_in if isinstance(item, str))
                if isinstance(used_in, list)
                else 0
            )
            usage_factor = 1.0 + min(2.0, usage_count * 0.2)
            material_scale = 6.0 * economy_weight * usage_factor

            for row in composition:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                wt_pct = row.get("wt_pct")
                if isinstance(wt_pct, bool) or not isinstance(wt_pct, (int, float)):
                    continue
                add_symbol(symbol=symbol, amount=max(0.0, float(wt_pct)) * material_scale)

        if not demand_totals:
            return {}
        max_score = max(demand_totals.values())
        if max_score <= 0:
            return {}
        return {
            symbol: max(0.0, min(1.0, value / max_score))
            for symbol, value in demand_totals.items()
        }

    def _element_base_price_credits(
        self, element: dict[str, Any], demand_score: float
    ) -> float:
        atomic = int(element.get("atomic_number", 1))
        symbol = str(element.get("symbol", "X"))
        group_block = str(element.get("group_block", "")).casefold()
        standard_state = str(element.get("standard_state", "")).casefold()

        base = 1.5 + (float(atomic) ** 1.12) * 0.42
        if symbol in COMMON_ELEMENT_SYMBOLS:
            base *= 0.45
        if symbol in RARE_ELEMENT_SYMBOLS:
            base *= 2.8
        if atomic >= 57:
            base *= 1.35
        if "lanthanide" in group_block or "actinide" in group_block:
            base *= 1.45
        if "noble gas" in group_block and symbol not in {"He", "Ne"}:
            base *= 1.2
        if "alkali" in group_block:
            base *= 0.82
        if standard_state == "gas" and symbol not in {"H", "N", "O"}:
            base *= 1.15
        base *= 1.0 + (0.65 * max(0.0, min(1.0, demand_score)))
        return max(0.5, base)

    def _element_market_row(
        self,
        element: dict[str, Any],
        bucket: int,
        demand_score: float,
        holdings: float,
        region: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        atomic = int(element.get("atomic_number", 1))
        symbol = str(element.get("symbol"))
        seed = ((bucket * 1315423911) ^ (atomic * 2654435761)) & 0xFFFFFFFF
        rng = random.Random(seed)

        base_price = self._element_base_price_credits(element, demand_score=demand_score)
        volatility = max(0.02, min(0.28, 0.03 + (0.16 * demand_score)))
        if symbol in RARE_ELEMENT_SYMBOLS or atomic >= 57:
            volatility = min(0.32, volatility + 0.05)

        wave = math.sin((bucket / 7.0) + (atomic * 0.33)) * volatility
        jitter = rng.uniform(-volatility, volatility)
        mid_price = max(base_price * 0.55, base_price * (1.0 + wave + jitter))
        spread = 0.018 + (0.85 * volatility)
        liquidity_multiplier = 1.0
        risk_premium_pct = 0.0
        spread_multiplier = 1.0
        region_id = None
        if isinstance(region, dict):
            region_id = region.get("id")
            raw_liquidity = region.get("liquidity_multiplier", 1.0)
            raw_risk = region.get("risk_premium_pct", 0.0)
            raw_spread = region.get("spread_multiplier", 1.0)
            if isinstance(raw_liquidity, (int, float)) and not isinstance(raw_liquidity, bool):
                liquidity_multiplier = max(0.2, min(3.0, float(raw_liquidity)))
            if isinstance(raw_risk, (int, float)) and not isinstance(raw_risk, bool):
                risk_premium_pct = max(-30.0, min(120.0, float(raw_risk)))
            if isinstance(raw_spread, (int, float)) and not isinstance(raw_spread, bool):
                spread_multiplier = max(0.5, min(3.0, float(raw_spread)))
        mid_price = max(0.01, mid_price * (1.0 + (risk_premium_pct / 100.0)))
        mid_price = max(
            0.01,
            mid_price * (1.0 + ((1.0 / max(0.2, liquidity_multiplier)) - 1.0) * 0.28),
        )
        spread *= spread_multiplier
        ask_credits = max(0.01, mid_price * (1.0 + (spread / 2.0)))
        bid_credits = max(0.01, mid_price * (1.0 - (spread / 2.0)))

        credits_per_voidcoin = self._voidcoin_rate_credits(bucket=bucket)
        ask_voidcoin = ask_credits / credits_per_voidcoin
        bid_voidcoin = bid_credits / credits_per_voidcoin

        rarity = "common"
        if symbol in RARE_ELEMENT_SYMBOLS or atomic >= 57:
            rarity = "rare"
        elif atomic >= 37:
            rarity = "uncommon"

        return {
            "symbol": symbol,
            "name": element.get("name", symbol),
            "atomic_number": atomic,
            "group_block": element.get("group_block"),
            "standard_state": element.get("standard_state"),
            "rarity_tier": rarity,
            "demand_score": round(demand_score, 4),
            "volatility": round(volatility, 4),
            "mid_credits": round(mid_price, 4),
            "bid_credits": round(bid_credits, 4),
            "ask_credits": round(ask_credits, 4),
            "bid_voidcoin": round(bid_voidcoin, 8),
            "ask_voidcoin": round(ask_voidcoin, 8),
            "holdings": round(holdings, 3),
            "region_id": region_id,
            "regional_risk_premium_pct": round(risk_premium_pct, 3),
            "market_tick": bucket,
            "market_class": "element",
        }

    def _market_snapshot(self, player_id: str, limit: int, region_id: str | None = None) -> dict[str, Any]:
        bucket = self._market_epoch_bucket()
        demand_scores = self._element_demand_scores()
        holdings = self.state_store.get_inventory_amounts(player_id=player_id)
        wallet = self.state_store.get_wallet(player_id=player_id)
        region = None
        if isinstance(region_id, str):
            region = self.seed_store.region_index().get(region_id)
            if region is None:
                raise ValueError(f"Unknown region_id '{region_id}'")
        rows = [
            self._element_market_row(
                element=element,
                bucket=bucket,
                demand_score=demand_scores.get(str(element.get("symbol")), 0.0),
                holdings=holdings.get(str(element.get("symbol")), 0.0),
                region=region,
            )
            for element in self.seed_store.elements
            if isinstance(element, dict) and isinstance(element.get("symbol"), str)
        ]
        life_support_state = self.state_store.get_life_support_state(player_id=player_id)
        life_support_profile = self._life_support_rate_profile(player_id=player_id)
        demand_by_symbol = (
            life_support_profile.get("demand_per_hour", {})
            if isinstance(life_support_profile.get("demand_per_hour"), dict)
            else {}
        )
        production_by_symbol = (
            life_support_profile.get("production_per_hour", {})
            if isinstance(life_support_profile.get("production_per_hour"), dict)
            else {}
        )
        shortage_stress = (
            float(life_support_state.get("shortage_stress", 0.0))
            if isinstance(life_support_state.get("shortage_stress"), (int, float))
            and not isinstance(life_support_state.get("shortage_stress"), bool)
            else 0.0
        )
        for symbol in LIFE_SUPPORT_SYMBOLS:
            demand = (
                float(demand_by_symbol.get(symbol, 0.0))
                if isinstance(demand_by_symbol.get(symbol, 0.0), (int, float))
                else 0.0
            )
            production = (
                float(production_by_symbol.get(symbol, 0.0))
                if isinstance(production_by_symbol.get(symbol, 0.0), (int, float))
                else 0.0
            )
            pressure = max(0.0, min(1.0, (demand - production) / max(0.01, demand)))
            rows.append(
                self._life_support_market_row(
                    symbol=symbol,
                    bucket=bucket,
                    holdings=holdings.get(symbol, 0.0),
                    shortage_stress=shortage_stress,
                    demand_pressure=pressure,
                    region=region,
                )
            )

        def _market_sort_key(row: dict[str, Any]) -> tuple[int, int, str]:
            market_class = str(row.get("market_class", "element"))
            class_rank = 0 if market_class == "element" else 1
            atomic_raw = row.get("atomic_number")
            atomic_number = int(atomic_raw) if isinstance(atomic_raw, int) else 9999
            symbol = str(row.get("symbol", ""))
            return (class_rank, atomic_number, symbol)

        rows.sort(key=_market_sort_key)

        return {
            "market_tick": bucket,
            "player_id": player_id,
            "wallet": wallet,
            "credits_per_voidcoin": round(self._voidcoin_rate_credits(bucket), 4),
            "total": len(rows),
            "limit": limit,
            "region_id": region_id,
            "items": rows[:limit],
        }

    def _market_price_lookup(
        self,
        symbol: str,
        player_id: str,
        region_id: str | None = None,
    ) -> dict[str, Any]:
        symbol_key = symbol.strip().casefold()
        bucket = self._market_epoch_bucket()
        demand_scores = self._element_demand_scores()
        holdings = self.state_store.get_inventory_amounts(player_id=player_id, symbols=[symbol.strip()])
        region = None
        if isinstance(region_id, str):
            region = self.seed_store.region_index().get(region_id)
            if region is None:
                raise ValueError(f"Unknown region_id '{region_id}'")

        for element in self.seed_store.elements:
            if (
                isinstance(element, dict)
                and isinstance(element.get("symbol"), str)
                and element["symbol"].casefold() == symbol_key
            ):
                return self._element_market_row(
                    element=element,
                    bucket=bucket,
                    demand_score=demand_scores.get(element["symbol"], 0.0),
                    holdings=holdings.get(element["symbol"], 0.0),
                    region=region,
                )
        if symbol.strip() in LIFE_SUPPORT_SYMBOLS:
            life_support_state = self.state_store.get_life_support_state(player_id=player_id)
            life_support_profile = self._life_support_rate_profile(player_id=player_id)
            demand_by_symbol = (
                life_support_profile.get("demand_per_hour", {})
                if isinstance(life_support_profile.get("demand_per_hour"), dict)
                else {}
            )
            production_by_symbol = (
                life_support_profile.get("production_per_hour", {})
                if isinstance(life_support_profile.get("production_per_hour"), dict)
                else {}
            )
            demand = (
                float(demand_by_symbol.get(symbol.strip(), 0.0))
                if isinstance(demand_by_symbol.get(symbol.strip(), 0.0), (int, float))
                else 0.0
            )
            production = (
                float(production_by_symbol.get(symbol.strip(), 0.0))
                if isinstance(production_by_symbol.get(symbol.strip(), 0.0), (int, float))
                else 0.0
            )
            pressure = max(0.0, min(1.0, (demand - production) / max(0.01, demand)))
            shortage_stress = (
                float(life_support_state.get("shortage_stress", 0.0))
                if isinstance(life_support_state.get("shortage_stress"), (int, float))
                and not isinstance(life_support_state.get("shortage_stress"), bool)
                else 0.0
            )
            return self._life_support_market_row(
                symbol=symbol.strip(),
                bucket=bucket,
                holdings=holdings.get(symbol.strip(), 0.0),
                shortage_stress=shortage_stress,
                demand_pressure=pressure,
                region=region,
            )
        raise ValueError(f"Unknown element symbol '{symbol}'")

    def _trade_market(self, payload: dict[str, Any], side: str) -> dict[str, Any]:
        player_id = payload.get("player_id")
        symbol = payload.get("symbol")
        quantity = payload.get("quantity")
        currency = payload.get("currency", "credits")
        region_id = payload.get("region_id")

        if side not in {"buy", "sell"}:
            raise ValueError("Trade side must be buy or sell")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("symbol must be a non-empty string")
        if isinstance(quantity, bool) or not isinstance(quantity, (int, float)):
            raise ValueError("quantity must be numeric")
        quantity_value = float(quantity)
        if quantity_value <= 0:
            raise ValueError("quantity must be > 0")
        if quantity_value > 250_000:
            raise ValueError("quantity must be <= 250000")
        if currency not in {"credits", "voidcoin"}:
            raise ValueError("currency must be one of: credits, voidcoin")
        if region_id is not None and not isinstance(region_id, str):
            raise ValueError("region_id must be a string when provided")

        self._ensure_player_bootstrap(player_id.strip())
        row = self._market_price_lookup(
            symbol=symbol.strip(),
            player_id=player_id.strip(),
            region_id=region_id.strip() if isinstance(region_id, str) else None,
        )
        unit_price = (
            float(row["ask_credits"] if side == "buy" else row["bid_credits"])
            if currency == "credits"
            else float(row["ask_voidcoin"] if side == "buy" else row["bid_voidcoin"])
        )
        gross = unit_price * quantity_value
        fee = gross * 0.01
        net = gross + fee if side == "buy" else max(0.0, gross - fee)

        credits_delta = 0.0
        voidcoin_delta = 0.0
        if currency == "credits":
            credits_delta = -net if side == "buy" else net
        else:
            voidcoin_delta = -net if side == "buy" else net

        element_delta = quantity_value if side == "buy" else -quantity_value
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=credits_delta,
            voidcoin_delta=voidcoin_delta,
            element_deltas={symbol.strip(): element_delta},
        )
        market_class = str(row.get("market_class", "element"))
        asset_type = "commodity" if market_class == "commodity" else "element"
        trade_log = self.state_store.record_market_trade(
            trade_source="spot_buy" if side == "buy" else "spot_sell",
            buyer_player_id=player_id.strip() if side == "buy" else None,
            seller_player_id=player_id.strip() if side == "sell" else None,
            asset_type=asset_type,
            asset_id=symbol.strip(),
            quantity=quantity_value,
            currency=currency,
            unit_price=unit_price,
            gross_total=gross,
            maker_fee=fee if side == "sell" else 0.0,
            taker_fee=fee if side == "buy" else 0.0,
            region_id=row.get("region_id") if isinstance(row.get("region_id"), str) else None,
            metadata={
                "market_tick": row.get("market_tick"),
                "trade_side": side,
                "market_class": market_class,
            },
        )
        price_summary = self.state_store.market_price_summary(
            asset_type=asset_type,
            asset_id=symbol.strip(),
            currency=currency,
        )

        return {
            "trade_id": str(uuid.uuid4()),
            "player_id": player_id.strip(),
            "side": side,
            "currency": currency,
            "symbol": symbol.strip(),
            "asset_type": asset_type,
            "quantity": round(quantity_value, 3),
            "unit_price": round(unit_price, 8 if currency == "voidcoin" else 4),
            "gross_total": round(gross, 8 if currency == "voidcoin" else 4),
            "fee": round(fee, 8 if currency == "voidcoin" else 4),
            "net_total": round(net, 8 if currency == "voidcoin" else 4),
            "region_id": row.get("region_id"),
            "wallet": resource_state["wallet"],
            "inventory_symbol_amount": round(
                float(resource_state["inventory"].get(symbol.strip(), 0.0)),
                3,
            ),
            "market": row,
            "trade_log": trade_log,
            "price_summary": price_summary,
        }

    def _p2p_policy(self) -> dict[str, Any]:
        raw = self.seed_store.p2p_listing_policy
        maker = raw.get("maker_fee_pct", 1.25)
        taker = raw.get("taker_fee_pct", 1.75)
        min_quantity = raw.get("min_quantity", 1)
        max_quantity = raw.get("max_quantity", 100000)
        default_ttl = raw.get("default_ttl_hours", 72)
        supported = raw.get(
            "supported_asset_types",
            [
                "element",
                "module",
                "hull",
                "structure",
                "blueprint",
                "module_instance",
                "hull_instance",
                "planet_deed",
            ],
        )
        return {
            "maker_fee_pct": float(maker) if isinstance(maker, (int, float)) else 1.25,
            "taker_fee_pct": float(taker) if isinstance(taker, (int, float)) else 1.75,
            "min_quantity": float(min_quantity) if isinstance(min_quantity, (int, float)) else 1.0,
            "max_quantity": float(max_quantity) if isinstance(max_quantity, (int, float)) else 100000.0,
            "default_ttl_hours": float(default_ttl) if isinstance(default_ttl, (int, float)) else 72.0,
            "supported_asset_types": [
                item for item in supported if isinstance(item, str)
            ]
            if isinstance(supported, list)
            else [
                "element",
                "module",
                "hull",
                "structure",
                "blueprint",
                "module_instance",
                "hull_instance",
                "planet_deed",
            ],
        }

    def _create_market_listing(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        asset_type = payload.get("asset_type")
        asset_id = payload.get("asset_id")
        quantity = payload.get("quantity")
        currency = payload.get("currency", "credits")
        unit_price = payload.get("unit_price")
        region_id = payload.get("region_id")
        ttl_hours = payload.get("ttl_hours")

        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(asset_type, str) or not asset_type.strip():
            raise ValueError("asset_type must be a non-empty string")
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise ValueError("asset_id must be a non-empty string")
        if isinstance(quantity, bool) or not isinstance(quantity, (int, float)):
            raise ValueError("quantity must be numeric")
        if isinstance(unit_price, bool) or not isinstance(unit_price, (int, float)):
            raise ValueError("unit_price must be numeric")
        if currency not in {"credits", "voidcoin"}:
            raise ValueError("currency must be one of: credits, voidcoin")
        if region_id is not None and not isinstance(region_id, str):
            raise ValueError("region_id must be a string when provided")
        if ttl_hours is not None and (
            isinstance(ttl_hours, bool) or not isinstance(ttl_hours, (int, float))
        ):
            raise ValueError("ttl_hours must be numeric when provided")

        self._ensure_player_bootstrap(player_id.strip())
        if isinstance(region_id, str) and region_id.strip():
            if self.seed_store.region_index().get(region_id.strip()) is None:
                raise ValueError(f"Unknown region_id '{region_id}'")

        policy = self._p2p_policy()
        type_key = asset_type.strip().casefold()
        if type_key not in {item.casefold() for item in policy["supported_asset_types"]}:
            raise ValueError(
                "asset_type not supported by listing policy: "
                + ", ".join(policy["supported_asset_types"])
            )
        quantity_value = float(quantity)
        if quantity_value < float(policy["min_quantity"]) or quantity_value > float(policy["max_quantity"]):
            raise ValueError(
                "quantity must be between {} and {}".format(
                    int(policy["min_quantity"]),
                    int(policy["max_quantity"]),
                )
            )
        if float(unit_price) <= 0:
            raise ValueError("unit_price must be > 0")
        ttl_value = float(ttl_hours) if isinstance(ttl_hours, (int, float)) else float(
            policy["default_ttl_hours"]
        )
        ttl_value = max(1.0, min(168.0, ttl_value))
        listing_metadata: dict[str, Any] = {}

        if type_key == "element":
            self.state_store.apply_resource_delta(
                player_id=player_id.strip(),
                element_deltas={asset_id.strip(): -quantity_value},
            )
        elif type_key in {"module_instance", "hull_instance"}:
            if abs(quantity_value - 1.0) > 1e-9:
                raise ValueError(f"{type_key} listings must use quantity = 1")
            expected_asset_type = "module" if type_key == "module_instance" else "hull"
            self._ensure_market_escrow_profile()
            instance = self.state_store.get_crafted_instance(
                player_id=player_id.strip(),
                instance_id=asset_id.strip(),
            )
            if str(instance.get("asset_type")) != expected_asset_type:
                raise ValueError(
                    f"Instance '{asset_id}' is '{instance.get('asset_type')}', expected '{expected_asset_type}'"
                )
            moved = self.state_store.transfer_crafted_instance_owner(
                instance_id=asset_id.strip(),
                from_player_id=player_id.strip(),
                to_player_id=self.market_escrow_player_id,
            )
            quantity_value = 1.0
            listing_metadata = {
                "instance_id": str(moved.get("instance_id", asset_id.strip())),
                "base_asset_type": str(moved.get("asset_type", expected_asset_type)),
                "base_asset_id": str(moved.get("asset_id", "")),
                "quality_tier": str(moved.get("quality_tier", "standard")),
                "quality_score": float(moved.get("quality_score", 1.0)),
                "stat_multiplier": float(moved.get("stat_multiplier", 1.0)),
                "item_level": int(moved.get("item_level", 1))
                if isinstance(moved.get("item_level"), int)
                else 1,
                "synergy_tags": moved.get("synergy_tags", [])
                if isinstance(moved.get("synergy_tags"), list)
                else [],
            }
        elif type_key == "planet_deed":
            if abs(quantity_value - 1.0) > 1e-9:
                raise ValueError("planet_deed listings must use quantity = 1")
            self._ensure_market_escrow_profile()
            world = self.state_store.get_world(
                world_id=asset_id.strip(),
                player_id=player_id.strip(),
            )
            transferred_world = self.state_store.transfer_world_ownership(
                world_id=asset_id.strip(),
                from_player_id=player_id.strip(),
                to_player_id=self.market_escrow_player_id,
            )
            quantity_value = 1.0
            listing_metadata = {
                "world_id": str(transferred_world.get("world_id", asset_id.strip())),
                "world_name": str(transferred_world.get("name", world.get("name", ""))),
                "body_class": str(transferred_world.get("body_class", world.get("body_class", ""))),
                "rarity_score": float(transferred_world.get("rarity_score", world.get("rarity_score", 0.0))),
                "habitability_index": float(
                    transferred_world.get("habitability_index", world.get("habitability_index", 0.0))
                ),
            }
        else:
            if abs(quantity_value - round(quantity_value)) > 1e-9:
                raise ValueError("quantity must be an integer for non-element listings")
            self.state_store.adjust_asset_quantity(
                player_id=player_id.strip(),
                asset_type=type_key,
                asset_id=asset_id.strip(),
                quantity_delta=-int(round(quantity_value)),
            )

        listing = self.state_store.create_listing(
            seller_player_id=player_id.strip(),
            asset_type=type_key,
            asset_id=asset_id.strip(),
            quantity=quantity_value,
            currency=currency,
            unit_price=float(unit_price),
            region_id=region_id.strip() if isinstance(region_id, str) and region_id.strip() else None,
            ttl_hours=ttl_value,
            metadata=listing_metadata,
        )
        price_guidance = self.state_store.market_price_summary(
            asset_type=type_key,
            asset_id=asset_id.strip(),
            currency=currency,
            lookback_limit=200,
        )
        return {
            "ok": True,
            "listing": listing,
            "policy": policy,
            "price_guidance": price_guidance,
        }

    def _cancel_market_listing(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        listing_id = payload.get("listing_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(listing_id, str) or not listing_id.strip():
            raise ValueError("listing_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())

        listing_before = self.state_store.get_listing(listing_id=listing_id.strip())
        if listing_before.get("status") != "active":
            raise ValueError("Only active listings can be cancelled")
        cancelled = self.state_store.cancel_listing(
            seller_player_id=player_id.strip(),
            listing_id=listing_id.strip(),
        )
        remaining = float(cancelled.get("quantity_remaining", 0.0))
        refunded: dict[str, Any] = {"type": cancelled.get("asset_type"), "amount": 0}
        asset_type = str(cancelled.get("asset_type", ""))
        asset_id = str(cancelled.get("asset_id", ""))
        metadata = cancelled.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        if remaining > 0:
            if asset_type == "element":
                self.state_store.apply_resource_delta(
                    player_id=player_id.strip(),
                    element_deltas={asset_id: remaining},
                )
                refunded["amount"] = round(remaining, 3)
            elif asset_type in {"module_instance", "hull_instance"}:
                self._ensure_market_escrow_profile()
                instance_id = metadata.get("instance_id")
                if not isinstance(instance_id, str) or not instance_id.strip():
                    instance_id = asset_id
                moved = self.state_store.transfer_crafted_instance_owner(
                    instance_id=instance_id.strip(),
                    from_player_id=self.market_escrow_player_id,
                    to_player_id=player_id.strip(),
                )
                refunded["amount"] = 1
                refunded["instance"] = {
                    "instance_id": str(moved.get("instance_id", instance_id)),
                    "asset_type": str(moved.get("asset_type", asset_type)),
                    "asset_id": str(moved.get("asset_id", "")),
                    "quality_tier": str(moved.get("quality_tier", "standard")),
                    "quality_score": float(moved.get("quality_score", 1.0)),
                    "stat_multiplier": float(moved.get("stat_multiplier", 1.0)),
                }
            elif asset_type == "planet_deed":
                self._ensure_market_escrow_profile()
                world_id = metadata.get("world_id")
                if not isinstance(world_id, str) or not world_id.strip():
                    world_id = asset_id
                world = self.state_store.transfer_world_ownership(
                    world_id=world_id.strip(),
                    from_player_id=self.market_escrow_player_id,
                    to_player_id=player_id.strip(),
                )
                refunded["amount"] = 1
                refunded["world"] = {
                    "world_id": str(world.get("world_id", world_id)),
                    "name": str(world.get("name", "")),
                    "body_class": str(world.get("body_class", "")),
                }
            else:
                qty_refund = int(max(0, round(remaining)))
                if qty_refund > 0:
                    updated = self.state_store.adjust_asset_quantity(
                        player_id=player_id.strip(),
                        asset_type=asset_type,
                        asset_id=asset_id,
                        quantity_delta=qty_refund,
                    )
                    refunded["amount"] = qty_refund
                    refunded["asset_quantity_after"] = int(updated.get("quantity", 0))

        return {
            "ok": True,
            "listing_before": listing_before,
            "listing_after": cancelled,
            "refund": refunded,
        }

    def _buy_market_listing(self, payload: dict[str, Any]) -> dict[str, Any]:
        buyer_player_id = payload.get("player_id")
        listing_id = payload.get("listing_id")
        quantity = payload.get("quantity")
        if not isinstance(buyer_player_id, str) or not buyer_player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(listing_id, str) or not listing_id.strip():
            raise ValueError("listing_id must be a non-empty string")
        if isinstance(quantity, bool) or not isinstance(quantity, (int, float)):
            raise ValueError("quantity must be numeric")
        quantity_value = float(quantity)
        if quantity_value <= 0:
            raise ValueError("quantity must be > 0")

        self._ensure_player_bootstrap(buyer_player_id.strip())
        listing_before = self.state_store.get_listing(listing_id=listing_id.strip())
        if listing_before.get("status") != "active":
            raise ValueError("Listing is not active")
        seller_player_id = str(listing_before.get("seller_player_id", ""))
        if seller_player_id == buyer_player_id.strip():
            raise ValueError("Cannot buy your own listing")

        remaining = float(listing_before.get("quantity_remaining", 0.0))
        if quantity_value > remaining + 1e-9:
            raise ValueError("Requested quantity exceeds listing quantity_remaining")
        asset_type = str(listing_before.get("asset_type", ""))
        asset_id = str(listing_before.get("asset_id", ""))
        currency = str(listing_before.get("currency", "credits"))
        metadata = listing_before.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        unit_price = float(listing_before.get("unit_price", 0.0))
        if unit_price <= 0:
            raise ValueError("Listing unit_price is invalid")
        if currency not in {"credits", "voidcoin"}:
            raise ValueError("Listing currency must be credits or voidcoin")
        if asset_type != "element" and abs(quantity_value - round(quantity_value)) > 1e-9:
            raise ValueError("quantity must be an integer for non-element listings")

        policy = self._p2p_policy()
        gross = unit_price * quantity_value
        maker_fee = gross * (float(policy["maker_fee_pct"]) / 100.0)
        taker_fee = gross * (float(policy["taker_fee_pct"]) / 100.0)
        buyer_total = gross + taker_fee
        seller_net = max(0.0, gross - maker_fee)

        buyer_credits_delta = 0.0
        buyer_voidcoin_delta = 0.0
        seller_credits_delta = 0.0
        seller_voidcoin_delta = 0.0
        if currency == "credits":
            buyer_credits_delta = -buyer_total
            seller_credits_delta = seller_net
        else:
            buyer_voidcoin_delta = -buyer_total
            seller_voidcoin_delta = seller_net

        if asset_type == "element":
            buyer_state = self.state_store.apply_resource_delta(
                player_id=buyer_player_id.strip(),
                credits_delta=buyer_credits_delta,
                voidcoin_delta=buyer_voidcoin_delta,
                element_deltas={asset_id: quantity_value},
            )
            seller_wallet = self.state_store.adjust_wallet(
                player_id=seller_player_id,
                credits_delta=seller_credits_delta,
                voidcoin_delta=seller_voidcoin_delta,
            )
            buyer_asset_state: dict[str, Any] = {
                "inventory_symbol_amount": round(
                    float(buyer_state["inventory"].get(asset_id, 0.0)),
                    3,
                )
            }
        elif asset_type in {"module_instance", "hull_instance"}:
            if abs(quantity_value - 1.0) > 1e-9:
                raise ValueError(f"{asset_type} listings can only be bought with quantity = 1")
            buyer_wallet = self.state_store.adjust_wallet(
                player_id=buyer_player_id.strip(),
                credits_delta=buyer_credits_delta,
                voidcoin_delta=buyer_voidcoin_delta,
            )
            seller_wallet = self.state_store.adjust_wallet(
                player_id=seller_player_id,
                credits_delta=seller_credits_delta,
                voidcoin_delta=seller_voidcoin_delta,
            )
            self._ensure_market_escrow_profile()
            instance_id = metadata.get("instance_id")
            if not isinstance(instance_id, str) or not instance_id.strip():
                instance_id = asset_id
            moved = self.state_store.transfer_crafted_instance_owner(
                instance_id=instance_id.strip(),
                from_player_id=self.market_escrow_player_id,
                to_player_id=buyer_player_id.strip(),
            )
            buyer_state = {"wallet": buyer_wallet, "inventory": {}}
            buyer_asset_state = {
                "instance_id": str(moved.get("instance_id", instance_id)),
                "asset_type": str(moved.get("asset_type", asset_type)),
                "asset_id": str(moved.get("asset_id", "")),
                "quality_tier": str(moved.get("quality_tier", "standard")),
                "quality_score": float(moved.get("quality_score", 1.0)),
                "stat_multiplier": float(moved.get("stat_multiplier", 1.0)),
            }
        elif asset_type == "planet_deed":
            if abs(quantity_value - 1.0) > 1e-9:
                raise ValueError("planet_deed listings can only be bought with quantity = 1")
            buyer_wallet = self.state_store.adjust_wallet(
                player_id=buyer_player_id.strip(),
                credits_delta=buyer_credits_delta,
                voidcoin_delta=buyer_voidcoin_delta,
            )
            seller_wallet = self.state_store.adjust_wallet(
                player_id=seller_player_id,
                credits_delta=seller_credits_delta,
                voidcoin_delta=seller_voidcoin_delta,
            )
            self._ensure_market_escrow_profile()
            world_id = metadata.get("world_id")
            if not isinstance(world_id, str) or not world_id.strip():
                world_id = asset_id
            moved_world = self.state_store.transfer_world_ownership(
                world_id=world_id.strip(),
                from_player_id=self.market_escrow_player_id,
                to_player_id=buyer_player_id.strip(),
            )
            buyer_state = {"wallet": buyer_wallet, "inventory": {}}
            buyer_asset_state = {
                "world_id": str(moved_world.get("world_id", world_id)),
                "world_name": str(moved_world.get("name", "")),
                "body_class": str(moved_world.get("body_class", "")),
            }
        else:
            buyer_wallet = self.state_store.adjust_wallet(
                player_id=buyer_player_id.strip(),
                credits_delta=buyer_credits_delta,
                voidcoin_delta=buyer_voidcoin_delta,
            )
            seller_wallet = self.state_store.adjust_wallet(
                player_id=seller_player_id,
                credits_delta=seller_credits_delta,
                voidcoin_delta=seller_voidcoin_delta,
            )
            updated_asset = self.state_store.adjust_asset_quantity(
                player_id=buyer_player_id.strip(),
                asset_type=asset_type,
                asset_id=asset_id,
                quantity_delta=int(round(quantity_value)),
            )
            buyer_state = {"wallet": buyer_wallet, "inventory": {}}
            buyer_asset_state = {"asset_quantity": int(updated_asset.get("quantity", 0))}

        listing_after = self.state_store.update_listing_remaining(
            listing_id=listing_id.strip(),
            remaining=max(0.0, remaining - quantity_value),
        )
        trade_log = self.state_store.record_market_trade(
            trade_source="listing",
            buyer_player_id=buyer_player_id.strip(),
            seller_player_id=seller_player_id,
            asset_type=asset_type,
            asset_id=asset_id,
            quantity=quantity_value,
            currency=currency,
            unit_price=unit_price,
            gross_total=gross,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            region_id=str(listing_before.get("region_id")) if isinstance(listing_before.get("region_id"), str) else None,
            listing_id=str(listing_before.get("listing_id", "")),
            metadata=metadata,
        )
        price_summary = self.state_store.market_price_summary(
            asset_type=asset_type,
            asset_id=asset_id,
            currency=currency,
            lookback_limit=200,
        )
        return {
            "ok": True,
            "transaction_id": str(uuid.uuid4()),
            "listing_before": listing_before,
            "listing_after": listing_after,
            "buyer_player_id": buyer_player_id.strip(),
            "seller_player_id": seller_player_id,
            "asset_type": asset_type,
            "asset_id": asset_id,
            "currency": currency,
            "quantity": round(quantity_value, 3),
            "unit_price": round(unit_price, 8 if currency == "voidcoin" else 4),
            "gross_total": round(gross, 8 if currency == "voidcoin" else 4),
            "fees": {
                "maker_fee": round(maker_fee, 8 if currency == "voidcoin" else 4),
                "taker_fee": round(taker_fee, 8 if currency == "voidcoin" else 4),
                "maker_fee_pct": round(float(policy["maker_fee_pct"]), 4),
                "taker_fee_pct": round(float(policy["taker_fee_pct"]), 4),
            },
            "buyer_total": round(buyer_total, 8 if currency == "voidcoin" else 4),
            "seller_net": round(seller_net, 8 if currency == "voidcoin" else 4),
            "buyer_wallet": buyer_state["wallet"],
            "seller_wallet": seller_wallet,
            "buyer_asset_state": buyer_asset_state,
            "trade_log": trade_log,
            "price_summary": price_summary,
        }

    def _exchange_currency(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        direction = payload.get("direction")
        amount = payload.get("amount")

        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if direction not in {"buy_voidcoin", "sell_voidcoin"}:
            raise ValueError("direction must be one of: buy_voidcoin, sell_voidcoin")
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            raise ValueError("amount must be numeric")
        amount_value = float(amount)
        if amount_value <= 0:
            raise ValueError("amount must be > 0")

        self._ensure_player_bootstrap(player_id.strip())
        bucket = self._market_epoch_bucket()
        credits_per_voidcoin = self._voidcoin_rate_credits(bucket)
        fee_rate = 0.01

        if direction == "buy_voidcoin":
            credits_spent = amount_value
            voidcoin_gained = (credits_spent / credits_per_voidcoin) * (1.0 - fee_rate)
            resource_state = self.state_store.apply_resource_delta(
                player_id=player_id.strip(),
                credits_delta=-credits_spent,
                voidcoin_delta=voidcoin_gained,
            )
            return {
                "exchange_id": str(uuid.uuid4()),
                "direction": direction,
                "credits_per_voidcoin": round(credits_per_voidcoin, 4),
                "credits_spent": round(credits_spent, 4),
                "voidcoin_gained": round(voidcoin_gained, 8),
                "wallet": resource_state["wallet"],
            }

        voidcoin_spent = amount_value
        credits_gained = (voidcoin_spent * credits_per_voidcoin) * (1.0 - fee_rate)
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=credits_gained,
            voidcoin_delta=-voidcoin_spent,
        )
        return {
            "exchange_id": str(uuid.uuid4()),
            "direction": direction,
            "credits_per_voidcoin": round(credits_per_voidcoin, 4),
            "voidcoin_spent": round(voidcoin_spent, 8),
            "credits_gained": round(credits_gained, 4),
            "wallet": resource_state["wallet"],
        }

    def _catalog_lookup_item(self, item_id: str) -> tuple[str, dict[str, Any]]:
        if item_id.startswith("module."):
            item = self.seed_store.module_index().get(item_id)
            if item is None:
                raise ValueError(f"Unknown module id '{item_id}'")
            return "module", item
        if item_id.startswith("hull."):
            item = self.seed_store.hull_index().get(item_id)
            if item is None:
                raise ValueError(f"Unknown hull id '{item_id}'")
            return "hull", item
        if item_id.startswith("structure."):
            item = self.seed_store.structure_index().get(item_id)
            if item is None:
                raise ValueError(f"Unknown structure id '{item_id}'")
            return "structure", item
        if item_id.startswith("tech."):
            item = self.seed_store.tech_index().get(item_id)
            if item is None:
                raise ValueError(f"Unknown tech id '{item_id}'")
            return "tech", item
        raise ValueError("item_id must start with module., hull., structure., or tech.")

    def _normalize_cost(self, cost: dict[str, Any]) -> dict[str, Any]:
        credits_raw = cost.get("credits", 0)
        if isinstance(credits_raw, bool) or not isinstance(credits_raw, (int, float)):
            raise ValueError("Cost credits must be numeric")
        credits = max(0.0, float(credits_raw))
        elements_raw = cost.get("elements", [])
        if not isinstance(elements_raw, list):
            raise ValueError("Cost elements must be an array")
        normalized_elements: list[dict[str, Any]] = []
        for row in elements_raw:
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol")
            amount_raw = row.get("amount")
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            if isinstance(amount_raw, bool) or not isinstance(amount_raw, (int, float)):
                continue
            amount = float(amount_raw)
            if amount <= 0:
                continue
            normalized_elements.append(
                {"symbol": symbol.strip(), "amount": round(amount, 3)}
            )
        return {"credits": round(credits, 4), "elements": normalized_elements}

    def _ordered_elements_by_atomic(self) -> list[dict[str, Any]]:
        rows = [
            row
            for row in self.seed_store.elements
            if isinstance(row, dict)
            and isinstance(row.get("symbol"), str)
            and isinstance(row.get("atomic_number"), int)
        ]
        rows.sort(key=lambda row: (int(row.get("atomic_number", 999)), str(row.get("symbol", ""))))
        return rows

    def _tech_signature_element_symbol(self, tech_id: str) -> str:
        ordered = self._ordered_elements_by_atomic()
        symbols = [str(row.get("symbol")) for row in ordered if isinstance(row.get("symbol"), str)]
        if not symbols:
            return "Si"
        tech_ids = sorted(
            str(row.get("id"))
            for row in self.seed_store.tech_tree
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        )
        try:
            idx = tech_ids.index(tech_id)
        except ValueError:
            idx = sum((i + 1) * ord(ch) for i, ch in enumerate(tech_id))
        return symbols[idx % len(symbols)]

    def _element_scaled_amount(self, symbol: str, base_amount: float) -> int:
        amount = max(1.0, float(base_amount))
        element = self.seed_store.elements_by_symbol().get(symbol, {})
        atomic_raw = element.get("atomic_number")
        atomic = int(atomic_raw) if isinstance(atomic_raw, int) else 0
        if atomic >= 112:
            amount *= 0.06
        elif atomic >= 104:
            amount *= 0.09
        elif atomic >= 89:
            amount *= 0.14
        elif atomic >= 57:
            amount *= 0.32
        elif atomic >= 37:
            amount *= 0.55
        return max(1, int(round(amount)))

    def _tech_research_cost(self, node: dict[str, Any]) -> dict[str, Any]:
        tech_id = str(node.get("id", "tech.unknown"))
        tier = int(node.get("tier", 1))
        rp_cost = int(node.get("rp_cost", 100))
        branch = str(node.get("branch", "power_systems"))
        branch_elements = {
            "weapons": ["Fe", "Cu", "C"],
            "power_systems": ["Li", "Ni", "C"],
            "shield_systems": ["Si", "B", "Al"],
            "stealth_systems": ["C", "Si", "Ge"],
            "armor_systems": ["Fe", "Ti", "Cr"],
            "defensive_systems": ["Al", "Ti", "Ni"],
            "sensor_systems": ["Si", "Ga", "Ge"],
            "planetary_construction": ["Fe", "Al", "Si"],
            "faction_infrastructure": ["Fe", "Cu", "Ni"],
        }
        branch_rare = {
            "weapons": "W",
            "power_systems": "U",
            "shield_systems": "Hf",
            "stealth_systems": "Pt",
            "armor_systems": "Mo",
            "defensive_systems": "V",
            "sensor_systems": "Pd",
            "planetary_construction": "Ti",
            "faction_infrastructure": "Au",
        }
        branch_trace = {
            "weapons": ["Mn", "V", "Mo"],
            "power_systems": ["Zr", "Nb", "Be"],
            "shield_systems": ["Y", "Zr", "La"],
            "stealth_systems": ["In", "Te", "Se"],
            "armor_systems": ["Mn", "V", "Ni"],
            "defensive_systems": ["B", "Sc", "Y"],
            "sensor_systems": ["In", "Ag", "Ta"],
            "planetary_construction": ["Ca", "Na", "K"],
            "faction_infrastructure": ["Mn", "Zn", "Ag"],
        }
        core_symbols = branch_elements.get(branch, ["Fe", "Si", "Cu"])
        base_credits = rp_cost * (42 + (tier * 10))

        amounts_by_symbol: dict[str, int] = {}

        def add(symbol: str, amount: int) -> None:
            if amount <= 0:
                return
            amounts_by_symbol[symbol] = amounts_by_symbol.get(symbol, 0) + int(amount)

        for idx, symbol in enumerate(core_symbols):
            weight = len(core_symbols) - idx
            amount = int(round((rp_cost / 16.0) * (0.18 + (0.23 * weight)) * (1 + (tier * 0.15))))
            add(symbol, amount)

        if tier >= 4:
            rare_symbol = branch_rare.get(branch, "Mo")
            rare_base = (rp_cost / 300.0) * (1.0 + max(0, tier - 4) * 0.55)
            add(rare_symbol, self._element_scaled_amount(rare_symbol, rare_base))

        if tier >= 2:
            for symbol in branch_trace.get(branch, []):
                trace_base = (rp_cost / 2600.0) * (1.0 + (tier * 0.09))
                add(symbol, self._element_scaled_amount(symbol, trace_base))

        if branch == "power_systems" and any(
            tag in tech_id
            for tag in ("reactor", "fission", "fusion", "antimatter", "casimir", "plasma_casimir")
        ):
            nuclear_symbol = "U" if tier <= 6 else "Pu"
            nuclear_base = rp_cost / (640.0 if tier <= 6 else 860.0)
            add(nuclear_symbol, self._element_scaled_amount(nuclear_symbol, nuclear_base))

        if any(tag in tech_id for tag in ("quantum", "processor", "photonic", "sensor", "scanner", "er_epr")):
            add("Si", max(8, int(round(rp_cost / 92.0))))
            add("Ge", self._element_scaled_amount("Ge", rp_cost / 320.0))
            add("Ga", self._element_scaled_amount("Ga", rp_cost / 420.0))

        signature_symbol = self._tech_signature_element_symbol(tech_id=tech_id)
        signature_base = (rp_cost / 460.0) * (0.75 + (tier * 0.08))
        add(signature_symbol, self._element_scaled_amount(signature_symbol, signature_base))

        tech_key = tech_id.casefold()
        if any(
            token in tech_key
            for token in (
                "life_support",
                "habitat",
                "hydroponic",
                "biosphere",
                "water",
                "atmospheric",
                "food",
                "crew",
                "population",
            )
        ):
            life_support_symbols = {
                "H": rp_cost / 130.0,
                "O": rp_cost / 140.0,
                "N": rp_cost / 210.0,
                "C": rp_cost / 185.0,
                "P": rp_cost / 420.0,
                "K": rp_cost / 470.0,
            }
            for symbol, amount in life_support_symbols.items():
                add(symbol, self._element_scaled_amount(symbol, amount))

        if any(
            token in tech_key
            for token in (
                "print",
                "printer",
                "fabrication",
                "foundry",
                "nanoforge",
                "nanofab",
                "additive",
                "manufactur",
            )
        ):
            additive_symbols = {
                "Fe": rp_cost / 260.0,
                "Al": rp_cost / 320.0,
                "Ti": rp_cost / 540.0,
                "C": rp_cost / 380.0,
                "Si": rp_cost / 440.0,
            }
            for symbol, amount in additive_symbols.items():
                add(symbol, self._element_scaled_amount(symbol, amount))

        signature_element = self.seed_store.elements_by_symbol().get(signature_symbol, {})
        signature_atomic = signature_element.get("atomic_number")
        if isinstance(signature_atomic, int) and signature_atomic >= 110:
            # Superheavy "paper elements" require large precursor masses to isolate usable isotopic traces.
            precursor = "Bi" if tier >= 7 else "Pb"
            add(precursor, max(6, int(round((rp_cost / 120.0) * (1.0 + (tier * 0.05))))))

        ordered_elements = self._ordered_elements_by_atomic()
        order_index = {
            str(row.get("symbol")): idx
            for idx, row in enumerate(ordered_elements)
            if isinstance(row.get("symbol"), str)
        }
        elements = [
            {"symbol": symbol, "amount": int(amount)}
            for symbol, amount in amounts_by_symbol.items()
            if amount > 0
        ]
        elements.sort(key=lambda row: order_index.get(str(row.get("symbol")), 9999))
        return {"credits": int(base_credits), "elements": elements}

    def _scale_cost(self, base_cost: dict[str, Any], quantity: int) -> dict[str, Any]:
        normalized = self._normalize_cost(base_cost)
        if quantity <= 1:
            return normalized
        return {
            "credits": round(float(normalized["credits"]) * quantity, 4),
            "elements": [
                {"symbol": row["symbol"], "amount": round(float(row["amount"]) * quantity, 3)}
                for row in normalized["elements"]
            ],
        }

    def _base_cost_for_item(
        self, item_kind: str, item: dict[str, Any], item_id: str
    ) -> dict[str, Any]:
        if item_kind == "tech":
            return self._tech_research_cost(item)
        build_cost = item.get("build_cost")
        if not isinstance(build_cost, dict):
            raise ValueError(f"Item '{item_id}' does not define build_cost")
        return build_cost

    def _cost_delta_summary(
        self,
        base_cost: dict[str, Any],
        override_cost: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_base = self._normalize_cost(base_cost)
        normalized_override = self._normalize_cost(override_cost)
        base_elements = {
            row["symbol"]: float(row["amount"])
            for row in normalized_base["elements"]
        }
        override_elements = {
            row["symbol"]: float(row["amount"])
            for row in normalized_override["elements"]
        }
        element_deltas: list[dict[str, Any]] = []
        for symbol in sorted(set(base_elements.keys()) | set(override_elements.keys())):
            base_amount = base_elements.get(symbol, 0.0)
            override_amount = override_elements.get(symbol, 0.0)
            delta = round(override_amount - base_amount, 3)
            if abs(delta) < 1e-9:
                continue
            element_deltas.append(
                {
                    "symbol": symbol,
                    "base_amount": round(base_amount, 3),
                    "override_amount": round(override_amount, 3),
                    "delta": delta,
                    "direction": "increase" if delta > 0 else "decrease",
                }
            )
        element_deltas.sort(key=lambda row: (-abs(float(row["delta"])), row["symbol"]))
        return {
            "credits_delta": round(
                float(normalized_override["credits"]) - float(normalized_base["credits"]),
                4,
            ),
            "elements": element_deltas,
        }

    def _summarize_substitution(self, substitution: dict[str, Any]) -> dict[str, Any]:
        summary = {
            "id": substitution.get("id"),
            "item_id": substitution.get("item_id"),
            "name": substitution.get("name"),
            "description": substitution.get("description"),
            "tradeoff_notes": (
                substitution.get("tradeoff_notes")
                if isinstance(substitution.get("tradeoff_notes"), list)
                else []
            ),
            "override_cost": self._normalize_cost(substitution.get("override_cost", {})),
        }
        item_id = summary.get("item_id")
        if not isinstance(item_id, str):
            return summary
        try:
            item_kind, item = self._catalog_lookup_item(item_id=item_id)
            summary["item_kind"] = item_kind
            summary["item_name"] = item.get("name", item_id)
            base_cost = self._base_cost_for_item(
                item_kind=item_kind,
                item=item,
                item_id=item_id,
            )
            summary["cost_delta"] = self._cost_delta_summary(
                base_cost=base_cost,
                override_cost=summary["override_cost"],
            )
        except ValueError:
            # Keep substitution seed-readable even if item metadata cannot be resolved.
            pass
        return summary

    def _substitution_matches_search(
        self,
        substitution: dict[str, Any],
        query: str,
    ) -> bool:
        query_key = query.strip().casefold()
        if not query_key:
            return True
        haystacks: list[str] = []
        for key in ("id", "item_id", "name", "description"):
            value = substitution.get(key)
            if isinstance(value, str):
                haystacks.append(value)
        notes = substitution.get("tradeoff_notes")
        if isinstance(notes, list):
            for note in notes:
                if isinstance(note, str):
                    haystacks.append(note)
        item_id = substitution.get("item_id")
        if isinstance(item_id, str):
            try:
                _, item = self._catalog_lookup_item(item_id=item_id)
                item_name = item.get("name")
                if isinstance(item_name, str):
                    haystacks.append(item_name)
            except ValueError:
                pass
        return any(query_key in value.casefold() for value in haystacks)

    def _player_population_totals(self, player_id: str) -> dict[str, float]:
        worlds = self.state_store.list_worlds_for_player(player_id=player_id)
        structure_index = self.seed_store.structure_index()
        totals = {
            "current": 0.0,
            "capacity": 0.0,
            "growth_pct": 0.0,
            "cargo_support_pct": 0.0,
            "crew_support_pct": 0.0,
        }
        for world in worlds:
            if not isinstance(world, dict):
                continue
            current = world.get("population_current")
            capacity = world.get("population_capacity")
            growth = world.get("population_growth_per_day_pct")
            if isinstance(current, (int, float)) and not isinstance(current, bool):
                totals["current"] += max(0.0, float(current))
            if isinstance(capacity, (int, float)) and not isinstance(capacity, bool):
                totals["capacity"] += max(0.0, float(capacity))
            if isinstance(growth, (int, float)) and not isinstance(growth, bool):
                totals["growth_pct"] += max(0.0, float(growth))

            for structure_id in world.get("built_structures", []):
                if not isinstance(structure_id, str):
                    continue
                structure = structure_index.get(structure_id)
                if not isinstance(structure, dict):
                    continue
                modifiers = structure.get("modifiers", {})
                if not isinstance(modifiers, dict):
                    continue
                for key, total_key in (
                    ("population_capacity", "capacity"),
                    ("population_growth_pct", "growth_pct"),
                    ("cargo_throughput_pct", "cargo_support_pct"),
                    ("crew_support_pct", "crew_support_pct"),
                ):
                    raw = modifiers.get(key)
                    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                        totals[total_key] += max(0.0, float(raw))
        return totals

    def _resolve_crafting_substitution(
        self, item_id: str, substitution_id: str | None
    ) -> dict[str, Any] | None:
        if substitution_id is None:
            return None
        if not substitution_id.strip():
            raise ValueError("substitution_id cannot be empty")
        substitution = self.seed_store.substitution_index().get(substitution_id.strip())
        if substitution is None:
            raise ValueError(f"Unknown substitution_id '{substitution_id}'")
        if substitution.get("item_id") != item_id:
            raise ValueError(
                f"Substitution '{substitution_id}' is not valid for item '{item_id}'"
            )
        return substitution

    def _available_substitutions_for_item(self, item_id: str) -> list[dict[str, Any]]:
        rows = self.seed_store.substitutions_for_item(item_id=item_id)
        return [self._summarize_substitution(row) for row in rows]

    def _crafting_requirements(
        self,
        player_id: str,
        item_kind: str,
        item: dict[str, Any],
        world_id: str | None,
    ) -> dict[str, Any]:
        unlocked = set(self.state_store.list_unlocked_tech(player_id=player_id))
        requirements: list[str] = []
        missing: list[str] = []
        notes: list[str] = []

        if item_kind in {"module", "hull"}:
            required_tech = item.get("required_tech")
            if isinstance(required_tech, str):
                requirements.append(required_tech)
                if required_tech not in unlocked:
                    missing.append(required_tech)

        if item_kind == "structure":
            for prereq in item.get("prerequisites", []):
                if isinstance(prereq, str):
                    requirements.append(prereq)
                    if prereq not in unlocked:
                        missing.append(prereq)
            if not world_id:
                notes.append("world_id is required to build structures.")
            else:
                world = self.state_store.get_world(world_id=world_id, player_id=player_id)
                domain = item.get("domain")
                if (
                    isinstance(domain, str)
                    and isinstance(world.get("body_class"), str)
                    and domain not in {"any", world["body_class"]}
                ):
                    notes.append(
                        f"Structure domain '{domain}' incompatible with world body_class '{world['body_class']}'."
                    )
                built_ids = set(world.get("built_structures", []))
                if isinstance(item.get("id"), str) and item["id"] in built_ids:
                    notes.append("Structure already built on this world.")

        if item_kind == "tech":
            item_id = item.get("id")
            if isinstance(item_id, str) and self.state_store.is_tech_unlocked(player_id, item_id):
                notes.append("Tech already unlocked.")
            for prereq in item.get("prerequisites", []):
                if isinstance(prereq, str):
                    requirements.append(prereq)
                    if prereq not in unlocked:
                        missing.append(prereq)
            required_race_ids = item.get("required_race_ids", [])
            if isinstance(required_race_ids, list) and required_race_ids:
                profile = self.state_store.get_profile(player_id=player_id)
                race_id = profile.get("race_id")
                if not isinstance(race_id, str) or race_id not in required_race_ids:
                    notes.append(
                        "Tech requires one of race ids: {}".format(
                            ", ".join(str(r) for r in required_race_ids if isinstance(r, str))
                        )
                    )

        if (not self._has_admin_privileges(player_id)) and item_kind in {"module", "hull", "tech"}:
            tier_raw = item.get("tier", 1)
            tier = int(tier_raw) if isinstance(tier_raw, int) else 1
            if tier >= 6:
                population = self._player_population_totals(player_id=player_id)
                needed_population = float(max(8_000, tier * 7_000))
                if float(population["current"]) + 1e-9 < needed_population:
                    notes.append(
                        "Population support too low for tier {} item (need {:.0f}, have {:.0f}).".format(
                            tier, needed_population, float(population["current"])
                        )
                    )
                if item_kind == "hull":
                    needed_cargo_support = float(max(24.0, tier * 8.0))
                    if float(population["cargo_support_pct"]) + 1e-9 < needed_cargo_support:
                        notes.append(
                            "Cargo infrastructure too low for tier {} hull (need {:.1f} cargo support, have {:.1f}).".format(
                                tier,
                                needed_cargo_support,
                                float(population["cargo_support_pct"]),
                            )
                        )

        return {
            "required_tech": sorted(set(requirements)),
            "missing_tech": sorted(set(missing)),
            "notes": notes,
        }

    def _crafting_quote(
        self,
        player_id: str,
        item_id: str,
        quantity: int,
        world_id: str | None = None,
        substitution_id: str | None = None,
    ) -> dict[str, Any]:
        item_kind, item = self._catalog_lookup_item(item_id=item_id)
        selected_substitution = self._resolve_crafting_substitution(
            item_id=item_id, substitution_id=substitution_id
        )
        default_cost = self._base_cost_for_item(
            item_kind=item_kind,
            item=item,
            item_id=item_id,
        )
        if selected_substitution is not None:
            base_cost = selected_substitution.get("override_cost", {})
        else:
            base_cost = default_cost
        scaled_cost = self._scale_cost(base_cost=base_cost, quantity=quantity)
        available_substitutions = self._available_substitutions_for_item(item_id=item_id)
        requirements = self._crafting_requirements(
            player_id=player_id,
            item_kind=item_kind,
            item=item,
            world_id=world_id,
        )
        storage_notes: list[str] = []
        storage_preview: dict[str, Any] | None = None
        if item_kind in {"module", "hull"}:
            added_slots = self._estimate_personal_slot_delta_for_asset_add(
                player_id=player_id,
                asset_type=item_kind,
                asset_id=item_id,
                quantity=quantity,
            )
            added_instances = min(quantity, 24)
            storage_profile = self._compute_storage_profile(player_id=player_id)
            personal = storage_profile.get("personal", {})
            used = float(personal.get("used_slots", 0.0)) if isinstance(personal, dict) else 0.0
            capacity = (
                float(personal.get("capacity_slots", 0.0))
                if isinstance(personal, dict)
                else 0.0
            )
            projected_used = used + added_slots + added_instances
            if projected_used > capacity + 1e-9:
                overflow = projected_used - capacity
                storage_notes.append(
                    "Personal storage overflow {:.2f} slots; trash items or upgrade storage.".format(
                        overflow
                    )
                )
            storage_preview = {
                "current": storage_profile,
                "projected_personal_used_slots": round(projected_used, 3),
                "projected_personal_free_slots": round(max(0.0, capacity - projected_used), 3),
                "added_asset_stack_slots": round(added_slots, 3),
                "added_instance_slots": int(added_instances),
            }

        wallet = self.state_store.get_wallet(player_id=player_id)
        needed_symbols = [row["symbol"] for row in scaled_cost["elements"]]
        inventory = self.state_store.get_inventory_amounts(
            player_id=player_id,
            symbols=needed_symbols,
        )
        missing_elements: list[dict[str, Any]] = []
        for row in scaled_cost["elements"]:
            symbol = row["symbol"]
            needed = float(row["amount"])
            available = float(inventory.get(symbol, 0.0))
            if available + 1e-9 < needed:
                missing_elements.append(
                    {
                        "symbol": symbol,
                        "needed": round(needed, 3),
                        "available": round(available, 3),
                        "shortfall": round(needed - available, 3),
                    }
                )

        can_afford_credits = float(wallet.get("credits", 0.0)) + 1e-9 >= float(
            scaled_cost["credits"]
        )
        can_craft = (
            can_afford_credits
            and len(missing_elements) == 0
            and len(requirements["missing_tech"]) == 0
            and len(requirements["notes"]) == 0
            and len(storage_notes) == 0
        )

        return {
            "quote_id": str(uuid.uuid4()),
            "player_id": player_id,
            "item_id": item_id,
            "item_kind": item_kind,
            "item_name": item.get("name", item_id),
            "quantity": quantity,
            "world_id": world_id,
            "substitution_id": selected_substitution.get("id")
            if isinstance(selected_substitution, dict)
            else None,
            "selected_substitution": self._summarize_substitution(selected_substitution)
            if isinstance(selected_substitution, dict)
            else None,
            "available_substitutions": available_substitutions,
            "cost": scaled_cost,
            "wallet": wallet,
            "requirements": requirements,
            "storage_notes": storage_notes,
            "storage_preview": storage_preview,
            "missing_elements": missing_elements,
            "can_afford_credits": can_afford_credits,
            "can_craft": can_craft,
        }

    def _quality_tier_from_score(self, score: float) -> str:
        if score < 0.90:
            return "improvised"
        if score < 0.98:
            return "standard"
        if score < 1.06:
            return "refined"
        if score < 1.14:
            return "elite"
        return "prototype"

    def _module_synergy_template(self, module: dict[str, Any]) -> dict[str, Any]:
        family = str(module.get("family", "")).strip().casefold()
        by_family: dict[str, dict[str, Any]] = {
            "scanner": {
                "tags": ["scan_network", "sensor_mesh"],
                "focus_stat": "scan",
                "affix_pool": ["long_baseline", "cold_optics", "phase_array"],
            },
            "engine": {
                "tags": ["velocity_lattice", "thrust_vector"],
                "focus_stat": "defense",
                "affix_pool": ["burner_trim", "inertia_stabilized", "silent_boost"],
            },
            "shield": {
                "tags": ["shield_harmonics", "barrier_matrix"],
                "focus_stat": "shield",
                "affix_pool": ["phase_laced", "high_flux", "regenerative"],
            },
            "armor": {
                "tags": ["armor_reactive", "hull_hardening"],
                "focus_stat": "hull",
                "affix_pool": ["nanograin", "ablative_core", "stress_annealed"],
            },
            "reactor": {
                "tags": ["reactor_bus", "power_lattice"],
                "focus_stat": "energy",
                "affix_pool": ["high_q", "cold_sink", "pulse_stable"],
            },
            "jammer": {
                "tags": ["stealth_mask", "emission_cloak"],
                "focus_stat": "cloak",
                "affix_pool": ["null_phase", "noise_weave", "blackband"],
            },
            "relay": {
                "tags": ["command_mesh", "scan_network"],
                "focus_stat": "scan",
                "affix_pool": ["latency_trim", "burst_sync", "redundant_grid"],
            },
            "special": {
                "tags": ["exotic_matrix", "reactor_bus"],
                "focus_stat": "energy",
                "affix_pool": ["quantum_tuned", "field_inverted", "coherence_locked"],
            },
            "utility": {
                "tags": ["utility_backplane", "hull_hardening"],
                "focus_stat": "defense",
                "affix_pool": ["serviceable", "compact", "integrated"],
            },
            "weapon_laser": {
                "tags": ["laser_aperture", "weapon_synchronized"],
                "focus_stat": "attack",
                "affix_pool": ["tight_beam", "phase_tuned", "mirror_polished"],
            },
            "weapon_missile": {
                "tags": ["missile_guidance", "weapon_synchronized"],
                "focus_stat": "attack",
                "affix_pool": ["smart_seeker", "cold_launch", "terminal_boost"],
            },
            "weapon_ballistic": {
                "tags": ["kinetic_cluster", "weapon_synchronized"],
                "focus_stat": "attack",
                "affix_pool": ["sabot_refined", "coil_assist", "breech_stable"],
            },
            "weapon_plasma": {
                "tags": ["plasma_confinement", "weapon_synchronized"],
                "focus_stat": "attack",
                "affix_pool": ["magnetic_nozzle", "dense_core", "pulse_shaped"],
            },
            "weapon_railgun": {
                "tags": ["rail_coherence", "weapon_synchronized"],
                "focus_stat": "attack",
                "affix_pool": ["armature_trim", "superconducting", "field_braced"],
            },
        }
        return by_family.get(
            family,
            {
                "tags": [f"{family or 'general'}_synergy"],
                "focus_stat": "attack",
                "affix_pool": ["calibrated", "reinforced", "optimized"],
            },
        )

    def _roll_quality_profile(
        self,
        item_kind: str,
        item: dict[str, Any],
        rng: random.Random,
        player_id: str | None = None,
    ) -> dict[str, Any]:
        tier_raw = item.get("tier", 1)
        tier = int(tier_raw) if isinstance(tier_raw, int) else 1
        mean = 1.0 + min(0.05, tier * 0.0045)
        sigma = 0.055 + min(0.02, tier * 0.002)
        quality_mean_shift = 0.0
        quality_sigma_shift = 0.0
        jackpot_shift = 0.0
        preferred_focus = None
        if isinstance(player_id, str) and player_id.strip():
            identity_profile = self._player_identity_modifier_profile(player_id=player_id.strip())
            modifiers = (
                identity_profile.get("modifiers", {})
                if isinstance(identity_profile.get("modifiers"), dict)
                else {}
            )
            variance = (
                identity_profile.get("variance", {})
                if isinstance(identity_profile.get("variance"), dict)
                else {}
            )
            quality_mean_shift += (
                float(modifiers.get("research_pct", 0.0))
                + float(modifiers.get("research_production_pct", 0.0))
            ) * 0.00075
            quality_mean_shift += float(modifiers.get("hacking_pct", 0.0)) * 0.00045
            if item_kind == "hull":
                quality_mean_shift += float(modifiers.get("hull_pct", 0.0)) * 0.00035
            if item_kind == "module":
                quality_mean_shift += float(modifiers.get("scan_pct", 0.0)) * 0.00025
            variance_mean = variance.get("craft_quality_mean_shift_pct")
            variance_sigma = variance.get("craft_quality_sigma_shift_pct")
            variance_jackpot = variance.get("craft_jackpot_shift_pct")
            if isinstance(variance_mean, (int, float)) and not isinstance(variance_mean, bool):
                quality_mean_shift += float(variance_mean) / 100.0
            if isinstance(variance_sigma, (int, float)) and not isinstance(variance_sigma, bool):
                quality_sigma_shift += float(variance_sigma) / 100.0
            if isinstance(variance_jackpot, (int, float)) and not isinstance(variance_jackpot, bool):
                jackpot_shift += float(variance_jackpot) / 100.0
            focus_raw = variance.get("preferred_focus")
            if isinstance(focus_raw, str) and focus_raw.strip():
                preferred_focus = focus_raw.strip().casefold()

        mean = max(0.88, min(1.16, mean + quality_mean_shift))
        sigma = max(0.02, min(0.16, sigma * (1.0 + quality_sigma_shift)))
        score = max(0.72, min(1.34, rng.gauss(mean, sigma)))
        jackpot_chance = max(0.0025, min(0.11, (0.012 + (tier * 0.0038)) * (1.0 + jackpot_shift)))
        jackpot_triggered = rng.random() < jackpot_chance
        if jackpot_triggered:
            score = max(score, min(1.42, score + abs(rng.gauss(0.045, 0.022))))
        tier_name = self._quality_tier_from_score(score)

        stat_source = {}
        if item_kind == "module":
            stat_source = item.get("stat_bonuses", {})
        elif item_kind == "hull":
            stat_source = item.get("base_stats", {})

        stat_preview: dict[str, Any] = {}
        locked_precision_stats = {"deck_cost", "module_slots", "crew_min", "crew_max"}
        if item_kind == "module":
            raw_deck_cost = item.get("deck_cost")
            if isinstance(raw_deck_cost, (int, float)) and not isinstance(raw_deck_cost, bool):
                stat_preview["deck_cost"] = int(max(1, round(float(raw_deck_cost))))
        if isinstance(stat_source, dict):
            for key, raw in stat_source.items():
                if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                    base_value = float(raw)
                    if key in locked_precision_stats:
                        roll_sigma = 0.003
                    else:
                        roll_sigma = 0.028 + min(0.024, tier * 0.0019)
                    if preferred_focus and preferred_focus in key.casefold():
                        roll_sigma *= 0.84
                    roll_factor = 1.0 + rng.gauss(0.0, roll_sigma)
                    stat_preview[key] = round(base_value * score * max(0.78, min(1.28, roll_factor)), 3)
                else:
                    stat_preview[key] = raw

        size_nominal_m3 = None
        if item_kind == "module":
            deck_cost_raw = item.get("deck_cost")
            if isinstance(deck_cost_raw, (int, float)) and not isinstance(deck_cost_raw, bool):
                size_nominal_m3 = max(0.08, float(deck_cost_raw) * EQUIPMENT_M3_PER_DECK_POINT)
        elif item_kind == "hull":
            base_stats_raw = item.get("base_stats", {})
            if isinstance(base_stats_raw, dict):
                deck_raw = base_stats_raw.get("deck", 0.0)
                if isinstance(deck_raw, (int, float)) and not isinstance(deck_raw, bool):
                    size_nominal_m3 = max(1.0, float(deck_raw) * EQUIPMENT_M3_PER_DECK_POINT)
        size_roll_m3 = None
        if isinstance(size_nominal_m3, (int, float)):
            size_variance = max(-0.11, min(0.11, rng.gauss(0.0, 0.018 + (tier * 0.0006))))
            rolled_size = float(size_nominal_m3) * (1.0 + size_variance)
            if rolled_size < 1.0:
                step = 0.05
            elif rolled_size < 8.0:
                step = 0.25
            elif rolled_size < 80.0:
                step = 0.5
            else:
                step = 1.0
            size_roll_m3 = round(round(rolled_size / step) * step, 3)

        synergy_tags: list[str] = []
        affix_name = None
        affix_stat_bonuses: dict[str, float] = {}
        if item_kind == "module":
            template = self._module_synergy_template(item)
            tags_raw = template.get("tags")
            if isinstance(tags_raw, list):
                synergy_tags = [str(tag) for tag in tags_raw if isinstance(tag, str)]
            focus_stat = str(template.get("focus_stat", "attack"))
            affix_pool = template.get("affix_pool", [])
            affix_chance = max(0.08, min(0.62, 0.18 + ((score - 1.0) * 1.45)))
            if preferred_focus and preferred_focus in focus_stat.casefold():
                affix_chance = min(0.72, affix_chance + 0.08)
            if isinstance(affix_pool, list) and affix_pool and rng.random() < affix_chance:
                affix_name = str(affix_pool[rng.randrange(0, len(affix_pool))])
                focus_bonus_pct = max(2.0, min(20.0, abs(rng.gauss(6.0 + (tier * 0.9), 2.8))))
                affix_stat_bonuses[focus_stat] = round(focus_bonus_pct, 3)

        percentile_estimate = max(
            0.0,
            min(
                100.0,
                50.0 * (1.0 + math.erf((score - mean) / max(1e-6, sigma * math.sqrt(2.0)))),
            ),
        )

        return {
            "quality_tier": tier_name,
            "quality_score": round(score, 4),
            "stat_multiplier": round(score, 4),
            "item_level": 1,
            "item_xp": 0,
            "xp_to_next_level": int(120 + (tier * 28)),
            "tier": tier,
            "rolled_stats_preview": stat_preview,
            "size_m3": size_roll_m3,
            "size_nominal_m3": round(float(size_nominal_m3), 3)
            if isinstance(size_nominal_m3, (int, float))
            else None,
            "synergy_tags": synergy_tags,
            "affix_name": affix_name,
            "affix_stat_bonuses": affix_stat_bonuses,
            "jackpot_triggered": jackpot_triggered,
            "jackpot_chance": round(jackpot_chance, 6),
            "quality_percentile_estimate": round(percentile_estimate, 4),
            "mean_reference": round(mean, 5),
            "sigma_reference": round(sigma, 5),
            "source_player_id": player_id.strip() if isinstance(player_id, str) and player_id.strip() else None,
        }

    def _craft_item(
        self,
        player_id: str,
        item_id: str,
        quantity: int,
        world_id: str | None = None,
        substitution_id: str | None = None,
    ) -> dict[str, Any]:
        if quantity <= 0:
            raise ValueError("quantity must be > 0")
        quote = self._crafting_quote(
            player_id=player_id,
            item_id=item_id,
            quantity=quantity,
            world_id=world_id,
            substitution_id=substitution_id,
        )
        if not quote["can_craft"]:
            reasons: list[str] = []
            if not quote["can_afford_credits"]:
                reasons.append("insufficient credits")
            if quote["missing_elements"]:
                reasons.append("insufficient elements")
            if quote["requirements"]["missing_tech"]:
                reasons.append("missing prerequisite tech")
            if quote["requirements"]["notes"]:
                reasons.extend(quote["requirements"]["notes"])
            if quote.get("storage_notes"):
                reasons.extend(
                    [str(note) for note in quote["storage_notes"] if isinstance(note, str)]
                )
            raise ValueError(f"Cannot craft {item_id}: " + "; ".join(reasons))

        cost = quote["cost"]
        element_deltas = {
            row["symbol"]: -float(row["amount"])
            for row in cost["elements"]
        }
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id,
            credits_delta=-float(cost["credits"]),
            voidcoin_delta=0.0,
            element_deltas=element_deltas,
        )

        item_kind = quote["item_kind"]
        if item_kind == "tech":
            self.state_store.unlock_tech(player_id=player_id, tech_id=item_id)
            return {
                "build_id": str(uuid.uuid4()),
                "player_id": player_id,
                "item_id": item_id,
                "item_kind": item_kind,
                "unlocked": item_id,
                "wallet": resource_state["wallet"],
                "inventory_changes": resource_state["inventory"],
                "quote": quote,
            }

        if item_kind in {"module", "hull"}:
            added_stack_slots = self._estimate_personal_slot_delta_for_asset_add(
                player_id=player_id,
                asset_type=item_kind,
                asset_id=item_id,
                quantity=quantity,
            )
            added_instances = min(quantity, 24)
            self._ensure_storage_capacity_for_reward(
                player_id=player_id,
                additional_personal_asset_slots=added_stack_slots,
                additional_instance_slots=added_instances,
                additional_smuggle_asset_slots=0.0,
            )
            self.state_store.add_asset(
                player_id=player_id,
                asset_type=item_kind,
                asset_id=item_id,
                quantity=quantity,
            )
            if DETERMINISTIC_MODE:
                rng_seed = int(stable_hash_int(player_id, item_id, quantity, "craft_quality"))
            else:
                rng_seed = int(time.time()) ^ int(
                    stable_hash_int(player_id, item_id, quantity, "craft_quality")
                )
            rng = random.Random(rng_seed)
            quality_instances: list[dict[str, Any]] = []
            max_instance_rows = min(quantity, 24)
            for _ in range(max_instance_rows):
                quality = self._roll_quality_profile(
                    item_kind=item_kind,
                    item=self._catalog_lookup_item(item_id=item_id)[1],
                    rng=rng,
                    player_id=player_id,
                )
                stored = self.state_store.add_crafted_instance(
                    player_id=player_id,
                    asset_type=item_kind,
                    asset_id=item_id,
                    quality_payload=quality,
                )
                quality_instances.append(stored)
            assets = self.state_store.list_assets(
                player_id=player_id,
                asset_type=item_kind,
                limit=12,
            )
            quality_summary: dict[str, int] = {}
            for row in quality_instances:
                tier_name = str(row.get("quality_tier", "standard"))
                quality_summary[tier_name] = quality_summary.get(tier_name, 0) + 1
            return {
                "build_id": str(uuid.uuid4()),
                "player_id": player_id,
                "item_id": item_id,
                "item_kind": item_kind,
                "quantity": quantity,
                "wallet": resource_state["wallet"],
                "inventory_changes": resource_state["inventory"],
                "assets": assets,
                "quality_seed": rng_seed,
                "quality_summary": quality_summary,
                "quality_instances": quality_instances,
                "quote": quote,
            }

        if item_kind == "structure":
            if quantity != 1:
                raise ValueError("Structures must be built one at a time")
            if not world_id:
                raise ValueError("world_id is required for structure builds")
            world = self.state_store.add_world_structure(
                player_id=player_id,
                world_id=world_id,
                structure_id=item_id,
            )
            projection = self._project_world_structure(
                {"world": world, "structure_ids": world.get("built_structures", [])}
            )
            return {
                "build_id": str(uuid.uuid4()),
                "player_id": player_id,
                "item_id": item_id,
                "item_kind": item_kind,
                "world": world,
                "projection": projection,
                "wallet": resource_state["wallet"],
                "inventory_changes": resource_state["inventory"],
                "quote": quote,
            }

        raise ValueError(f"Unsupported item kind '{item_kind}'")

    def _start_manufacturing_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        item_id = payload.get("item_id")
        quantity = payload.get("quantity", 1)
        profile_id = payload.get("profile_id")
        world_id = payload.get("world_id")
        substitution_id = payload.get("substitution_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(item_id, str) or not item_id.strip():
            raise ValueError("item_id must be a non-empty string")
        if isinstance(quantity, bool) or not isinstance(quantity, int):
            raise ValueError("quantity must be an integer")
        if quantity <= 0:
            raise ValueError("quantity must be > 0")
        if profile_id is not None and not isinstance(profile_id, str):
            raise ValueError("profile_id must be a string when provided")
        if world_id is not None and not isinstance(world_id, str):
            raise ValueError("world_id must be a string when provided")
        if substitution_id is not None and not isinstance(substitution_id, str):
            raise ValueError("substitution_id must be a string when provided")
        self._ensure_player_bootstrap(player_id.strip())

        if isinstance(profile_id, str) and profile_id.strip():
            profile = next(
                (
                    row
                    for row in self.seed_store.manufacturing_profiles
                    if isinstance(row, dict) and row.get("id") == profile_id.strip()
                ),
                None,
            )
            if not isinstance(profile, dict):
                raise ValueError(f"Unknown profile_id '{profile_id}'")
        else:
            profile = self.seed_store.manufacturing_profiles[0]
        profile_id_resolved = str(profile.get("id", "mfg_profile.baseline_fabricator"))
        max_parallel_jobs = int(profile.get("max_parallel_jobs", 2))

        active_jobs = self.state_store.list_manufacturing_jobs(
            player_id=player_id.strip(),
            status="active",
            limit=200,
        )
        if (not self._has_admin_privileges(player_id)) and len(active_jobs) >= max_parallel_jobs:
            raise ValueError(
                "Maximum active manufacturing jobs reached for profile "
                f"({len(active_jobs)}/{max_parallel_jobs})"
            )

        item_kind, item = self._catalog_lookup_item(item_id=item_id.strip())
        if item_kind not in {"module", "hull"}:
            raise ValueError("manufacturing/start currently supports module.* or hull.* item_id")

        quote = self._crafting_quote(
            player_id=player_id.strip(),
            item_id=item_id.strip(),
            quantity=quantity,
            world_id=world_id.strip() if isinstance(world_id, str) else None,
            substitution_id=substitution_id.strip() if isinstance(substitution_id, str) else None,
        )
        if not quote["can_craft"]:
            reasons: list[str] = []
            if not quote["can_afford_credits"]:
                reasons.append("insufficient credits")
            if quote["missing_elements"]:
                reasons.append("insufficient elements")
            if quote["requirements"]["missing_tech"]:
                reasons.append("missing prerequisite tech")
            if quote["requirements"]["notes"]:
                reasons.extend(quote["requirements"]["notes"])
            if quote.get("storage_notes"):
                reasons.extend(
                    [str(note) for note in quote["storage_notes"] if isinstance(note, str)]
                )
            raise ValueError("Cannot start manufacturing: " + "; ".join(reasons))

        tier_raw = item.get("tier", 1)
        tier = int(tier_raw) if isinstance(tier_raw, int) else 1
        population = self._player_population_totals(player_id=player_id.strip())
        workforce_per_tier = float(profile.get("workforce_per_tier", 850.0))
        workforce_required = workforce_per_tier * max(1, tier) * max(1, quantity)
        if (
            (not self._has_admin_privileges(player_id))
            and float(population["current"]) + 1e-9 < workforce_required
        ):
            raise ValueError(
                "Insufficient workforce capacity for manufacturing (need {:.0f}, have {:.0f})".format(
                    workforce_required,
                    float(population["current"]),
                )
            )

        fleet = self._ensure_fleet_initialized(player_id.strip())
        active_hull_id = str(fleet.get("active_hull_id", ""))
        active_hull = self.seed_store.hull_index().get(active_hull_id, {})
        hull_stats = active_hull.get("base_stats", {})
        cargo_capacity = 0.0
        if isinstance(hull_stats, dict):
            raw_cap = hull_stats.get("cargo_capacity_tons", hull_stats.get("cargo", 0.0))
            if isinstance(raw_cap, (int, float)) and not isinstance(raw_cap, bool):
                cargo_capacity = float(raw_cap)
        cargo_per_tier = float(profile.get("cargo_per_tier", 45.0))
        cargo_required = cargo_per_tier * max(1, tier) * max(1, quantity)
        if (not self._has_admin_privileges(player_id)) and cargo_capacity + 1e-9 < cargo_required:
            raise ValueError(
                "Insufficient cargo capacity for this job (need {:.1f}, have {:.1f})".format(
                    cargo_required,
                    cargo_capacity,
                )
            )

        compute = self._player_compute_profile(player_id=player_id.strip())
        compute_per_hour = float(compute["compute_power_per_hour"])
        base_throughput = float(profile.get("base_throughput_units_per_hour", 1.0))
        compute_factor = 1.0 + min(2.2, compute_per_hour / 18000.0)
        workforce_factor = (
            1.0
            if workforce_required <= 0
            else min(1.2, max(0.3, float(population["current"]) / workforce_required))
        )
        cargo_factor = 1.0 if cargo_required <= 0 else min(1.15, max(0.4, cargo_capacity / cargo_required))
        throughput = max(0.05, base_throughput * compute_factor * workforce_factor * cargo_factor)

        workload = float(quantity) * (1.0 + (tier * 0.7))
        if item_kind == "hull":
            workload *= 1.9
        duration_seconds = max(300, int(round((workload / throughput) * 3600.0)))

        cost = quote["cost"]
        element_deltas = {row["symbol"]: -float(row["amount"]) for row in cost["elements"]}
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=-float(cost["credits"]),
            voidcoin_delta=0.0,
            element_deltas=element_deltas,
        )
        job = self.state_store.start_manufacturing_job(
            player_id=player_id.strip(),
            item_id=item_id.strip(),
            quantity=quantity,
            profile_id=profile_id_resolved,
            workload=workload,
            throughput_per_hour=throughput,
            duration_seconds=duration_seconds,
            cost_payload=cost,
            world_id=world_id.strip() if isinstance(world_id, str) else None,
            substitution_id=substitution_id.strip() if isinstance(substitution_id, str) else None,
        )
        return {
            "player_id": player_id.strip(),
            "job": job,
            "profile": profile,
            "quote": quote,
            "constraints": {
                "workforce_required": round(workforce_required, 3),
                "workforce_available": round(float(population["current"]), 3),
                "cargo_required": round(cargo_required, 3),
                "cargo_capacity": round(cargo_capacity, 3),
                "throughput_per_hour": round(throughput, 4),
                "workload": round(workload, 4),
                "duration_seconds": duration_seconds,
            },
            "wallet": resource_state["wallet"],
            "inventory_changes": resource_state["inventory"],
        }

    def _level_asset_instance(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        instance_id = payload.get("instance_id")
        levels = payload.get("levels", 1)
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise ValueError("instance_id must be a non-empty string")
        if isinstance(levels, bool) or not isinstance(levels, int):
            raise ValueError("levels must be an integer")
        if levels <= 0 or levels > 20:
            raise ValueError("levels must be between 1 and 20")
        self._ensure_player_bootstrap(player_id.strip())
        instance = self.state_store.get_crafted_instance(
            player_id=player_id.strip(),
            instance_id=instance_id.strip(),
        )
        if str(instance.get("asset_type")) not in {"module", "hull"}:
            raise ValueError("Only module and hull instances can be leveled")
        tier_raw = instance.get("tier", 1)
        tier = int(tier_raw) if isinstance(tier_raw, int) else 1
        if tier <= 0:
            tier = 1
        payload_level_raw = instance.get("item_level", 1)
        current_level = payload_level_raw if isinstance(payload_level_raw, int) else 1
        target_level = max(1, min(40, current_level + levels))
        if target_level == current_level:
            return {
                "player_id": player_id.strip(),
                "instance_before": instance,
                "instance_after": instance,
                "cost": {"credits": 0.0, "elements": []},
                "leveled": False,
            }
        cost = self._item_level_upgrade_cost(
            current_level=current_level,
            target_level=target_level,
            tier=tier,
        )
        element_deltas = {
            row["symbol"]: -float(row["amount"])
            for row in cost["elements"]
            if isinstance(row, dict) and isinstance(row.get("symbol"), str)
        }
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=-float(cost["credits"]),
            voidcoin_delta=0.0,
            element_deltas=element_deltas,
        )
        payload_data = {
            key: value
            for key, value in instance.items()
            if key
            not in {
                "instance_id",
                "asset_type",
                "asset_id",
                "quality_tier",
                "quality_score",
                "stat_multiplier",
                "created_utc",
            }
        }
        payload_data["item_level"] = target_level
        payload_data["item_xp"] = 0
        payload_data["xp_to_next_level"] = int(140 + (target_level * 22))
        quality_score = float(instance.get("quality_score", 1.0))
        stat_multiplier = quality_score * self._item_level_stat_multiplier(
            level=target_level,
            tier=tier,
        )
        updated = self.state_store.update_crafted_instance(
            player_id=player_id.strip(),
            instance_id=instance_id.strip(),
            payload=payload_data,
            quality_tier=str(instance.get("quality_tier", "standard")),
            quality_score=quality_score,
            stat_multiplier=stat_multiplier,
        )
        return {
            "player_id": player_id.strip(),
            "instance_before": instance,
            "instance_after": updated,
            "cost": cost,
            "wallet": resource_state["wallet"],
            "inventory_changes": resource_state["inventory"],
            "leveled": True,
        }

    def _claim_manufacturing_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        job_id = payload.get("job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        before = self.state_store.get_manufacturing_job(
            player_id=player_id.strip(),
            job_id=job_id.strip(),
        )
        claimed = self.state_store.claim_manufacturing_job(
            player_id=player_id.strip(),
            job_id=job_id.strip(),
        )
        granted_assets: list[dict[str, Any]] = []
        quality_instances: list[dict[str, Any]] = []
        quality_summary: dict[str, int] = {}
        if before["status"] != "claimed":
            item_kind, item = self._catalog_lookup_item(item_id=str(claimed["item_id"]))
            qty = int(claimed["quantity"])
            if item_kind in {"module", "hull"}:
                added_stack_slots = self._estimate_personal_slot_delta_for_asset_add(
                    player_id=player_id.strip(),
                    asset_type=item_kind,
                    asset_id=str(claimed["item_id"]),
                    quantity=qty,
                )
                self._ensure_storage_capacity_for_reward(
                    player_id=player_id.strip(),
                    additional_personal_asset_slots=added_stack_slots,
                    additional_instance_slots=min(qty, 24),
                    additional_smuggle_asset_slots=0.0,
                )
                self.state_store.add_asset(
                    player_id=player_id.strip(),
                    asset_type=item_kind,
                    asset_id=str(claimed["item_id"]),
                    quantity=qty,
                )
                granted_assets.append(
                    {
                        "asset_type": item_kind,
                        "asset_id": str(claimed["item_id"]),
                        "quantity": qty,
                    }
                )
                rng = random.Random(
                    stable_hash_int(job_id.strip(), player_id.strip(), "mfg_quality")
                )
                for _ in range(min(qty, 24)):
                    quality = self._roll_quality_profile(
                        item_kind=item_kind,
                        item=item,
                        rng=rng,
                        player_id=player_id.strip(),
                    )
                    stored = self.state_store.add_crafted_instance(
                        player_id=player_id.strip(),
                        asset_type=item_kind,
                        asset_id=str(claimed["item_id"]),
                        quality_payload=quality,
                    )
                    quality_instances.append(stored)
                    tier_name = str(stored.get("quality_tier", "standard"))
                    quality_summary[tier_name] = quality_summary.get(tier_name, 0) + 1
        return {
            "player_id": player_id.strip(),
            "job": claimed,
            "was_already_claimed": before["status"] == "claimed",
            "granted_assets": granted_assets,
            "quality_summary": quality_summary,
            "quality_instances": quality_instances,
        }

    def _cancel_manufacturing_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        job_id = payload.get("job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        before = self.state_store.get_manufacturing_job(
            player_id=player_id.strip(),
            job_id=job_id.strip(),
        )
        if before["status"] == "claimed":
            raise ValueError("Claimed manufacturing jobs cannot be cancelled")
        if before["status"] == "completed":
            raise ValueError("Completed manufacturing jobs must be claimed, not cancelled")
        cancelled = self.state_store.cancel_manufacturing_job(
            player_id=player_id.strip(),
            job_id=job_id.strip(),
        )
        remaining = float(before.get("remaining_seconds", 0.0))
        duration = max(1.0, float(before.get("duration_seconds", 1.0)))
        progress = max(0.0, min(1.0, 1.0 - (remaining / duration)))
        refund_ratio = 0.85 - (0.65 * progress)
        refund_ratio = max(0.2, min(0.85, refund_ratio))
        cost = before.get("cost", {})
        credits = 0.0
        elements: list[dict[str, Any]] = []
        if isinstance(cost, dict):
            raw_credits = cost.get("credits", 0.0)
            if isinstance(raw_credits, (int, float)) and not isinstance(raw_credits, bool):
                credits = float(raw_credits) * refund_ratio
            raw_elements = cost.get("elements", [])
            if isinstance(raw_elements, list):
                for row in raw_elements:
                    if not isinstance(row, dict):
                        continue
                    symbol = row.get("symbol")
                    amount = row.get("amount")
                    if not isinstance(symbol, str):
                        continue
                    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                        continue
                    elements.append(
                        {"symbol": symbol, "amount": float(amount) * refund_ratio}
                    )
        deltas = {row["symbol"]: row["amount"] for row in elements if row["amount"] > 0}
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=credits,
            voidcoin_delta=0.0,
            element_deltas=deltas,
        )
        return {
            "player_id": player_id.strip(),
            "job_before": before,
            "job_after": cancelled,
            "refund_ratio": round(refund_ratio, 4),
            "refund": {
                "credits": round(credits, 4),
                "elements": [
                    {"symbol": row["symbol"], "amount": round(row["amount"], 3)}
                    for row in elements
                    if row["amount"] > 0
                ],
            },
            "wallet": resource_state["wallet"],
            "inventory_changes": resource_state["inventory"],
        }

    def _start_reverse_engineering(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        recipe_id = payload.get("recipe_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(recipe_id, str) or not recipe_id.strip():
            raise ValueError("recipe_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        recipe = self.seed_store.reverse_recipe_index().get(recipe_id.strip())
        if not isinstance(recipe, dict):
            raise ValueError(f"Unknown recipe_id '{recipe_id}'")

        required_races = recipe.get("required_race_ids", [])
        if isinstance(required_races, list) and required_races:
            profile = self.state_store.get_profile(player_id=player_id.strip())
            race_id = profile.get("race_id")
            if (not self._has_admin_privileges(player_id)) and (
                not isinstance(race_id, str) or race_id not in required_races
            ):
                raise ValueError(
                    "Recipe requires one of race ids: {}".format(
                        ", ".join(str(row) for row in required_races if isinstance(row, str))
                    )
                )

        consumable_id = str(recipe.get("required_consumable_id", ""))
        consumable = self.seed_store.consumable_index().get(consumable_id)
        if not isinstance(consumable, dict):
            raise ValueError(f"Recipe references unknown consumable '{consumable_id}'")
        required_tech = consumable.get("required_tech")
        if (
            (not self._has_admin_privileges(player_id))
            and isinstance(required_tech, str)
            and not self.state_store.is_tech_unlocked(player_id.strip(), required_tech)
        ):
            raise ValueError(f"Required consumable tech is not unlocked: {required_tech}")

        assets = self.state_store.list_assets(
            player_id=player_id.strip(),
            asset_type="consumable",
            limit=120,
        )
        qty_available = 0
        for row in assets:
            if isinstance(row, dict) and row.get("asset_id") == consumable_id:
                qty_available = int(row.get("quantity", 0))
                break
        if qty_available <= 0:
            raise ValueError(
                f"Missing required consumable '{consumable_id}'. Build or acquire one first."
            )

        active_jobs = self.state_store.list_reverse_jobs(
            player_id=player_id.strip(),
            status="active",
            limit=60,
        )
        if (not self._has_admin_privileges(player_id)) and len(active_jobs) >= 2:
            raise ValueError("Maximum active reverse-engineering jobs reached (2)")

        self.state_store.adjust_asset_quantity(
            player_id=player_id.strip(),
            asset_type="consumable",
            asset_id=consumable_id,
            quantity_delta=-1,
        )

        compute = self._player_compute_profile(player_id=player_id.strip())
        compute_per_hour = max(1.0, float(compute["compute_power_per_hour"]))
        compute_cost_raw = recipe.get("compute_cost", 80000)
        duration_raw = recipe.get("duration_seconds", 28800)
        compute_cost = float(compute_cost_raw) if isinstance(compute_cost_raw, (int, float)) else 80000.0
        base_duration = int(duration_raw) if isinstance(duration_raw, int) else 28800
        compute_eta = int(round((compute_cost / compute_per_hour) * 3600.0))
        duration_seconds = max(1800, max(base_duration, compute_eta))

        job = self.state_store.start_reverse_job(
            player_id=player_id.strip(),
            recipe_id=recipe_id.strip(),
            target_item_id=str(recipe.get("target_item_id")),
            consumable_id=consumable_id,
            unlock_blueprint_id=str(recipe.get("unlock_blueprint_id")),
            compute_cost=compute_cost,
            duration_seconds=duration_seconds,
        )
        return {
            "player_id": player_id.strip(),
            "job": job,
            "recipe": recipe,
            "consumable_consumed": consumable_id,
            "compute_profile": compute,
            "duration_seconds": duration_seconds,
        }

    def _claim_reverse_engineering(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        job_id = payload.get("job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        before = self.state_store.get_reverse_job(player_id=player_id.strip(), job_id=job_id.strip())
        claimed = self.state_store.claim_reverse_job(player_id=player_id.strip(), job_id=job_id.strip())
        blueprint_granted = None
        if before["status"] != "claimed":
            blueprint_id = str(claimed.get("unlock_blueprint_id", ""))
            self.state_store.add_asset(
                player_id=player_id.strip(),
                asset_type="blueprint",
                asset_id=blueprint_id,
                quantity=1,
            )
            assets = self.state_store.list_assets(
                player_id=player_id.strip(),
                asset_type="blueprint",
                limit=160,
            )
            qty = 0
            for row in assets:
                if isinstance(row, dict) and row.get("asset_id") == blueprint_id:
                    qty = int(row.get("quantity", 0))
                    break
            blueprint_granted = {
                "asset_id": blueprint_id,
                "quantity_added": 1,
                "total_quantity": qty,
            }
        return {
            "player_id": player_id.strip(),
            "job": claimed,
            "was_already_claimed": before["status"] == "claimed",
            "blueprint_granted": blueprint_granted,
        }

    def _mission_index(self) -> dict[str, dict[str, Any]]:
        return {
            row["id"]: row
            for row in self.seed_store.missions
            if isinstance(row, dict) and isinstance(row.get("id"), str)
        }

    def _mission_requirements(
        self,
        player_id: str,
        mission: dict[str, Any],
    ) -> dict[str, Any]:
        requirements = mission.get("requirements", {})
        if not isinstance(requirements, dict):
            requirements = {}
        blockers: list[str] = []

        progress = self.state_store.get_combat_progress(player_id=player_id)
        combat_rank = int(progress.get("combat_rank", 1)) if isinstance(progress.get("combat_rank"), int) else 1
        min_rank_raw = requirements.get("min_rank", 1)
        min_rank = int(min_rank_raw) if isinstance(min_rank_raw, int) else 1
        if combat_rank < max(1, min_rank):
            blockers.append(f"Requires combat rank {min_rank} (current {combat_rank}).")

        profile = self.state_store.get_profile(player_id=player_id)
        race_id = profile.get("race_id")
        profession_id = profile.get("profession_id")
        required_races = requirements.get("required_race_ids", [])
        if isinstance(required_races, list) and required_races:
            if not isinstance(race_id, str) or race_id not in required_races:
                blockers.append(
                    "Requires race in: {}".format(
                        ", ".join(str(row) for row in required_races if isinstance(row, str))
                    )
                )
        required_professions = requirements.get("required_profession_ids", [])
        if isinstance(required_professions, list) and required_professions:
            if not isinstance(profession_id, str) or profession_id not in required_professions:
                blockers.append(
                    "Requires profession in: {}".format(
                        ", ".join(str(row) for row in required_professions if isinstance(row, str))
                    )
                )

        required_factions = requirements.get("required_faction_ids", [])
        if isinstance(required_factions, list) and required_factions:
            affiliation = self.state_store.get_player_faction_affiliation(player_id=player_id)
            current_faction_id = (
                affiliation.get("faction_id")
                if isinstance(affiliation, dict)
                and isinstance(affiliation.get("faction_id"), str)
                else None
            )
            if not isinstance(current_faction_id, str) or current_faction_id not in required_factions:
                blockers.append(
                    "Requires faction in: {}".format(
                        ", ".join(str(row) for row in required_factions if isinstance(row, str))
                    )
                )

        required_missions = requirements.get("required_missions", [])
        if isinstance(required_missions, list) and required_missions:
            claimed_ids = set(self.state_store.list_claimed_mission_ids(player_id=player_id))
            missing = [
                mission_id
                for mission_id in required_missions
                if isinstance(mission_id, str) and mission_id not in claimed_ids
            ]
            if missing:
                blockers.append(
                    "Requires completed missions: {}".format(", ".join(sorted(missing)))
                )

        required_scan_count = requirements.get("required_scan_count")
        if isinstance(required_scan_count, (int, float)) and not isinstance(required_scan_count, bool):
            scanned = self.state_store.count_discovered_worlds(player_id=player_id)
            if scanned + 1e-9 < float(required_scan_count):
                blockers.append(
                    "Requires discovered worlds >= {:.0f} (current {}).".format(
                        float(required_scan_count),
                        scanned,
                    )
                )

        required_planet_types = requirements.get("required_planet_types", [])
        if isinstance(required_planet_types, list) and required_planet_types:
            discovered = self.state_store.list_discovered_worlds(player_id=player_id, limit=4000)
            owned = self.state_store.list_worlds_for_player(player_id=player_id)
            seen_types: set[str] = set()
            for row in [*discovered, *owned]:
                if not isinstance(row, dict):
                    continue
                for key in ("planet_type_id", "type_id", "subtype", "template_id"):
                    value = row.get(key)
                    if isinstance(value, str):
                        seen_types.add(value)
            missing_types = [
                type_id
                for type_id in required_planet_types
                if isinstance(type_id, str) and type_id not in seen_types
            ]
            if missing_types:
                blockers.append(
                    "Requires discovered planet types: {}".format(
                        ", ".join(sorted(missing_types))
                    )
                )

        return {
            "combat_rank": combat_rank,
            "min_rank": max(1, min_rank),
            "blockers": blockers,
            "is_eligible": len(blockers) == 0,
        }

    def _mission_objective(self, mission: dict[str, Any]) -> dict[str, Any]:
        requirements = mission.get("requirements", {})
        if not isinstance(requirements, dict):
            requirements = {}
        required_scan_count = requirements.get("required_scan_count")
        if isinstance(required_scan_count, (int, float)) and not isinstance(required_scan_count, bool):
            target = max(1.0, float(required_scan_count))
            return {
                "kind": "scan_worlds",
                "target": target,
                "label": f"Discover {int(round(target))} worlds",
            }
        required_planet_types = requirements.get("required_planet_types", [])
        if isinstance(required_planet_types, list) and required_planet_types:
            target = max(
                1.0,
                float(
                    len(
                        [item for item in required_planet_types if isinstance(item, str)]
                    )
                ),
            )
            return {
                "kind": "discover_planet_types",
                "target": target,
                "label": f"Discover {int(round(target))} required planet types",
            }
        return {
            "kind": "command_report",
            "target": 1.0,
            "label": "Submit mission report",
        }

    def _mission_progress_value(self, player_id: str, mission: dict[str, Any]) -> float:
        objective = self._mission_objective(mission)
        kind = objective["kind"]
        if kind == "scan_worlds":
            return float(self.state_store.count_discovered_worlds(player_id=player_id))
        if kind == "discover_planet_types":
            requirements = mission.get("requirements", {})
            if not isinstance(requirements, dict):
                return 0.0
            required_planet_types = requirements.get("required_planet_types", [])
            if not isinstance(required_planet_types, list):
                return 0.0
            required = {
                item for item in required_planet_types if isinstance(item, str)
            }
            discovered = self.state_store.list_discovered_worlds(player_id=player_id, limit=4000)
            owned = self.state_store.list_worlds_for_player(player_id=player_id)
            matched: set[str] = set()
            for row in [*discovered, *owned]:
                if not isinstance(row, dict):
                    continue
                for key in ("planet_type_id", "type_id", "subtype", "template_id"):
                    value = row.get(key)
                    if isinstance(value, str) and value in required:
                        matched.add(value)
            return float(len(matched))
        return 1.0

    def _mission_reward_payload(self, player_id: str, mission: dict[str, Any]) -> dict[str, Any]:
        rewards = mission.get("rewards", {})
        if not isinstance(rewards, dict):
            rewards = {}
        credits = 0.0
        xp = 0.0
        rp = 0.0
        if isinstance(rewards.get("credits"), (int, float)) and not isinstance(rewards.get("credits"), bool):
            credits = float(rewards.get("credits", 0.0))
        if isinstance(rewards.get("xp"), (int, float)) and not isinstance(rewards.get("xp"), bool):
            xp = float(rewards.get("xp", 0.0))
        if isinstance(rewards.get("rp"), (int, float)) and not isinstance(rewards.get("rp"), bool):
            rp = float(rewards.get("rp", 0.0))
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id,
            credits_delta=credits,
            voidcoin_delta=0.0,
            element_deltas={},
        )
        combat_progress_after = self.state_store.get_combat_progress(player_id=player_id)
        ship_progress_after = self._ensure_fleet_initialized(player_id=player_id)
        if xp > 0:
            combat_progress_after = self.state_store.grant_combat_xp(player_id=player_id, xp_delta=xp)
            ship_progress_after = self.state_store.grant_fleet_xp(
                player_id=player_id,
                xp_delta=max(0.0, xp * 0.65),
            )
        item_ids = rewards.get("item_ids", [])
        granted_items: list[dict[str, Any]] = []
        if isinstance(item_ids, list):
            for item_id in item_ids:
                if not isinstance(item_id, str):
                    continue
                if item_id.startswith("module."):
                    self.state_store.add_asset(
                        player_id=player_id,
                        asset_type="module",
                        asset_id=item_id,
                        quantity=1,
                    )
                    granted_items.append({"asset_type": "module", "asset_id": item_id, "quantity": 1})
                elif item_id.startswith("hull."):
                    self.state_store.add_asset(
                        player_id=player_id,
                        asset_type="hull",
                        asset_id=item_id,
                        quantity=1,
                    )
                    granted_items.append({"asset_type": "hull", "asset_id": item_id, "quantity": 1})
                elif item_id.startswith("tech."):
                    self.state_store.unlock_tech(player_id=player_id, tech_id=item_id)
                    granted_items.append({"asset_type": "tech_unlock", "asset_id": item_id, "quantity": 1})
        return {
            "credits": round(credits, 4),
            "xp": round(xp, 3),
            "rp": round(rp, 3),
            "items": granted_items,
            "combat_progress_after": combat_progress_after,
            "ship_progress_after": ship_progress_after,
            "wallet": resource_state["wallet"],
            "inventory": resource_state["inventory"],
        }

    def _accept_mission(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        mission_id = payload.get("mission_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(mission_id, str) or not mission_id.strip():
            raise ValueError("mission_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        mission = self._mission_index().get(mission_id.strip())
        if not isinstance(mission, dict):
            raise ValueError(f"Unknown mission_id '{mission_id}'")

        requirement_state = self._mission_requirements(player_id=player_id.strip(), mission=mission)
        if not bool(requirement_state["is_eligible"]):
            raise ValueError("; ".join(str(row) for row in requirement_state["blockers"]))

        active_jobs = self.state_store.list_mission_jobs(
            player_id=player_id.strip(),
            status="active",
            limit=200,
        )
        if len(active_jobs) >= 10:
            raise ValueError("Maximum active missions reached (10)")
        for job in active_jobs:
            if isinstance(job, dict) and job.get("mission_id") == mission_id.strip():
                raise ValueError("Mission is already active")
        objective = self._mission_objective(mission=mission)
        existing_claimed = self.state_store.list_claimed_mission_ids(player_id=player_id.strip())
        repeatable = bool(mission.get("repeatable", False))
        if (not repeatable) and mission_id.strip() in set(existing_claimed):
            raise ValueError("Mission already completed and not repeatable")
        mission_payload = {
            "mission": {
                "id": mission.get("id"),
                "name": mission.get("name"),
                "category": mission.get("category"),
                "rewards": mission.get("rewards"),
                "requirements": mission.get("requirements"),
            },
            "objective": objective,
        }
        job = self.state_store.create_mission_job(
            player_id=player_id.strip(),
            mission_id=mission_id.strip(),
            objective_target=float(objective["target"]),
            payload=mission_payload,
        )
        return {
            "player_id": player_id.strip(),
            "mission": mission,
            "job": job,
        }

    def _progress_mission(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        mission_job_id = payload.get("mission_job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(mission_job_id, str) or not mission_job_id.strip():
            raise ValueError("mission_job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        before = self.state_store.get_mission_job(
            player_id=player_id.strip(),
            mission_job_id=mission_job_id.strip(),
        )
        status = str(before.get("status", "active"))
        if status in {"claimed", "abandoned"}:
            return {"player_id": player_id.strip(), "job_before": before, "job_after": before}
        mission_id = before.get("mission_id")
        mission = self._mission_index().get(mission_id) if isinstance(mission_id, str) else None
        if not isinstance(mission, dict):
            raise ValueError(f"Mission seed missing for mission_id '{mission_id}'")
        progress_value = self._mission_progress_value(player_id=player_id.strip(), mission=mission)
        target = float(before.get("objective_target", 1.0))
        status_after = "completed" if progress_value + 1e-9 >= target else "active"
        after = self.state_store.set_mission_progress(
            player_id=player_id.strip(),
            mission_job_id=mission_job_id.strip(),
            progress_value=max(progress_value, float(before.get("progress_value", 0.0))),
            status=status_after,
        )
        return {
            "player_id": player_id.strip(),
            "job_before": before,
            "job_after": after,
        }

    def _claim_mission(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        mission_job_id = payload.get("mission_job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(mission_job_id, str) or not mission_job_id.strip():
            raise ValueError("mission_job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        progress = self._progress_mission(
            {"player_id": player_id.strip(), "mission_job_id": mission_job_id.strip()}
        )
        after = progress["job_after"]
        if not isinstance(after, dict):
            raise ValueError("Mission progress update failed")
        status = str(after.get("status", "active"))
        if status == "claimed":
            return {
                "player_id": player_id.strip(),
                "job": after,
                "already_claimed": True,
            }
        if status != "completed":
            raise ValueError("Mission is not yet complete")
        mission_id = after.get("mission_id")
        mission = self._mission_index().get(mission_id) if isinstance(mission_id, str) else None
        if not isinstance(mission, dict):
            raise ValueError(f"Mission seed missing for mission_id '{mission_id}'")
        reward_payload = self._mission_reward_payload(player_id=player_id.strip(), mission=mission)
        claimed = self.state_store.set_mission_progress(
            player_id=player_id.strip(),
            mission_job_id=mission_job_id.strip(),
            progress_value=float(after.get("objective_target", 1.0)),
            status="claimed",
        )
        return {
            "player_id": player_id.strip(),
            "job_before": progress["job_before"],
            "job_claimed": claimed,
            "reward_grants": reward_payload,
        }

    def _faction_align(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        faction_id = payload.get("faction_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(faction_id, str) or not faction_id.strip():
            raise ValueError("faction_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        faction = self.seed_store.faction_index().get(faction_id.strip())
        if not isinstance(faction, dict):
            raise ValueError(f"Unknown faction_id '{faction_id}'")
        legion_membership = self.state_store.get_player_active_legion_membership(
            player_id=player_id.strip()
        )
        if (
            isinstance(legion_membership, dict)
            and isinstance(legion_membership.get("legion"), dict)
            and isinstance(legion_membership["legion"].get("faction_id"), str)
            and legion_membership["legion"]["faction_id"].strip()
            and legion_membership["legion"]["faction_id"] != faction_id.strip()
        ):
            raise ValueError(
                "Cannot change faction while in a faction-bound legion. Leave legion first."
            )
        affiliation = self.state_store.set_player_faction_affiliation(
            player_id=player_id.strip(),
            faction_id=faction_id.strip(),
            standing=0.0,
            role="member",
        )
        return {
            "player_id": player_id.strip(),
            "faction_affiliation": affiliation,
            "faction": faction,
        }

    def _faction_leave(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        legion_membership = self.state_store.get_player_active_legion_membership(
            player_id=player_id.strip()
        )
        if (
            isinstance(legion_membership, dict)
            and isinstance(legion_membership.get("legion"), dict)
            and isinstance(legion_membership["legion"].get("faction_id"), str)
            and legion_membership["legion"]["faction_id"].strip()
        ):
            raise ValueError(
                "Cannot clear faction while in a faction-bound legion. Leave legion first."
            )
        result = self.state_store.clear_player_faction_affiliation(player_id=player_id.strip())
        return {
            "player_id": player_id.strip(),
            "result": result,
        }

    def _create_legion(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        name = payload.get("name")
        tagline = payload.get("tagline")
        description = payload.get("description")
        faction_id = payload.get("faction_id")
        visibility = payload.get("visibility", "invite_only")
        min_combat_rank_raw = payload.get("min_combat_rank", 1)
        charter = payload.get("charter")
        tax_rate_pct = payload.get("tax_rate_pct")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
        if not isinstance(tagline, str) or not tagline.strip():
            raise ValueError("tagline must be a non-empty string")
        if not isinstance(description, str) or not description.strip():
            raise ValueError("description must be a non-empty string")
        if faction_id is not None and not isinstance(faction_id, str):
            raise ValueError("faction_id must be a string when provided")
        if isinstance(min_combat_rank_raw, bool) or not isinstance(min_combat_rank_raw, (int, float)):
            raise ValueError("min_combat_rank must be numeric")
        if charter is not None and not isinstance(charter, str):
            raise ValueError("charter must be a string when provided")
        if tax_rate_pct is not None and (
            isinstance(tax_rate_pct, bool) or not isinstance(tax_rate_pct, (int, float))
        ):
            raise ValueError("tax_rate_pct must be numeric when provided")
        self._ensure_player_bootstrap(player_id.strip())
        faction_value = faction_id.strip() if isinstance(faction_id, str) and faction_id.strip() else None
        if isinstance(faction_value, str):
            faction = self.seed_store.faction_index().get(faction_value)
            if not isinstance(faction, dict):
                raise ValueError(f"Unknown faction_id '{faction_value}'")
            affiliation = self.state_store.get_player_faction_affiliation(player_id=player_id.strip())
            if not isinstance(affiliation, dict):
                affiliation = self.state_store.set_player_faction_affiliation(
                    player_id=player_id.strip(),
                    faction_id=faction_value,
                    standing=0.0,
                    role="member",
                )
            elif affiliation.get("faction_id") != faction_value:
                raise ValueError("Player faction alignment must match legion faction_id")
        policy: dict[str, Any] = {}
        if isinstance(charter, str) and charter.strip():
            policy["charter"] = charter.strip()
        if isinstance(tax_rate_pct, (int, float)) and not isinstance(tax_rate_pct, bool):
            policy["tax_rate_pct"] = round(float(tax_rate_pct), 4)
        legion = self.state_store.create_legion(
            owner_player_id=player_id.strip(),
            name=name.strip(),
            tagline=tagline.strip(),
            description=description.strip(),
            faction_id=faction_value,
            visibility=str(visibility),
            min_combat_rank=int(min_combat_rank_raw),
            policy=policy,
        )
        membership = self.state_store.get_player_active_legion_membership(player_id=player_id.strip())
        return {
            "player_id": player_id.strip(),
            "legion": legion,
            "membership": membership,
        }

    def _join_legion(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        legion_id = payload.get("legion_id")
        message = payload.get("message", "")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(legion_id, str) or not legion_id.strip():
            raise ValueError("legion_id must be a non-empty string")
        if message is not None and not isinstance(message, str):
            raise ValueError("message must be a string when provided")
        self._ensure_player_bootstrap(player_id.strip())
        result = self.state_store.request_or_join_legion(
            player_id=player_id.strip(),
            legion_id=legion_id.strip(),
            message=message if isinstance(message, str) else "",
        )
        legion = self.state_store.get_legion(
            legion_id=legion_id.strip(),
            viewer_player_id=player_id.strip(),
        )
        return {
            "player_id": player_id.strip(),
            "legion": legion,
            "result": result,
        }

    def _respond_legion_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        request_id = payload.get("request_id")
        decision = payload.get("decision")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("request_id must be a non-empty string")
        if not isinstance(decision, str) or not decision.strip():
            raise ValueError("decision must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        result = self.state_store.respond_legion_join_request(
            actor_player_id=player_id.strip(),
            request_id=request_id.strip(),
            decision=decision.strip(),
        )
        return {
            "player_id": player_id.strip(),
            "result": result,
        }

    def _leave_legion(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        legion_id = payload.get("legion_id")
        successor_player_id = payload.get("successor_player_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if legion_id is not None and not isinstance(legion_id, str):
            raise ValueError("legion_id must be a string when provided")
        if successor_player_id is not None and not isinstance(successor_player_id, str):
            raise ValueError("successor_player_id must be a string when provided")
        self._ensure_player_bootstrap(player_id.strip())
        result = self.state_store.leave_legion(
            player_id=player_id.strip(),
            legion_id=legion_id.strip() if isinstance(legion_id, str) else None,
            successor_player_id=(
                successor_player_id.strip() if isinstance(successor_player_id, str) else None
            ),
        )
        membership = self.state_store.get_player_active_legion_membership(
            player_id=player_id.strip()
        )
        return {
            "player_id": player_id.strip(),
            "result": result,
            "legion_membership": membership,
        }

    def _set_legion_member_role(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        legion_id = payload.get("legion_id")
        target_player_id = payload.get("target_player_id")
        role = payload.get("role")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(legion_id, str) or not legion_id.strip():
            raise ValueError("legion_id must be a non-empty string")
        if not isinstance(target_player_id, str) or not target_player_id.strip():
            raise ValueError("target_player_id must be a non-empty string")
        if not isinstance(role, str) or not role.strip():
            raise ValueError("role must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        member = self.state_store.set_legion_member_role(
            actor_player_id=player_id.strip(),
            legion_id=legion_id.strip(),
            target_player_id=target_player_id.strip(),
            role=role.strip(),
        )
        return {
            "player_id": player_id.strip(),
            "member": member,
        }

    def _create_legion_proposal(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        legion_id = payload.get("legion_id")
        title = payload.get("title")
        proposal_type = payload.get("proposal_type")
        proposal_payload = payload.get("payload", {})
        expires_hours = payload.get("expires_hours", 48.0)
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(legion_id, str) or not legion_id.strip():
            raise ValueError("legion_id must be a non-empty string")
        if not isinstance(title, str) or not title.strip():
            raise ValueError("title must be a non-empty string")
        if not isinstance(proposal_type, str) or not proposal_type.strip():
            raise ValueError("proposal_type must be a non-empty string")
        if proposal_payload is not None and not isinstance(proposal_payload, dict):
            raise ValueError("payload must be an object when provided")
        if isinstance(expires_hours, bool) or not isinstance(expires_hours, (int, float)):
            raise ValueError("expires_hours must be numeric")
        self._ensure_player_bootstrap(player_id.strip())
        proposal = self.state_store.create_legion_proposal(
            player_id=player_id.strip(),
            legion_id=legion_id.strip(),
            title=title.strip(),
            proposal_type=proposal_type.strip(),
            payload=proposal_payload if isinstance(proposal_payload, dict) else {},
            expires_hours=float(expires_hours),
        )
        return {
            "player_id": player_id.strip(),
            "proposal": proposal,
        }

    def _vote_legion_proposal(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        proposal_id = payload.get("proposal_id")
        vote = payload.get("vote")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(proposal_id, str) or not proposal_id.strip():
            raise ValueError("proposal_id must be a non-empty string")
        if not isinstance(vote, str) or not vote.strip():
            raise ValueError("vote must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        proposal = self.state_store.cast_legion_vote(
            player_id=player_id.strip(),
            proposal_id=proposal_id.strip(),
            vote=vote.strip(),
        )
        return {
            "player_id": player_id.strip(),
            "proposal": proposal,
        }

    def _finalize_legion_proposal(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        proposal_id = payload.get("proposal_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(proposal_id, str) or not proposal_id.strip():
            raise ValueError("proposal_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        proposal = self.state_store.finalize_legion_proposal(
            actor_player_id=player_id.strip(),
            proposal_id=proposal_id.strip(),
        )
        return {
            "player_id": player_id.strip(),
            "proposal": proposal,
        }

    def _accept_contract(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        template_id = payload.get("template_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(template_id, str) or not template_id.strip():
            raise ValueError("template_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        template = self.seed_store.contract_template_index().get(template_id.strip())
        if not isinstance(template, dict):
            raise ValueError(f"Unknown template_id '{template_id}'")

        active = self.state_store.list_contract_jobs(
            player_id=player_id.strip(),
            status="active",
            limit=120,
        )
        if (not self._has_admin_privileges(player_id)) and len(active) >= 6:
            raise ValueError("Maximum active contracts reached (6)")
        if any(
            isinstance(row, dict) and row.get("template_id") == template_id.strip()
            for row in active
        ):
            raise ValueError("Contract template is already active for this player")

        objective = template.get("objective", {})
        if not isinstance(objective, dict):
            raise ValueError(f"Contract template '{template_id}' has invalid objective payload")
        required_value_raw = objective.get("required_value")
        if isinstance(required_value_raw, bool) or not isinstance(required_value_raw, (int, float)):
            raise ValueError(f"Contract template '{template_id}' missing objective.required_value")
        objective_target = max(1.0, float(required_value_raw))
        duration_raw = template.get("base_duration_hours", 12)
        if isinstance(duration_raw, bool) or not isinstance(duration_raw, (int, float)):
            duration_raw = 12
        duration_hours = max(1.0, min(168.0, float(duration_raw)))
        expires_epoch = int(time.time() + duration_hours * 3600.0)
        expires_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_epoch))
        contract_payload = {
            "template": template,
            "accepted_context": {
                "accepted_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "region_id": payload.get("region_id"),
            },
        }
        job = self.state_store.create_contract_job(
            player_id=player_id.strip(),
            template_id=template_id.strip(),
            objective_target=objective_target,
            expires_utc=expires_utc,
            payload=contract_payload,
        )
        return {
            "player_id": player_id.strip(),
            "job": job,
            "template": template,
        }

    def _complete_contract(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        contract_job_id = payload.get("contract_job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(contract_job_id, str) or not contract_job_id.strip():
            raise ValueError("contract_job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())

        current = self.state_store.get_contract_job(
            player_id=player_id.strip(),
            contract_job_id=contract_job_id.strip(),
        )
        status = str(current.get("status", "active"))
        if status in {"expired", "abandoned"}:
            raise ValueError(f"Cannot complete contract in status '{status}'")
        if status == "claimed":
            return {
                "player_id": player_id.strip(),
                "job": current,
                "already_claimed": True,
            }
        template = self.seed_store.contract_template_index().get(str(current["template_id"]), {})
        objective = template.get("objective", {}) if isinstance(template, dict) else {}
        if not isinstance(objective, dict):
            objective = {}
        objective_kind = str(objective.get("kind", ""))
        objective_symbol = objective.get("symbol")
        objective_required_raw = objective.get("required_value", current.get("objective_target", 1.0))
        objective_required = (
            float(objective_required_raw)
            if isinstance(objective_required_raw, (int, float)) and not isinstance(objective_required_raw, bool)
            else float(current.get("objective_target", 1.0))
        )
        target = max(1.0, objective_required)
        progress_value = 0.0
        delivery_deltas: dict[str, float] = {}
        if objective_kind == "deliver_element":
            if not isinstance(objective_symbol, str) or not objective_symbol.strip():
                raise ValueError("Contract objective missing deliver_element symbol")
            inventory = self.state_store.get_inventory_amounts(
                player_id=player_id.strip(),
                symbols=[objective_symbol.strip()],
            )
            available = float(inventory.get(objective_symbol.strip(), 0.0))
            progress_value = available
            if progress_value + 1e-9 >= target:
                delivery_deltas[objective_symbol.strip()] = -target
            else:
                raise ValueError(
                    "Contract objective unmet: need {:.2f} {}, available {:.2f}".format(
                        target,
                        objective_symbol.strip(),
                        available,
                    )
                )
        elif objective_kind == "win_battles":
            metrics = self.state_store.get_battle_metrics(player_id=player_id.strip())
            progress_value = float(metrics.get("battles_won", 0))
        elif objective_kind == "scan_worlds":
            progress_value = float(self.state_store.count_discovered_worlds(player_id=player_id.strip()))
        elif objective_kind == "build_structures":
            progress_value = float(self.state_store.count_world_structures(player_id=player_id.strip()))
        else:
            raise ValueError(
                f"Unsupported contract objective kind '{objective_kind}' for objective-driven completion"
            )

        if progress_value + 1e-9 < target:
            raise ValueError(
                "Contract objective unmet ({:.2f}/{:.2f}, kind={})".format(
                    progress_value,
                    target,
                    objective_kind,
                )
            )

        completed = self.state_store.set_contract_progress(
            player_id=player_id.strip(),
            contract_job_id=contract_job_id.strip(),
            progress_value=max(progress_value, target),
            status="completed",
        )
        rewards = template.get("rewards", {}) if isinstance(template, dict) else {}
        credits = 0.0
        voidcoin = 0.0
        element_deltas: dict[str, float] = {}
        xp = 0.0
        rp = 0.0
        if isinstance(rewards, dict):
            raw_credits = rewards.get("credits", 0.0)
            raw_voidcoin = rewards.get("voidcoin", 0.0)
            raw_xp = rewards.get("xp", 0.0)
            raw_rp = rewards.get("rp", 0.0)
            if isinstance(raw_credits, (int, float)) and not isinstance(raw_credits, bool):
                credits = float(raw_credits)
            if isinstance(raw_voidcoin, (int, float)) and not isinstance(raw_voidcoin, bool):
                voidcoin = float(raw_voidcoin)
            if isinstance(raw_xp, (int, float)) and not isinstance(raw_xp, bool):
                xp = float(raw_xp)
            if isinstance(raw_rp, (int, float)) and not isinstance(raw_rp, bool):
                rp = float(raw_rp)
            rows = rewards.get("elements", [])
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    symbol = row.get("symbol")
                    amount = row.get("amount")
                    if not isinstance(symbol, str):
                        continue
                    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
                        continue
                    if float(amount) <= 0:
                        continue
                    element_deltas[symbol] = element_deltas.get(symbol, 0.0) + float(amount)

        combined_deltas = dict(element_deltas)
        for symbol, delta in delivery_deltas.items():
            combined_deltas[symbol] = combined_deltas.get(symbol, 0.0) + float(delta)
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id.strip(),
            credits_delta=credits,
            voidcoin_delta=voidcoin,
            element_deltas=combined_deltas,
        )
        combat_progress_after = self.state_store.get_combat_progress(player_id=player_id.strip())
        ship_progress_after = self._ensure_fleet_initialized(player_id=player_id.strip())
        if xp > 0:
            combat_progress_after = self.state_store.grant_combat_xp(
                player_id=player_id.strip(),
                xp_delta=xp,
            )
            ship_progress_after = self.state_store.grant_fleet_xp(
                player_id=player_id.strip(),
                xp_delta=max(0.0, xp * 0.55),
            )
        claimed = self.state_store.set_contract_progress(
            player_id=player_id.strip(),
            contract_job_id=contract_job_id.strip(),
            progress_value=max(progress_value, target),
            status="claimed",
        )
        return {
            "player_id": player_id.strip(),
            "job_before": current,
            "job_completed": completed,
            "job_claimed": claimed,
            "reward_grants": {
                "credits": round(credits, 4),
                "voidcoin": round(voidcoin, 8),
                "elements": [
                    {"symbol": symbol, "amount": round(amount, 3)}
                    for symbol, amount in sorted(element_deltas.items())
                ],
                "objective_costs": [
                    {"symbol": symbol, "amount": round(amount, 3)}
                    for symbol, amount in sorted(delivery_deltas.items())
                ],
                "xp": round(xp, 3),
                "rp": round(rp, 3),
            },
            "combat_progress_after": combat_progress_after,
            "ship_progress_after": ship_progress_after,
            "wallet": resource_state["wallet"],
            "inventory_changes": resource_state["inventory"],
        }

    def _abandon_contract(self, payload: dict[str, Any]) -> dict[str, Any]:
        player_id = payload.get("player_id")
        contract_job_id = payload.get("contract_job_id")
        if not isinstance(player_id, str) or not player_id.strip():
            raise ValueError("player_id must be a non-empty string")
        if not isinstance(contract_job_id, str) or not contract_job_id.strip():
            raise ValueError("contract_job_id must be a non-empty string")
        self._ensure_player_bootstrap(player_id.strip())
        current = self.state_store.get_contract_job(
            player_id=player_id.strip(),
            contract_job_id=contract_job_id.strip(),
        )
        status = str(current.get("status", "active"))
        if status == "claimed":
            raise ValueError("Claimed contracts cannot be abandoned")
        if status in {"abandoned", "expired"}:
            return {
                "player_id": player_id.strip(),
                "job_before": current,
                "job_after": current,
                "idempotent": True,
            }
        updated = self.state_store.set_contract_progress(
            player_id=player_id.strip(),
            contract_job_id=contract_job_id.strip(),
            progress_value=float(current.get("progress_value", 0.0)),
            status="abandoned",
        )
        return {
            "player_id": player_id.strip(),
            "job_before": current,
            "job_after": updated,
        }

    def _world_is_depletable(self, world: dict[str, Any]) -> bool:
        body_class = world.get("body_class")
        if isinstance(body_class, str) and body_class in {"asteroid", "comet"}:
            return True
        marker = world.get("is_depletable")
        return isinstance(marker, bool) and marker

    def _normalize_world_resource_model(self, world: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(world)
        lodes_raw = normalized.get("element_lodes")
        lodes = lodes_raw if isinstance(lodes_raw, list) else []
        normalized_lodes: list[dict[str, Any]] = []
        total_units = 0
        for row in lodes:
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol")
            units_raw = row.get("estimated_units", 0)
            if not isinstance(symbol, str) or not symbol.strip():
                continue
            if isinstance(units_raw, bool) or not isinstance(units_raw, (int, float)):
                units = 0
            else:
                units = max(0, int(round(float(units_raw))))
            next_row = dict(row)
            next_row["estimated_units"] = units
            normalized_lodes.append(next_row)
            total_units += units

        normalized["element_lodes"] = normalized_lodes
        normalized["estimated_total_units"] = int(total_units)
        depletable = self._world_is_depletable(normalized)
        if depletable:
            initial_raw = normalized.get("initial_total_units")
            if isinstance(initial_raw, bool) or not isinstance(initial_raw, (int, float)):
                initial_total = int(total_units)
            else:
                initial_total = int(round(float(initial_raw)))
            initial_total = max(initial_total, int(total_units))
            normalized["initial_total_units"] = int(initial_total)
            normalized["remaining_total_units"] = int(total_units)
            normalized["is_depletable"] = True
            normalized["resource_model"] = "finite_ore_body"
            normalized["depleted"] = bool(total_units <= 0)
            normalized["depletion_ratio"] = round(
                1.0 - (float(total_units) / float(max(1, initial_total))),
                4,
            )
        else:
            normalized["initial_total_units"] = None
            normalized["remaining_total_units"] = None
            normalized["is_depletable"] = False
            normalized["resource_model"] = "renewable_or_deep_cycle_reservoir"
            normalized["depleted"] = False
            normalized["depletion_ratio"] = 0.0
        return normalized

    def _harvest_world(self, player_id: str, world_id: str, hours: float) -> dict[str, Any]:
        world = self.state_store.get_world(world_id=world_id, player_id=player_id)
        world = self._normalize_world_resource_model(world)
        depletable = self._world_is_depletable(world)
        projection = self._project_world_structure(
            {"world": world, "structure_ids": world.get("built_structures", [])}
        )
        if DETERMINISTIC_MODE:
            rng_seed = int(stable_hash_int(player_id, world_id, round(hours, 3), "harvest"))
        else:
            rng_seed = int(time.time()) ^ int(stable_hash_int(player_id, world_id, "harvest"))
        rng = random.Random(rng_seed)

        lodes = world.get("element_lodes")
        remaining_by_symbol: dict[str, float] = {}
        if isinstance(lodes, list):
            for row in lodes:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                units_raw = row.get("estimated_units")
                if not isinstance(symbol, str):
                    continue
                if isinstance(units_raw, bool) or not isinstance(units_raw, (int, float)):
                    continue
                remaining_by_symbol[symbol] = remaining_by_symbol.get(symbol, 0.0) + max(0.0, float(units_raw))

        deltas: dict[str, float] = {}
        harvested: list[dict[str, Any]] = []
        depleted_symbols: list[str] = []
        for row in projection.get("output_preview", []):
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol")
            hourly_units = row.get("hourly_units")
            if not isinstance(symbol, str):
                continue
            if isinstance(hourly_units, bool) or not isinstance(hourly_units, (int, float)):
                continue
            noise = rng.uniform(0.94, 1.06)
            amount = max(0.0, float(hourly_units) * hours * noise)
            if depletable:
                remaining_units = max(0.0, float(remaining_by_symbol.get(symbol, 0.0)))
                if remaining_units <= 0.0:
                    continue
                if amount > remaining_units:
                    amount = remaining_units
                remaining_by_symbol[symbol] = max(0.0, remaining_units - amount)
                if remaining_by_symbol[symbol] <= 1e-9:
                    depleted_symbols.append(symbol)
            if amount <= 0:
                continue
            deltas[symbol] = deltas.get(symbol, 0.0) + amount
            harvested.append(
                {
                    "symbol": symbol,
                    "amount": round(amount, 3),
                    "rare_class": bool(row.get("rare_class", False)),
                }
            )

        harvested.sort(key=lambda item: float(item["amount"]), reverse=True)
        resource_state = self.state_store.apply_resource_delta(
            player_id=player_id,
            element_deltas=deltas,
        )

        world_after = world
        depletion_summary: dict[str, Any] = {
            "is_depletable": bool(depletable),
            "resource_model": world.get("resource_model"),
            "initial_total_units": world.get("initial_total_units"),
            "remaining_total_units_before": world.get("remaining_total_units"),
            "remaining_total_units_after": world.get("remaining_total_units"),
            "depleted": bool(world.get("depleted", False)),
            "depletion_ratio": world.get("depletion_ratio"),
            "depleted_symbols": sorted(set(depleted_symbols)),
        }
        if depletable:
            consumed_by_symbol = {
                symbol: max(0.0, float(deltas.get(symbol, 0.0)))
                for symbol in remaining_by_symbol.keys()
            }
            updated_lodes: list[dict[str, Any]] = []
            for row in lodes if isinstance(lodes, list) else []:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                units_raw = row.get("estimated_units", 0)
                if isinstance(units_raw, bool) or not isinstance(units_raw, (int, float)):
                    units = 0.0
                else:
                    units = max(0.0, float(units_raw))
                if isinstance(symbol, str):
                    consume_left = max(0.0, float(consumed_by_symbol.get(symbol, 0.0)))
                    if consume_left > 1e-9 and units > 0.0:
                        applied = min(units, consume_left)
                        units -= applied
                        consumed_by_symbol[symbol] = consume_left - applied
                next_row = dict(row)
                next_row["estimated_units"] = int(round(max(0.0, units)))
                updated_lodes.append(next_row)
            world_after = dict(world)
            world_after["element_lodes"] = updated_lodes
            world_after = self._normalize_world_resource_model(world_after)
            world_after = self.state_store.update_world_payload(player_id=player_id, world=world_after)
            depletion_summary = {
                "is_depletable": True,
                "resource_model": "finite_ore_body",
                "initial_total_units": world_after.get("initial_total_units"),
                "remaining_total_units_before": world.get("remaining_total_units"),
                "remaining_total_units_after": world_after.get("remaining_total_units"),
                "depleted": bool(world_after.get("depleted", False)),
                "depletion_ratio": world_after.get("depletion_ratio"),
                "depleted_symbols": sorted(set(depleted_symbols)),
            }

        return {
            "harvest_id": str(uuid.uuid4()),
            "player_id": player_id,
            "world_id": world_id,
            "hours": round(hours, 3),
            "harvested": harvested,
            "wallet": resource_state["wallet"],
            "world": world_after,
            "depletion": depletion_summary,
            "inventory_changes": {
                symbol: round(value, 3)
                for symbol, value in resource_state["inventory"].items()
            },
            "projection_summary": projection.get("summary"),
        }

    def _player_discovery_profile(self, player_id: str | None) -> dict[str, Any]:
        baseline = {
            "player_id": None,
            "combat_rank": 1,
            "owned_worlds": 0,
            "discovered_bodies": 0,
            "unlocked_tech": 0,
            "active_hull_tier": 1,
            "scan_module_bonus": 0.0,
            "scan_rating_bonus": 0.0,
            "difficulty_bias": 0.0,
            "progression_index": 0.0,
        }
        if not isinstance(player_id, str) or not player_id.strip():
            return baseline

        player_key = player_id.strip()
        try:
            progress = self.state_store.get_combat_progress(player_id=player_key)
            combat_rank_raw = progress.get("combat_rank", 1)
            combat_rank = int(combat_rank_raw) if isinstance(combat_rank_raw, int) else 1
            owned_worlds = self.state_store.list_worlds_for_player(player_id=player_key)
            discovered = self.state_store.list_discovered_worlds(player_id=player_key, limit=4000)
            unlocked_tech = self.state_store.list_unlocked_tech(player_id=player_key, limit=1600)
        except StateStoreError:
            return baseline
        faction_profile = self._player_faction_bonus_profile(player_id=player_key)
        faction_bonuses = faction_profile.get("bonuses", {})
        faction_scan_bonus_pct = (
            float(faction_bonuses.get("scan_pct", 0.0))
            if isinstance(faction_bonuses, dict)
            and isinstance(faction_bonuses.get("scan_pct"), (int, float))
            and not isinstance(faction_bonuses.get("scan_pct"), bool)
            else 0.0
        )

        module_index = self.seed_store.module_index()
        module_assets = self.state_store.list_assets(
            player_id=player_key,
            asset_type="module",
            limit=700,
        )
        scan_module_bonus = 0.0
        for row in module_assets:
            asset_id = row.get("asset_id")
            quantity_raw = row.get("quantity", 0)
            if not isinstance(asset_id, str):
                continue
            if isinstance(quantity_raw, bool) or not isinstance(quantity_raw, (int, float)):
                continue
            quantity = max(0.0, float(quantity_raw))
            if quantity <= 0:
                continue
            module = module_index.get(asset_id)
            if not isinstance(module, dict):
                continue
            stat_bonuses = module.get("stat_bonuses", {})
            if not isinstance(stat_bonuses, dict):
                continue
            scan_raw = stat_bonuses.get("scan", 0.0)
            if isinstance(scan_raw, (int, float)) and not isinstance(scan_raw, bool):
                scan_module_bonus += float(scan_raw) * quantity
            lock_raw = stat_bonuses.get("sensor_lock", 0.0)
            if isinstance(lock_raw, (int, float)) and not isinstance(lock_raw, bool):
                scan_module_bonus += float(lock_raw) * 0.35 * quantity

        fleet = self._ensure_fleet_initialized(player_id=player_key)
        active_hull_id = fleet.get("active_hull_id")
        active_hull_tier = 1
        if isinstance(active_hull_id, str):
            hull = self.seed_store.hull_index().get(active_hull_id)
            if isinstance(hull, dict):
                tier_raw = hull.get("tier", 1)
                if isinstance(tier_raw, int):
                    active_hull_tier = max(1, tier_raw)

        progression_index = (
            (combat_rank * 0.85)
            + (len(unlocked_tech) * 0.055)
            + (len(discovered) * 0.022)
            + (len(owned_worlds) * 1.25)
            + (active_hull_tier * 1.1)
        )
        difficulty_bias = max(0.0, min(1.0, progression_index / 56.0))
        scan_rating_bonus = (
            (combat_rank * 1.35)
            + min(58.0, len(unlocked_tech) * 0.11)
            + min(36.0, len(discovered) * 0.05)
            + min(96.0, scan_module_bonus * 0.14)
            + (active_hull_tier * 2.4)
        )
        if abs(faction_scan_bonus_pct) > 1e-9:
            scan_rating_bonus *= 1.0 + (faction_scan_bonus_pct / 100.0)
            difficulty_bias = max(
                0.0,
                min(1.0, difficulty_bias - min(0.08, max(0.0, faction_scan_bonus_pct) * 0.0022)),
            )
        return {
            "player_id": player_key,
            "combat_rank": int(combat_rank),
            "owned_worlds": int(len(owned_worlds)),
            "discovered_bodies": int(len(discovered)),
            "unlocked_tech": int(len(unlocked_tech)),
            "active_hull_tier": int(active_hull_tier),
            "scan_module_bonus": round(scan_module_bonus, 3),
            "faction_scan_bonus_pct": round(faction_scan_bonus_pct, 3),
            "scan_rating_bonus": round(scan_rating_bonus, 3),
            "difficulty_bias": round(difficulty_bias, 4),
            "progression_index": round(progression_index, 4),
            "faction_profile": faction_profile,
        }

    def _pick_discovery_template(
        self,
        templates: list[dict[str, Any]],
        rng: random.Random,
        target_difficulty: float,
    ) -> dict[str, Any]:
        weighted: list[tuple[dict[str, Any], float]] = []
        for template in templates:
            if not isinstance(template, dict):
                continue
            raw_diff = template.get("scan_difficulty", 1.0)
            raw_yield = template.get("mining_yield_factor", 1.0)
            scan_difficulty = float(raw_diff) if isinstance(raw_diff, (int, float)) else 1.0
            mining_factor = float(raw_yield) if isinstance(raw_yield, (int, float)) else 1.0
            distance = abs(scan_difficulty - target_difficulty)
            weight = 1.45 / (1.0 + (distance * 2.25))
            if scan_difficulty <= target_difficulty:
                weight *= 1.0 + min(0.45, (target_difficulty - scan_difficulty) * 0.16)
            else:
                weight *= max(0.18, 1.0 - ((scan_difficulty - target_difficulty) * 0.24))
            weight *= 0.85 + min(0.45, (max(0.3, mining_factor) - 0.7) * 0.45)
            weighted.append((template, max(0.01, weight)))
        if not weighted:
            raise ValueError("No celestial templates available for weighted discovery selection")
        total = sum(weight for _, weight in weighted)
        roll = rng.uniform(0.0, total)
        cursor = 0.0
        for template, weight in weighted:
            cursor += weight
            if roll <= cursor:
                return template
        return weighted[-1][0]

    def _build_world_designation(self, body_class: str, rng: random.Random) -> str:
        if body_class == "comet":
            year = rng.randrange(2120, 2299)
            half_month = "ABCDEFGHJKLMNOPQRSTUVWXYZ"[rng.randrange(0, 24)]
            seq = rng.randrange(1, 200)
            return f"C/{year} {half_month}{seq}"
        if body_class == "asteroid":
            year = rng.randrange(2120, 2299)
            half_month = "ABCDEFGHJKLMNOPQRSTUVWXYZ"[rng.randrange(0, 24)]
            seq = rng.randrange(1, 900)
            return f"SSA {year} {half_month}{seq}"
        if body_class == "star":
            ra_h = rng.randrange(0, 24)
            ra_m = rng.randrange(0, 60)
            dec_sign = "+" if rng.random() >= 0.5 else "-"
            dec_d = rng.randrange(0, 90)
            dec_m = rng.randrange(0, 60)
            return f"SS J{ra_h:02d}{ra_m:02d}{dec_sign}{dec_d:02d}{dec_m:02d}"
        sector = "".join(chr(rng.randrange(65, 91)) for _ in range(3))
        serial = rng.randrange(100, 999)
        suffix = chr(ord("b") + rng.randrange(0, 9))
        prefix = {
            "planet": "SSP",
            "moon": "SSM",
            "gas_giant": "SSG",
        }.get(body_class, "SSX")
        return f"{prefix}-{sector}-{serial}{suffix}"

    def _run_discovery_scan(
        self,
        body_class: str | None,
        count: int,
        seed: int,
        scan_power: float,
        player_id: str | None = None,
    ) -> dict[str, Any]:
        templates = self.seed_store.celestial_templates
        if body_class is not None:
            body_key = body_class.casefold()
            templates = [
                item
                for item in templates
                if isinstance(item, dict)
                and isinstance(item.get("body_class"), str)
                and item["body_class"].casefold() == body_key
            ]

        if not templates:
            raise ValueError("No celestial templates available for requested body_class")

        discovery_profile = self._player_discovery_profile(player_id=player_id)
        scan_rating_bonus = float(discovery_profile.get("scan_rating_bonus", 0.0))
        difficulty_bias = float(discovery_profile.get("difficulty_bias", 0.0))
        effective_scan_power = max(10.0, scan_power + scan_rating_bonus)
        target_difficulty = max(
            0.6,
            min(
                3.0,
                1.0
                + (difficulty_bias * 1.08)
                + max(0.0, (effective_scan_power - 100.0) / 260.0),
            ),
        )

        rng = random.Random(seed)
        element_index = self.seed_store.elements_by_symbol()
        items: list[dict[str, Any]] = []
        for _ in range(count):
            template = self._pick_discovery_template(
                templates=templates,
                rng=rng,
                target_difficulty=target_difficulty,
            )
            scan_difficulty_raw = template.get("scan_difficulty", 1.0)
            scan_difficulty = (
                float(scan_difficulty_raw)
                if isinstance(scan_difficulty_raw, (int, float))
                else 1.0
            )
            detection_score = (effective_scan_power / 100.0) * (1.0 + (difficulty_bias * 0.16))
            threshold = scan_difficulty * rng.uniform(0.86, 1.24)
            confidence = 1.0 / (1.0 + math.exp((threshold - detection_score) * 4.1))
            detection_confidence = max(0.06, min(0.995, confidence))
            items.append(
                self._build_discovered_body(
                    template=template,
                    rng=rng,
                    scan_power=effective_scan_power,
                    element_index=element_index,
                    detection_confidence=detection_confidence,
                    progression_bias=difficulty_bias,
                )
            )

        return {
            "scan_id": str(uuid.uuid4()),
            "seed": seed,
            "player_id": player_id.strip() if isinstance(player_id, str) and player_id.strip() else None,
            "body_class_filter": body_class,
            "scan_power": round(scan_power, 2),
            "effective_scan_power": round(effective_scan_power, 3),
            "target_difficulty": round(target_difficulty, 4),
            "count": count,
            "items": items,
            "discovery_profile": discovery_profile,
        }

    def _base_units_for_body_class(self, body_class: str) -> int:
        base = {
            "asteroid": 7000,
            "comet": 9000,
            "moon": 18000,
            "planet": 52000,
            "gas_giant": 90000,
            "star": 240000,
        }
        return base.get(body_class, 12000)

    def _build_discovered_body(
        self,
        template: dict[str, Any],
        rng: random.Random,
        scan_power: float,
        element_index: dict[str, dict[str, Any]],
        detection_confidence: float,
        progression_bias: float,
    ) -> dict[str, Any]:
        body_class = str(template.get("body_class", "unknown"))
        scan_difficulty = float(template.get("scan_difficulty", 1.0))
        mining_yield_factor_raw = template.get("mining_yield_factor", 1.0)
        mining_yield_factor = (
            float(mining_yield_factor_raw)
            if isinstance(mining_yield_factor_raw, (int, float))
            else 1.0
        )
        scan_factor = max(0.22, min(3.4, (scan_power / 100.0) / max(0.1, scan_difficulty)))
        richness = max(
            0.12,
            min(
                3.8,
                scan_factor
                * rng.uniform(0.72, 1.32)
                * max(0.35, mining_yield_factor)
                * (0.62 + (0.56 * max(0.0, min(1.0, detection_confidence)))),
            ),
        )
        base_units = self._base_units_for_body_class(body_class)

        elements: list[dict[str, Any]] = []

        def append_composition(composition: list[dict[str, Any]], trace_scale: float) -> None:
            for row in composition:
                if not isinstance(row, dict):
                    continue
                symbol = row.get("symbol")
                ratio_pct = row.get("ratio_pct")
                if not isinstance(symbol, str):
                    continue
                if isinstance(ratio_pct, bool) or not isinstance(ratio_pct, (int, float)):
                    continue
                if ratio_pct <= 0:
                    continue

                element = element_index.get(symbol, {})
                units = int(
                    round(
                        base_units
                        * (float(ratio_pct) / 100.0)
                        * richness
                        * trace_scale
                        * rng.uniform(0.82, 1.18)
                    )
                )
                if units <= 0:
                    continue
                elements.append(
                    {
                        "symbol": symbol,
                        "name": element.get("name", symbol),
                        "ratio_pct": round(float(ratio_pct), 3),
                        "estimated_units": units,
                        "atomic_number": element.get("atomic_number"),
                    }
                )

        major = template.get("major_composition", [])
        if isinstance(major, list):
            append_composition(major, trace_scale=1.0)
        trace = template.get("trace_composition", [])
        if isinstance(trace, list):
            append_composition(trace, trace_scale=0.2)

        if not elements and isinstance(major, list) and len(major) > 0 and isinstance(major[0], dict):
            fallback_symbol = major[0].get("symbol")
            if isinstance(fallback_symbol, str):
                fallback_element = element_index.get(fallback_symbol, {})
                elements.append(
                    {
                        "symbol": fallback_symbol,
                        "name": fallback_element.get("name", fallback_symbol),
                        "ratio_pct": 100.0,
                        "estimated_units": max(120, int(round(base_units * 0.02))),
                        "atomic_number": fallback_element.get("atomic_number"),
                    }
                )

        elements.sort(key=lambda item: int(item.get("estimated_units", 0)), reverse=True)

        rare_symbols = {"Pt", "Pd", "Ir", "Au", "U", "Th", "Re", "W", "Pu"}
        rare_count = 0
        for row in elements:
            atomic_raw = row.get("atomic_number")
            symbol = row.get("symbol")
            is_rare = (
                isinstance(atomic_raw, int)
                and atomic_raw >= 57
            ) or (
                isinstance(symbol, str) and symbol in rare_symbols
            )
            if is_rare:
                rare_count += 1

        habitability_raw = template.get("habitability_score")
        habitability = (
            float(habitability_raw)
            if isinstance(habitability_raw, (int, float))
            else 0.0
        )
        habitability = max(0.0, min(1.0, habitability))
        hazard_base = {
            "asteroid": 0.64,
            "comet": 0.71,
            "moon": 0.46,
            "planet": 0.33,
            "gas_giant": 0.78,
            "star": 0.95,
        }.get(body_class, 0.6)
        subtype = str(template.get("subtype", "unknown"))
        subtype_key = subtype.casefold()
        if "volcanic" in subtype_key or "hot" in subtype_key:
            hazard_base += 0.11
        if "ocean" in subtype_key or "temperate" in subtype_key:
            hazard_base -= 0.08
        if "metal" in subtype_key:
            hazard_base += 0.05
        environment_hazard = max(
            0.04,
            min(
                0.995,
                hazard_base
                + (scan_difficulty * 0.07)
                + rng.uniform(-0.09, 0.12)
                - (habitability * 0.22),
            ),
        )
        hidden_signature = max(
            0.05,
            min(
                0.995,
                0.22
                + (scan_difficulty * 0.19)
                + ((1.0 - detection_confidence) * 0.42)
                + (progression_bias * 0.12)
                + rng.uniform(-0.07, 0.09),
            ),
        )
        rarity_score = max(
            0.02,
            min(
                0.995,
                0.08
                + (scan_difficulty * 0.23)
                + (richness * 0.08)
                + (rare_count * 0.027)
                + ((1.0 - detection_confidence) * 0.16)
                + rng.uniform(-0.05, 0.06),
            ),
        )

        if body_class == "planet":
            potential = (
                (220.0 + (habitability * 1450.0))
                * (1.2 - environment_hazard)
                * rng.uniform(0.72, 1.26)
            )
        elif body_class == "moon":
            potential = (
                (32.0 + (habitability * 260.0))
                * (1.18 - environment_hazard)
                * rng.uniform(0.62, 1.22)
            )
        elif body_class == "gas_giant":
            potential = max(0.0, rng.uniform(0.3, 2.8) * (1.2 - environment_hazard))
        elif body_class in {"asteroid", "comet"}:
            potential = max(0.0, rng.uniform(0.05, 1.4) * (1.05 - environment_hazard))
        else:
            potential = 0.0
        population_potential_millions = max(0, int(round(potential)))
        population_capacity = max(0, int(round(population_potential_millions * 1000.0)))
        if habitability >= 0.35 and population_capacity > 0:
            initial_ratio = max(0.002, min(0.08, 0.006 + (habitability * 0.034)))
            population_current = int(round(population_capacity * initial_ratio))
        else:
            population_current = 0
        population_growth_per_day_pct = max(
            0.0,
            round(
                (habitability * 3.4)
                - (environment_hazard * 1.2)
                + rng.uniform(0.08, 0.92),
                3,
            ),
        )

        traits: list[str] = [body_class]
        if habitability >= 0.65:
            traits.append("habitable")
        if environment_hazard >= 0.78:
            traits.append("high_hazard")
        if rare_count >= 3:
            traits.append("rare_lodes")
        if richness >= 1.45:
            traits.append("high_yield")
        if hidden_signature >= 0.82:
            traits.append("cloaked_signature")
        if rarity_score >= 0.78:
            traits.append("anomalous")
        if detection_confidence < 0.38:
            traits.append("low_confidence_scan")
        if rarity_score >= 0.86 and rng.random() < 0.12:
            traits.append("precursor_artifact_signature")
        traits.append(subtype)
        unique_traits: list[str] = []
        for trait in traits:
            trait_key = str(trait).strip().replace(" ", "_").lower()
            if not trait_key or trait_key in unique_traits:
                continue
            unique_traits.append(trait_key)

        recommended_structures = [
            structure["id"]
            for structure in self.seed_store.structures
            if isinstance(structure, dict)
            and isinstance(structure.get("id"), str)
            and isinstance(structure.get("domain"), str)
            and structure["domain"] in {body_class, "any"}
        ][:4]
        designation = self._build_world_designation(body_class=body_class, rng=rng)

        return {
            "world_id": f"world.{uuid.uuid4().hex[:12]}",
            "name": f"{template.get('name', 'Unknown Body')} {designation}",
            "designation": designation,
            "template_id": template.get("id"),
            "body_class": body_class,
            "subtype": subtype,
            "scan_difficulty": round(scan_difficulty, 3),
            "richness_multiplier": round(richness, 3),
            "habitability_score": round(habitability, 4),
            "detection_confidence": round(detection_confidence, 4),
            "rarity_score": round(rarity_score, 4),
            "environment_hazard": round(environment_hazard, 4),
            "hidden_signature": round(hidden_signature, 4),
            "population_potential_millions": int(population_potential_millions),
            "population_capacity": int(population_capacity),
            "population_current": int(max(0, population_current)),
            "population_growth_per_day_pct": round(population_growth_per_day_pct, 3),
            "estimated_total_units": sum(item["estimated_units"] for item in elements),
            "initial_total_units": sum(item["estimated_units"] for item in elements),
            "remaining_total_units": (
                sum(item["estimated_units"] for item in elements)
                if body_class in {"asteroid", "comet"}
                else None
            ),
            "is_depletable": body_class in {"asteroid", "comet"},
            "resource_model": (
                "finite_ore_body"
                if body_class in {"asteroid", "comet"}
                else "renewable_or_deep_cycle_reservoir"
            ),
            "depleted": False,
            "depletion_ratio": 0.0,
            "recommended_structures": recommended_structures,
            # Keep a broader composition slice so scans expose richer chemistry.
            "element_lodes": elements[:24],
            "traits": unique_traits[:10],
        }

    def _project_world_population(self, world: dict[str, Any], days: float) -> dict[str, Any]:
        world_id = world.get("world_id")
        if not isinstance(world_id, str) or not world_id.strip():
            raise ValueError("world.world_id must be a non-empty string")
        if isinstance(days, bool) or not isinstance(days, (int, float)):
            raise ValueError("days must be numeric")
        days = max(0.0, min(3650.0, float(days)))

        potential_raw = world.get("population_potential_millions", 0)
        potential_millions = (
            float(potential_raw)
            if isinstance(potential_raw, (int, float)) and not isinstance(potential_raw, bool)
            else 0.0
        )
        base_capacity_raw = world.get("population_capacity")
        if isinstance(base_capacity_raw, (int, float)) and not isinstance(base_capacity_raw, bool):
            base_capacity = max(0.0, float(base_capacity_raw))
        else:
            base_capacity = max(0.0, potential_millions * 1000.0)
        base_current_raw = world.get("population_current")
        if isinstance(base_current_raw, (int, float)) and not isinstance(base_current_raw, bool):
            base_current = max(0.0, float(base_current_raw))
        else:
            base_current = max(0.0, base_capacity * 0.012)
        base_growth_raw = world.get("population_growth_per_day_pct", 0.0)
        base_growth = (
            float(base_growth_raw)
            if isinstance(base_growth_raw, (int, float)) and not isinstance(base_growth_raw, bool)
            else 0.0
        )

        built_structures = world.get("built_structures", [])
        if not isinstance(built_structures, list):
            built_structures = []
        structure_index = self.seed_store.structure_index()
        capacity_bonus = 0.0
        growth_bonus = 0.0
        for structure_id in built_structures:
            if not isinstance(structure_id, str):
                continue
            structure = structure_index.get(structure_id)
            if not isinstance(structure, dict):
                continue
            modifiers = structure.get("modifiers", {})
            if not isinstance(modifiers, dict):
                continue
            cap_raw = modifiers.get("population_capacity")
            growth_raw = modifiers.get("population_growth_pct")
            if isinstance(cap_raw, (int, float)) and not isinstance(cap_raw, bool):
                capacity_bonus += max(0.0, float(cap_raw))
            if isinstance(growth_raw, (int, float)) and not isinstance(growth_raw, bool):
                growth_bonus += max(0.0, float(growth_raw))

        capacity_total = max(0.0, base_capacity + capacity_bonus)
        growth_total_pct = max(0.0, min(15.0, base_growth + growth_bonus))
        current = min(capacity_total, max(0.0, base_current))

        def grow_step(value: float, dt_days: float) -> float:
            if value <= 0.0 or capacity_total <= 0.0 or growth_total_pct <= 0.0:
                return value
            r = growth_total_pct / 100.0
            logistic = value * r * dt_days * (1.0 - (value / max(1.0, capacity_total)))
            return max(0.0, min(capacity_total, value + logistic))

        whole_days = int(math.floor(days))
        frac = max(0.0, days - whole_days)
        for _ in range(whole_days):
            current = grow_step(current, 1.0)
        if frac > 1e-9:
            current = grow_step(current, frac)

        return {
            "projection_id": str(uuid.uuid4()),
            "world_id": world_id,
            "days": round(days, 3),
            "population": {
                "start_current": round(base_current, 3),
                "projected_current": round(current, 3),
                "capacity": round(capacity_total, 3),
                "growth_per_day_pct": round(growth_total_pct, 4),
                "growth_delta": round(current - base_current, 3),
                "utilization_ratio": round(
                    current / max(1.0, capacity_total),
                    4,
                ),
            },
            "structure_modifiers": {
                "capacity_bonus": round(capacity_bonus, 3),
                "growth_bonus_pct": round(growth_bonus, 3),
                "structure_count": len([s for s in built_structures if isinstance(s, str)]),
            },
        }

    def _project_world_structure(self, payload: dict[str, Any]) -> dict[str, Any]:
        world = payload.get("world")
        if not isinstance(world, dict):
            raise ValueError("Payload must include object 'world'")
        world_id = world.get("world_id")
        if not isinstance(world_id, str) or not world_id:
            raise ValueError("world.world_id must be a non-empty string")
        body_class = world.get("body_class")
        if not isinstance(body_class, str) or not body_class:
            raise ValueError("world.body_class must be a non-empty string")

        lodes = world.get("element_lodes")
        if not isinstance(lodes, list) or not lodes:
            raise ValueError("world.element_lodes must be a non-empty array")

        structure_ids = payload.get("structure_ids")
        if not isinstance(structure_ids, list):
            raise ValueError("structure_ids must be an array")

        structure_index = {
            item["id"]: item
            for item in self.seed_store.structures
            if isinstance(item, dict) and isinstance(item.get("id"), str)
        }

        selected_structures: list[dict[str, Any]] = []
        for raw_id in structure_ids:
            if not isinstance(raw_id, str):
                raise ValueError("structure_ids must contain strings")
            structure = structure_index.get(raw_id)
            if structure is None:
                raise ValueError(f"Unknown structure id: {raw_id}")
            domain = structure.get("domain")
            if isinstance(domain, str) and domain not in {body_class, "any"}:
                raise ValueError(f"Structure {raw_id} cannot be used on body_class '{body_class}'")
            selected_structures.append(structure)

        modifiers = {
            "mining_yield_pct": 0.0,
            "rare_find_bonus_pct": 0.0,
            "scan_bonus_pct": 0.0,
            "research_yield_pct": 0.0,
            "defense_bonus_pct": 0.0,
        }
        upkeep_total = 0.0
        for structure in selected_structures:
            structure_mod = structure.get("modifiers", {})
            if isinstance(structure_mod, dict):
                for key in modifiers:
                    raw = structure_mod.get(key, 0.0)
                    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                        continue
                    modifiers[key] += float(raw)
            upkeep_raw = structure.get("upkeep_per_hour", 0.0)
            if isinstance(upkeep_raw, (int, float)) and not isinstance(upkeep_raw, bool):
                upkeep_total += float(upkeep_raw)

        mining_multiplier = max(0.05, 1.0 + (modifiers["mining_yield_pct"] / 100.0))
        rare_multiplier = max(0.05, 1.0 + (modifiers["rare_find_bonus_pct"] / 100.0))

        output_items: list[dict[str, Any]] = []
        for item in lodes:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            units = item.get("estimated_units")
            if not isinstance(symbol, str):
                continue
            if isinstance(units, bool) or not isinstance(units, (int, float)):
                continue

            atomic_number = item.get("atomic_number")
            is_rare = (
                isinstance(atomic_number, int) and atomic_number >= 57
            ) or symbol in {"Pt", "Pd", "Ir", "Au", "U", "Th", "Re", "W"}
            hourly = float(units) * 0.012 * mining_multiplier * (rare_multiplier if is_rare else 1.0)
            output_items.append(
                {
                    "symbol": symbol,
                    "hourly_units": round(hourly, 2),
                    "rare_class": is_rare,
                }
            )

        output_items.sort(key=lambda item: item["hourly_units"], reverse=True)

        return {
            "projection_id": str(uuid.uuid4()),
            "world_id": world_id,
            "body_class": body_class,
            "structures": [
                {"id": structure["id"], "name": structure.get("name")}
                for structure in selected_structures
            ],
            "modifier_totals": {key: round(value, 3) for key, value in modifiers.items()},
            "upkeep_per_hour": round(upkeep_total, 2),
            "output_preview": output_items[:12],
            "summary": {
                "top_output_symbol": output_items[0]["symbol"] if output_items else None,
                "total_hourly_units": round(
                    sum(item["hourly_units"] for item in output_items), 2
                ),
                "rare_output_items": sum(1 for item in output_items if item["rare_class"]),
            },
        }

    def _simulate_combat(self, payload: dict[str, Any]) -> dict[str, Any]:
        rng = random.Random(payload["context"]["seed"])
        mode = payload["context"]["mode"]
        max_rounds = payload["context"]["max_rounds"]
        damage_cap = max(10.0, payload["context"]["damage_cap"])
        counterfire_enabled = payload["context"]["counterfire_enabled"]
        tactical_commands = payload["context"].get("tactical_commands", {})
        commands_by_round: dict[str, dict[int, dict[str, Any]]] = {
            "attacker": {},
            "defender": {},
        }
        for side in ("attacker", "defender"):
            rows = tactical_commands.get(side, [])
            if not isinstance(rows, list):
                rows = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                round_raw = row.get("round")
                if isinstance(round_raw, int) and not isinstance(round_raw, bool):
                    commands_by_round[side][int(round_raw)] = row

        participants: dict[str, dict[str, Any]] = {}
        for side in ("attacker", "defender"):
            stats = payload[side]["stats"]
            profiles = payload[side]["profiles"]
            participants[side] = {
                "name": payload[side]["name"],
                "attack": stats["attack"],
                "defense": stats["defense"],
                "scan": stats["scan"],
                "cloak": stats["cloak"],
                "hull_max": stats["hull"],
                "hull": stats["hull"],
                "shield_max": stats["shield"],
                "shield": stats["shield"],
                "energy_max": stats["energy"],
                "energy": stats["energy"],
                "heat": 0.0,
                "thermal_capacity": max(40.0, stats["energy"] * 0.9),
                "cooling_per_round": max(4.0, 6.0 + (stats["defense"] * 0.015)),
                "energy_used": 0.0,
                "damage_dealt": 0.0,
                "damage_taken": 0.0,
                "position": 120.0 if side == "attacker" else 940.0,
                "damage_profile": profiles["damage_profile"],
                "resistance_profile": profiles["resistance_profile"],
            }

        log_entries: list[dict[str, Any]] = []
        seq = 1
        rounds_fought = 0
        distance_sum = 0.0
        strike_count = 0

        def _tactical_effect(side: str, round_number: int) -> dict[str, float | str]:
            row = commands_by_round[side].get(round_number)
            base: dict[str, float | str] = {
                "action": "none",
                "magnitude": 1.0,
                "outgoing_mult": 1.0,
                "incoming_mult": 1.0,
                "scan_bonus": 0.0,
                "cloak_bonus": 0.0,
                "crit_bonus": 0.0,
                "energy_mult": 1.0,
                "heat_flat": 0.0,
            }
            if not isinstance(row, dict):
                return base
            action = str(row.get("action", "none"))
            magnitude = row.get("magnitude", 1.0)
            if isinstance(magnitude, bool) or not isinstance(magnitude, (int, float)):
                magnitude = 1.0
            m = max(0.2, min(3.0, float(magnitude)))
            base["action"] = action
            base["magnitude"] = m
            if action == "main_ability":
                base["outgoing_mult"] = 1.0 + (0.16 * m)
                base["crit_bonus"] = 0.01 * m
                base["energy_mult"] = 1.0 + (0.12 * m)
                base["heat_flat"] = 2.0 + (3.0 * m)
            elif action == "boost_thrust":
                base["scan_bonus"] = 8.0 * m
                base["cloak_bonus"] = 5.0 * m
                base["incoming_mult"] = max(0.55, 1.0 - (0.14 * m))
                base["energy_mult"] = 1.0 + (0.08 * m)
                base["heat_flat"] = 1.0 + (2.0 * m)
            elif action == "evade":
                base["outgoing_mult"] = max(0.45, 1.0 - (0.18 * m))
                base["incoming_mult"] = max(0.40, 1.0 - (0.20 * m))
                base["crit_bonus"] = -(0.008 * m)
                base["energy_mult"] = 1.0 + (0.05 * m)
            elif action == "stealth_burst":
                base["outgoing_mult"] = 1.0 + (0.08 * m)
                base["incoming_mult"] = max(0.38, 1.0 - (0.22 * m))
                base["scan_bonus"] = -(3.0 * m)
                base["cloak_bonus"] = 18.0 * m
                base["crit_bonus"] = 0.012 * m
                base["energy_mult"] = 1.0 + (0.14 * m)
                base["heat_flat"] = 2.6 + (2.6 * m)
            return base

        def _movement_step(round_number: int) -> None:
            attacker = participants["attacker"]
            defender = participants["defender"]
            attacker_fx = _tactical_effect("attacker", round_number)
            defender_fx = _tactical_effect("defender", round_number)

            if attacker["hull"] > 0:
                drift = rng.uniform(-24.0, 24.0)
                attacker["position"] += drift
                if attacker_fx["action"] == "boost_thrust":
                    delta = 86.0 * float(attacker_fx["magnitude"])
                    if attacker["position"] < defender["position"]:
                        attacker["position"] += delta
                    else:
                        attacker["position"] -= delta
                elif attacker_fx["action"] == "evade":
                    attacker["position"] += rng.uniform(-48.0, 48.0) * float(
                        attacker_fx["magnitude"]
                    )

            if defender["hull"] > 0:
                drift = rng.uniform(-24.0, 24.0)
                defender["position"] += drift
                if defender_fx["action"] == "boost_thrust":
                    delta = 74.0 * float(defender_fx["magnitude"])
                    if defender["position"] > attacker["position"]:
                        defender["position"] += delta
                    else:
                        defender["position"] -= delta
                elif defender_fx["action"] == "evade":
                    defender["position"] += rng.uniform(-48.0, 48.0) * float(
                        defender_fx["magnitude"]
                    )

            attacker["position"] = max(0.0, min(1400.0, attacker["position"]))
            defender["position"] = max(0.0, min(1400.0, defender["position"]))

        def strike(actor_key: str, target_key: str, round_number: int) -> None:
            nonlocal seq
            nonlocal distance_sum
            nonlocal strike_count
            actor = participants[actor_key]
            target = participants[target_key]
            if actor["hull"] <= 0 or target["hull"] <= 0:
                return

            actor_fx = _tactical_effect(actor_key, round_number)
            target_fx = _tactical_effect(target_key, round_number)
            actor_scan_total = actor["scan"] + float(actor_fx["scan_bonus"])
            target_cloak_total = target["cloak"] + float(target_fx["cloak_bonus"])
            scan_advantage = max(
                0.0,
                actor_scan_total - target_cloak_total,
            )
            scan_deficit = max(0.0, target_cloak_total - actor_scan_total)
            crit_chance = min(
                0.45,
                max(
                    0.01,
                    0.04 + (scan_advantage / 700.0) + float(actor_fx["crit_bonus"]),
                ),
            )
            critical = rng.random() < crit_chance
            random_factor = rng.uniform(0.72, 1.28)
            distance = abs(float(actor["position"]) - float(target["position"]))
            distance_sum += distance
            strike_count += 1
            range_efficiency = max(0.55, min(1.18, 1.08 - (distance / 2300.0)))
            if str(actor_fx["action"]) == "main_ability":
                range_efficiency = max(0.85, range_efficiency)
            stealth_evasion_factor = max(0.56, 1.0 - (scan_deficit / 900.0))
            if str(target_fx["action"]) == "stealth_burst":
                stealth_evasion_factor *= 0.94

            ratio = (actor["attack"] + 1.0) / max(1.0, target["defense"] + 1.0)
            gl_curve = 0.75 + 0.55 * math.tanh((ratio - 1.0) * 1.25)
            base_damage = max(
                1.0,
                actor["attack"]
                * 0.52
                * gl_curve
                * random_factor
                * range_efficiency
                * stealth_evasion_factor
                * float(actor_fx["outgoing_mult"]),
            )
            if str(actor_fx["action"]) == "stealth_burst":
                base_damage *= 1.03 + (0.045 * float(actor_fx["magnitude"]))

            if critical:
                base_damage *= rng.uniform(1.35, 1.7)

            energy_taxonomy = (
                float(actor["damage_profile"].get("thermal", 0.0))
                + float(actor["damage_profile"].get("plasma", 0.0))
                + float(actor["damage_profile"].get("ion", 0.0))
            )
            energy_cost = (
                max(3.0, actor["attack"] * 0.035) * float(actor_fx["energy_mult"])
            )
            energy_cost *= 1.0 + (0.28 * energy_taxonomy)
            power_deficit = 0.0
            if actor["energy"] < energy_cost:
                power_deficit = (energy_cost - actor["energy"]) / energy_cost
                base_damage *= max(0.25, 1.0 - (0.7 * power_deficit))
                energy_cost = actor["energy"]

            actor["energy"] = max(0.0, actor["energy"] - energy_cost)
            actor["energy_used"] += energy_cost

            actor["heat"] += 0.65 * energy_cost + (0.1 * actor["attack"])
            actor["heat"] += float(actor_fx["heat_flat"])
            heat_ratio = actor["heat"] / actor["thermal_capacity"]
            if heat_ratio > 0.8:
                throttle = min(0.7, (heat_ratio - 0.8) * 0.6)
                base_damage *= max(0.3, 1.0 - throttle)

            weighted_resistance = 0.0
            for dtype in DAMAGE_TYPES:
                weighted_resistance += float(actor["damage_profile"].get(dtype, 0.0)) * float(
                    target["resistance_profile"].get(dtype, 0.0)
                )
            scan_penetration = scan_advantage / 2200.0
            ratio_penetration = (
                ((actor["attack"] / max(1.0, target["defense"])) - 1.0) * 0.03
            )
            penetration = min(
                0.2,
                max(
                    0.0,
                    scan_penetration + ratio_penetration,
                ),
            )
            effective_resistance = max(0.02, min(0.72, weighted_resistance - penetration))
            mitigation_factor = max(0.28, 1.0 - effective_resistance)
            raw_damage = min(
                damage_cap,
                max(
                    1.0,
                    base_damage * float(target_fx["incoming_mult"]) * mitigation_factor,
                ),
            )
            shield_damage = min(target["shield"], raw_damage)
            hull_damage = min(target["hull"], max(0.0, raw_damage - shield_damage))

            target["shield"] = max(0.0, target["shield"] - shield_damage)
            target["hull"] = max(0.0, target["hull"] - hull_damage)
            target["damage_taken"] += shield_damage + hull_damage
            actor["damage_dealt"] += shield_damage + hull_damage

            note = "critical strike" if critical else "standard exchange"
            if power_deficit > 0:
                note = "power deficit throttled output"
            elif heat_ratio > 1.0:
                note = "thermal overload throttled output"
            elif scan_advantage > 0:
                note = "scan advantage applied"
            if actor_fx["action"] != "none":
                note = f"{note}; tactical:{actor_fx['action']}"

            log_entries.append(
                {
                    "seq": seq,
                    "round": round_number,
                    "phase": actor_key,
                    "actor": actor["name"],
                    "target": target["name"],
                    "critical": critical,
                    "scan_advantage": round(scan_advantage, 2),
                    "random_factor": round(random_factor, 4),
                    "raw_damage": round(raw_damage, 2),
                    "shield_damage": round(shield_damage, 2),
                    "hull_damage": round(hull_damage, 2),
                    "target_shield_after": round(target["shield"], 2),
                    "target_hull_after": round(target["hull"], 2),
                    "engagement_distance": round(distance, 2),
                    "range_efficiency": round(range_efficiency, 4),
                    "scan_deficit": round(scan_deficit, 2),
                    "stealth_evasion_factor": round(stealth_evasion_factor, 4),
                    "weighted_resistance": round(weighted_resistance, 4),
                    "penetration": round(penetration, 4),
                    "effective_resistance": round(effective_resistance, 4),
                    "mitigation_factor": round(mitigation_factor, 4),
                    "actor_energy_after": round(actor["energy"], 2),
                    "actor_heat_ratio": round(heat_ratio, 3),
                    "tactical_action": actor_fx["action"],
                    "tactical_magnitude": round(float(actor_fx["magnitude"]), 3),
                    "target_tactical_action": target_fx["action"],
                    "note": note,
                }
            )
            seq += 1

        for round_index in range(1, max_rounds + 1):
            rounds_fought = round_index
            for side in ("attacker", "defender"):
                fx = _tactical_effect(side, round_index)
                if fx["action"] == "none":
                    continue
                log_entries.append(
                    {
                        "seq": seq,
                        "round": round_index,
                        "phase": f"{side}_command",
                        "actor": participants[side]["name"],
                        "tactical_action": fx["action"],
                        "tactical_magnitude": round(float(fx["magnitude"]), 3),
                    }
                )
                seq += 1
            _movement_step(round_index)
            strike("attacker", "defender", round_index)
            if participants["defender"]["hull"] <= 0:
                break
            if counterfire_enabled:
                strike("defender", "attacker", round_index)
                if participants["attacker"]["hull"] <= 0:
                    break

            for side in ("attacker", "defender"):
                p = participants[side]
                p["heat"] = max(0.0, p["heat"] - p["cooling_per_round"])
                # Small passive shield recovery when not broken
                if p["shield"] > 0:
                    p["shield"] = min(p["shield_max"], p["shield"] + (0.015 * p["shield_max"]))

        attacker_alive = participants["attacker"]["hull"] > 0
        defender_alive = participants["defender"]["hull"] > 0
        if attacker_alive and not defender_alive:
            winner = "attacker"
        elif defender_alive and not attacker_alive:
            winner = "defender"
        elif participants["attacker"]["damage_dealt"] > participants["defender"]["damage_dealt"]:
            winner = "attacker"
        elif participants["defender"]["damage_dealt"] > participants["attacker"]["damage_dealt"]:
            winner = "defender"
        else:
            winner = "draw"

        return {
            "battle_id": payload["battle_id"],
            "formula_version": "v1_gl_reciprocal",
            "seed": payload["context"]["seed"],
            "mode": mode,
            "winner": winner,
            "rounds_fought": rounds_fought,
            "damage_totals": {
                "attacker_to_defender": round(participants["attacker"]["damage_dealt"], 2),
                "defender_to_attacker": round(participants["defender"]["damage_dealt"], 2),
            },
            "energy_used": {
                "attacker": round(participants["attacker"]["energy_used"], 2),
                "defender": round(participants["defender"]["energy_used"], 2),
            },
            "remaining": {
                "attacker": {
                    "hull": round(participants["attacker"]["hull"], 2),
                    "shield": round(participants["attacker"]["shield"], 2),
                    "energy": round(participants["attacker"]["energy"], 2),
                },
                "defender": {
                    "hull": round(participants["defender"]["hull"], 2),
                    "shield": round(participants["defender"]["shield"], 2),
                    "energy": round(participants["defender"]["energy"], 2),
                },
            },
            "post_battle_log": log_entries,
            "summary": {
                "attacker_disabled": participants["attacker"]["hull"] <= 0,
                "defender_disabled": participants["defender"]["hull"] <= 0,
                "counterfire_enabled": counterfire_enabled,
                "avg_engagement_distance": round(
                    distance_sum / max(1.0, strike_count),
                    2,
                ),
                "tactical_commands_applied": {
                    "attacker": len(commands_by_round["attacker"]),
                    "defender": len(commands_by_round["defender"]),
                },
            },
        }


def resolve_default_seed_dir() -> Path:
    server_dir = Path(__file__).resolve().parent
    project_root = server_dir.parents[1]
    return project_root / "Data" / "Seeds"


def resolve_default_state_db() -> Path:
    server_dir = Path(__file__).resolve().parent
    project_root = server_dir.parents[1]
    return project_root / "Data" / "state" / "spaceshift_state.sqlite3"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SpaceShift stdlib mock backend")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument(
        "--seed-dir",
        type=Path,
        default=resolve_default_seed_dir(),
        help="Directory containing seed JSON files",
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=resolve_default_state_db(),
        help="SQLite file for persistent state (used when SPACESHIFT_DB_BACKEND=sqlite)",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
    args = parse_args()
    db_backend = env_casefold_choice(
        "SPACESHIFT_DB_BACKEND",
        default=DEFAULT_DB_BACKEND,
        allowed_values=DB_BACKEND_VALUES,
    )
    try:
        MockServerHandler.validate_auth_configuration()
    except ValueError as exc:
        print(f"Invalid auth configuration: {exc}")
        return 2

    if not (1 <= args.port <= 65535):
        print("Port must be in range 1-65535")
        return 2

    try:
        store = SeedStore.load(args.seed_dir)
    except SeedDataError as exc:
        print(f"Failed to load seed data: {exc}")
        return 1

    try:
        state = PersistentState(args.state_db)
    except OSError as exc:
        print(f"Failed to initialize persistent state database ({db_backend}): {exc}")
        return 1

    MockServerHandler.seed_store = store
    MockServerHandler.state_store = state
    server = ThreadingHTTPServer((args.host, args.port), MockServerHandler)
    logging.info("Serving SpaceShift mock backend on http://%s:%s", args.host, args.port)
    logging.info("Seed directory: %s", args.seed_dir)
    logging.info("State backend: %s", state.db_backend)
    logging.info("Auth mode: %s", MockServerHandler.auth_mode)
    if state.db_backend == "sqlite":
        logging.info("State DB: %s", args.state_db)
    else:
        logging.info(
            "Postgres DSN source: SPACESHIFT_POSTGRES_DSN (value hidden)"
        )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutdown requested, stopping server")
    finally:
        server.server_close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
