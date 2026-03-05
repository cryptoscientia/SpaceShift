# SpaceShift Mock Server

Minimal Python stdlib backend that serves SpaceShift seed data from `SpaceShiftGame/Data/Seeds`.

## Requirements

- Python 3.10+ (3.11+ recommended)
- SQLite backend: no external dependencies
- Postgres backend: install `psycopg` (`pip install -r SpaceShiftGame/Code/server/requirements.txt`)

## Run

From the repository root:

```bash
python3 SpaceShiftGame/Code/server/mock_server.py
```

Default bind address is `127.0.0.1:8000`.

Optional flags:

```bash
python3 SpaceShiftGame/Code/server/mock_server.py --host 0.0.0.0 --port 8080
python3 SpaceShiftGame/Code/server/mock_server.py --seed-dir SpaceShiftGame/Data/Seeds
python3 SpaceShiftGame/Code/server/mock_server.py --state-db SpaceShiftGame/Data/state/spaceshift_state.sqlite3
```

## Database Backend Switch (Runtime)

Backend is selected with environment variables:

- `SPACESHIFT_DB_BACKEND=sqlite|postgres` (default: `sqlite`)
- `SPACESHIFT_POSTGRES_DSN` (required when backend is `postgres`)

SQLite mode example (default fallback):

```bash
SPACESHIFT_DB_BACKEND=sqlite \
python3 SpaceShiftGame/Code/server/mock_server.py \
  --state-db SpaceShiftGame/Data/state/spaceshift_state.sqlite3
```

Postgres mode example:

```bash
SPACESHIFT_DB_BACKEND=postgres \
SPACESHIFT_POSTGRES_DSN='postgresql://spaceshift:spaceshift@127.0.0.1:5432/spaceshift' \
python3 SpaceShiftGame/Code/server/mock_server.py --host 0.0.0.0 --port 8000
```

Postgres runtime behavior:

- connection uses `psycopg` with UTC session timezone.
- startup schema bootstrap runs on Postgres using the same runtime table set.
- connect attempts retry `3` times with short backoff (`2s` timeout per attempt).
- if DSN is missing/unreachable, startup exits with a clear init error instead of hanging.
- `--state-db` is ignored while `SPACESHIFT_DB_BACKEND=postgres`.

## SQLite Runtime Hardening (Default Backend)

The mock server remains SQLite-by-default, but now applies production-safe concurrency settings on every connection:

- `journal_mode=WAL`
- `busy_timeout=12000ms` (minimum enforced: `1000ms`)
- `synchronous=NORMAL`
- `wal_autocheckpoint=1000`
- transaction mode: `IMMEDIATE` (reduces lock-escalation races under concurrent writers)

Optional tuning environment variables:

- `SPACESHIFT_SQLITE_BUSY_TIMEOUT_MS` (default `12000`)
- `SPACESHIFT_SQLITE_SYNCHRONOUS` (`OFF|NORMAL|FULL|EXTRA`, default `NORMAL`)
- `SPACESHIFT_SQLITE_WAL_AUTOCHECKPOINT_PAGES` (default `1000`)
- `SPACESHIFT_SQLITE_JOURNAL_MODE` (`WAL` by default; other SQLite modes supported for troubleshooting)

## Authentication Modes

The backend supports two auth modes:

- `SPACESHIFT_AUTH_MODE=local|jwt` (default: `local`)

`local` mode (default):

- keeps existing in-process bearer session behavior.
- supports dev logins (`/api/player/login`, `/api/admin/login`) when their env toggles are enabled.
- session TTL is controlled by `SPACESHIFT_SESSION_TTL_SECONDS`.

`jwt` mode (IdP-backed bearer verification):

- bearer tokens are verified on every authenticated request.
- required env:
  - `SPACESHIFT_JWT_ISSUER`
  - `SPACESHIFT_JWT_AUDIENCE`
  - `SPACESHIFT_JWT_ALGORITHMS` (default `RS256`, comma-separated)
  - signing key source:
    - `SPACESHIFT_JWT_JWKS_URL` (required when `RS256` is enabled)
    - `SPACESHIFT_JWT_HS256_SECRET` (required when `HS256` is enabled, useful for deterministic local tests)
- validated claims: `exp`, `nbf`, `iss`, `aud`.
- token subject (`sub`) is mapped to a stable internal player key (`player.idp.<sha256-prefix>`).
- invalid/expired/mismatched tokens return HTTP `401` with clear auth error messages.

JWT RS256 example (hosted, secure default):

```bash
SPACESHIFT_AUTH_MODE=jwt \
SPACESHIFT_AUTH_REQUIRED=true \
SPACESHIFT_JWT_ISSUER='https://idp.example.com/' \
SPACESHIFT_JWT_AUDIENCE='spaceshift-api' \
SPACESHIFT_JWT_ALGORITHMS='RS256' \
SPACESHIFT_JWT_JWKS_URL='https://idp.example.com/.well-known/jwks.json' \
SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN=false \
SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN=false \
SPACESHIFT_ENABLE_ADMIN_GOD_MODE=false \
python3 SpaceShiftGame/Code/server/mock_server.py --host 0.0.0.0 --port ${PORT:-8000}
```

JWT HS256 example (deterministic local test mode):

```bash
SPACESHIFT_AUTH_MODE=jwt \
SPACESHIFT_AUTH_REQUIRED=true \
SPACESHIFT_JWT_ISSUER='https://auth.spaceshift.local' \
SPACESHIFT_JWT_AUDIENCE='spaceshift-local' \
SPACESHIFT_JWT_ALGORITHMS='HS256' \
SPACESHIFT_JWT_HS256_SECRET='replace-with-strong-secret' \
python3 SpaceShiftGame/Code/server/mock_server.py
```

## Web Deployment Environment

For website play, set these environment variables on your hosted backend:

- `SPACESHIFT_ALLOWED_ORIGINS`:
  - comma-separated web origins, for example:
  - `https://spaceshift.pages.dev,https://spaceshift.example.com`
  - use `*` only for temporary public testing.
- `SPACESHIFT_AUTH_REQUIRED=true`
- `SPACESHIFT_AUTH_MODE=jwt`
- `SPACESHIFT_JWT_ISSUER=<your-idp-issuer>`
- `SPACESHIFT_JWT_AUDIENCE=<your-api-audience>`
- `SPACESHIFT_JWT_ALGORITHMS=RS256`
- `SPACESHIFT_JWT_JWKS_URL=<your-idp-jwks-url>`
- `SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN=false` (or keep `true` for demo/testing)
- `SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN=false`
- `SPACESHIFT_ENABLE_ADMIN_GOD_MODE=false`
- `SPACESHIFT_SESSION_TTL_SECONDS=86400` (set `0` to disable expiry)

Hosted startup command example:

```bash
python3 SpaceShiftGame/Code/server/mock_server.py --host 0.0.0.0 --port ${PORT:-8000}
```

## Smoke Tests

Run the lightweight, stdlib-only endpoint smoke tests from the repository root:

```bash
python3 SpaceShiftGame/Code/server/smoke_test.py
```

What this covers:
- `GET /health`
- `POST /api/player/login` (dev login: `player` / `player`)
- managed run also includes a separate JWT auth smoke pass (`SPACESHIFT_AUTH_MODE=jwt`, HS256 deterministic token path, no external JWKS/network dependency).
- `GET /api/missions` (including AI mission ids such as `mission.ai_*`)
- `GET /api/elements` (full 118 + descriptive metadata fields)
- `GET /api/materials` (+ category filter)
- `GET /api/crafting/substitutions` (+ `item_id` filter)
- `POST /api/crafting/quote` with `substitution_id`
- `GET /api/research/compute` + `GET /api/research/jobs`
- `GET /api/research/tracks` (future constellation-style objective lanes)
- `GET /api/factions` + `GET /api/factions/status`
- `GET /api/legions` + `GET /api/legions/detail|members|requests|governance|events|me`
- `POST /api/factions/align|leave`
- `POST /api/legions/create|join|leave|requests/respond|members/role`
- `POST /api/legions/governance/propose|vote|finalize`
- `GET /api/combat/contacts` + `POST /api/combat/engage` (`flee`)
- `GET /api/fairplay/policy`
- `GET /api/economy/wallet` + `GET /api/economy/inventory`
- `GET /api/inventory/storage` + `POST /api/inventory/storage/upgrade`
- `GET /api/research/unlocks` + `GET /api/fleet/status`
- `GET /api/market/regions` + `GET /api/market/snapshot` + `GET /api/market/listings`
- `GET /api/market/policy` + `GET /api/market/history`
- `POST /api/market/exchange` + `POST /api/market/buy` + `POST /api/market/sell`
- `POST /api/crafting/build` + `GET /api/assets` + `GET /api/assets/instances` + `GET /api/assets/smuggled`
- `POST /api/assets/smuggle/move` + `POST /api/inventory/trash`
- `GET /api/discovery/scan` + `POST /api/worlds/claim` + `GET /api/worlds/owned` + `GET /api/worlds/detail` + `POST /api/worlds/harvest`
- `GET /api/consumables` + `GET /api/contracts/board` + `GET /api/contracts/jobs`
- `GET /api/manufacturing/jobs` + `GET /api/reverse-engineering/jobs`
- `GET /api/admin/players` + `GET /api/admin/actions`
- Advanced POST flows:
  - `POST /api/fitting/simulate`
  - `POST /api/assets/instances/level-up`
  - `POST /api/admin/players/moderate`
  - `POST /api/admin/crafting/jackpot`
  - `POST /api/market/listings/create|buy|cancel`
  - `POST /api/manufacturing/start|cancel`
  - `POST /api/contracts/accept|complete|abandon`

To run against an already-running server instead of launching a temporary local instance:

```bash
python3 SpaceShiftGame/Code/server/smoke_test.py --base-url http://127.0.0.1:8000
```

When running against an existing server, the smoke flow creates a run-scoped player id
(`player.smoke.<time_ns>`) via `POST /api/profile/save`.

## Postgres Migration Bundle (Incremental Path)

Generate a Postgres-ready migration bundle from current SQLite runtime data:

```bash
python3 SpaceShiftGame/Code/server/sqlite_to_postgres_bundle.py \
  --sqlite-db SpaceShiftGame/Data/state/spaceshift_state.sqlite3 \
  --output-dir SpaceShiftGame/Reports/postgres_migration_bundle_2026-03-04
```

Bundle outputs:

- `manifest.json` (tables, columns, row counts, dependency-informed load order)
- `schema_sqlite.sql` (raw SQLite DDL snapshot)
- `schema_postgres.sql` (best-effort converted DDL for Postgres)
- `load_postgres.sql` (`psql` script that applies schema + loads CSV exports)
- `data/*.csv` (table data exports)

Load into Postgres (from the bundle directory):

```bash
cd SpaceShiftGame/Reports/postgres_migration_bundle_2026-03-04
psql "$POSTGRES_DSN" -f load_postgres.sql
```

Notes:

- Review `schema_postgres.sql` before production use, especially numeric type choices.
- Empty tables are omitted from CSV/loader by default; add `--include-empty-tables` to emit all tables.
- Runtime server behavior is unchanged: SQLite remains the active datastore until explicit backend cutover.

## Simulation Suite

Run the reproducible simulation/reporting harness:

```bash
python3 SpaceShiftGame/Code/server/run_simulation_suite.py --tag 2026-03-02_celestial_r14 --seed 20260302
```

Long-run profile (larger Monte Carlo/sample sweeps):

```bash
python3 SpaceShiftGame/Code/server/run_simulation_suite.py --profile long --tag 2026-03-02_celestial_long_r14 --seed 20260302
```

Note: long profile now applies a hardened HTTP retry/backoff policy and moderated burst factors for better end-to-end stability under heavy simulation load.

Run regression checks against the latest simulation output:

```bash
python3 SpaceShiftGame/Code/server/check_simulation_regression.py
```

## CI Gate (Local + GitHub Actions)

Run the backend CI gate locally from the repository root:

```bash
bash SpaceShiftGame/Code/server/run_ci_gate.sh
```

This gate runs, in order:
- `py_compile` on core server scripts (`mock_server.py`, `smoke_test.py`, `run_simulation_suite.py`, `check_simulation_regression.py`, `check_endurance_regression.py`, `simpy_timeflow.py`)
- `smoke_test.py`
- `run_simulation_suite.py --profile standard`
- `check_simulation_regression.py`

Deterministic defaults used by the gate:
- `PYTHONHASHSEED=0`
- `TZ=UTC`
- `SPACESHIFT_DETERMINISTIC=1`
- `SIM_PROFILE=standard`
- `SIM_SEED=20260304`
- `SIM_TAG=ci_standard`

Optional overrides:

```bash
SIM_SEED=20260305 SIM_TAG=ci_standard_seed_20260305 bash SpaceShiftGame/Code/server/run_ci_gate.sh
PYTHON_BIN=python3.12 bash SpaceShiftGame/Code/server/run_ci_gate.sh
```

Run the endurance/soak gate locally (multi-seed + aggregate):

```bash
bash SpaceShiftGame/Code/server/run_ci_endurance_gate.sh
```

Endurance defaults:
- `SIM_PROFILE=long`
- `SIM_SEEDS=20260304,20260305` (must include at least 2 seeds)
- `SIM_TAG_PREFIX=ci_endurance`
- `ENDURANCE_THRESHOLDS=SpaceShiftGame/Reports/endurance_thresholds_v1.json`

Endurance overrides:

```bash
SIM_PROFILE=standard SIM_SEEDS=20260304,20260305 bash SpaceShiftGame/Code/server/run_ci_endurance_gate.sh
SIM_PROFILE=long SIM_SEEDS=20260304,20260305,20260306 SIM_TAG_PREFIX=ci_endurance_manual bash SpaceShiftGame/Code/server/run_ci_endurance_gate.sh
```

Run endurance aggregate regression checks directly:

```bash
python3 SpaceShiftGame/Code/server/check_endurance_regression.py \
  --report SpaceShiftGame/Reports/latest_endurance_standard.json \
  --thresholds SpaceShiftGame/Reports/endurance_thresholds_v1.json \
  --output SpaceShiftGame/Reports/endurance_regression_latest_standard.json
```

GitHub Actions workflows:
- `.github/workflows/backend-ci-gate.yml`
  - runs on `push` + `pull_request`
  - matrix: `ubuntu-latest` + `macos-latest`, Python `3.11` + `3.12`
  - uploads simulation/regression artifacts per matrix leg
- `.github/workflows/backend-endurance-gate.yml`
  - runs on daily schedule + manual dispatch
  - profile matrix: `standard` + `long`
  - default per-profile seed sets: `20260304,20260305,20260306`
  - executes multi-seed endurance gate, endurance aggregate regression checks, and uploads aggregate/regression artifacts
- `.github/workflows/backend-alert-smoke.yml`
  - manual webhook health check for alert channel verification
- optional failure alert secret for both workflows:
  - `SPACESHIFT_CI_ALERT_WEBHOOK` (JSON webhook endpoint; used only on workflow failure)
- alert operations runbook:
  - `SpaceShiftGame/Design/ci_alert_ops_runbook_2026-03-04.md`

Run standalone SimPy scenario sweeps (multi-seed, multi-scenario queue/market/extraction analysis):

```bash
python3 SpaceShiftGame/Code/server/run_simpy_sweep.py \
  --profiles standard long \
  --runs-per-scenario 12 \
  --seed-base 20260304 \
  --tag 2026-03-04_full_sweep_r1
```

Regenerate element-cost coverage (all 118 elements used by gameplay cost pipelines):

```bash
python3 SpaceShiftGame/Data/Tools/generate_element_cost_coverage.py \
  --output SpaceShiftGame/Reports/element_cost_coverage_2026-03-02.json
```

Aggregate multiple runs into one endurance summary:

```bash
python3 SpaceShiftGame/Code/server/aggregate_simulation_endurance.py \
  --label 2026-03-02_long_endurance \
  --inputs \
    SpaceShiftGame/Reports/simulation_suite_2026-03-02_long.json \
    SpaceShiftGame/Reports/simulation_suite_2026-03-02_long_s2.json \
    SpaceShiftGame/Reports/simulation_suite_2026-03-02_long_s3.json \
  --output-json SpaceShiftGame/Reports/simulation_endurance_summary_2026-03-02.json \
  --output-md SpaceShiftGame/Reports/simulation_endurance_summary_2026-03-02.md
```

Artifacts are written to `SpaceShiftGame/Reports/`:
- `simulation_suite_<tag>.json`
- `simulation_suite_<tag>.md`
- `latest_simulation_report.json` (synced to latest run)
- `simulation_regression_latest.json` (regression threshold result)
- `simulation_regression_<tag>.json` (per-run regression output)
- `simulation_endurance_summary_<date>.json|md` (cross-run aggregate)
- `endurance_regression_<tag>.json` (per-run endurance aggregate regression output)
- `latest_endurance_<profile>.json|md` (latest endurance aggregate by profile)
- `endurance_regression_latest_<profile>.json` (latest endurance regression by profile)
- `simpy_sweep_<tag>.json|md` (standalone SimPy multi-scenario aggregate)

## Endpoints

- `GET /health`
- `GET /api/missions?limit=20`
- `GET /api/missions/jobs?player_id=player.commander&status=active`
- `GET /api/modules?family=weapon_ballistic`
- `GET /api/tech-tree?branch=planetary_construction&limit=20`
- `GET /api/races`
- `GET /api/factions`
- `GET /api/factions/status?player_id=player.commander`
- `GET /api/professions`
- `GET /api/legions?limit=20&faction_id=faction.aurelian_compact`
- `GET /api/legions/detail?legion_id=legion.abc123&player_id=player.commander`
- `GET /api/legions/members?legion_id=legion.abc123&status=active&limit=20`
- `GET /api/legions/requests?player_id=player.commander&legion_id=legion.abc123&status=pending&limit=20`
- `GET /api/legions/governance?legion_id=legion.abc123&status=open&limit=20`
- `GET /api/legions/events?legion_id=legion.abc123&limit=20`
- `GET /api/legions/me?player_id=player.commander`
- `GET /api/abilities`
- `GET /api/artifacts`
- `GET /api/blueprints`
- `GET /api/events`
- `GET /api/planet-types`
- `GET /api/starter-ships`
- `GET /api/elements?limit=200`
- `GET /api/materials?limit=40&category=metamaterial`
- `GET /api/crafting/substitutions?item_id=module.special_cognitive_battle_core_mk6&limit=20`
- `GET /api/celestial-templates?body_class=asteroid`
- `GET /api/structures?domain=planet`
- `GET /api/lore?limit=12`
- `GET /api/profile?player_id=player.commander`
- `GET /api/profile/memory?player_id=player.commander`
- `GET /api/economy/wallet?player_id=player.commander`
- `GET /api/economy/inventory?player_id=player.commander&limit=20`
- `GET /api/inventory/storage?player_id=player.commander`
- `GET /api/fairplay/policy`
- `GET /api/covert/policy?player_id=player.commander`
- `GET /api/covert/cooldowns?player_id=player.commander`
- `GET /api/covert/logs?player_id=player.commander&perspective=both&limit=20`
- `GET /api/research/unlocks?player_id=player.commander`
- `GET /api/research/tracks?player_id=player.commander&limit=20`
- `GET /api/research/compute?player_id=player.commander`
- `GET /api/research/jobs?player_id=player.commander&status=active&limit=20`
- `GET /api/manufacturing/jobs?player_id=player.commander&status=active&limit=20`
- `GET /api/reverse-engineering/jobs?player_id=player.commander&status=active&limit=20`
- `GET /api/assets?player_id=player.commander&asset_type=module&limit=20`
- `GET /api/assets/instances?player_id=player.commander&asset_type=module&limit=20`
- `GET /api/assets/smuggled?player_id=player.commander&asset_type=module&limit=20`
- `GET /api/market/snapshot?player_id=player.commander&limit=20`
- `GET /api/market/listings?limit=20&status=active`
- `GET /api/market/history?limit=20&asset_type=element&asset_id=Fe&currency=credits`
- `GET /api/market/policy`
- `GET /api/market/regions`
- `GET /api/consumables?limit=20`
- `GET /api/contracts/board?limit=20`
- `GET /api/contracts/jobs?player_id=player.commander&status=active&limit=20`
- `GET /api/fleet/status?player_id=player.commander`
- `GET /api/energy?player_id=player.commander`
- `GET /api/combat/contacts?player_id=player.commander&count=6`
- `GET /api/discovery/scan?player_id=player.commander&body_class=asteroid&count=6&scan_power=130`
- `GET /api/discovery/catalog?player_id=player.commander&limit=40`
- `GET /api/worlds/owned?player_id=player.commander`
- `GET /api/worlds/detail?player_id=player.commander&world_id=world.abc123`
- `GET /api/admin/players?player_id=admin&limit=20`
- `GET /api/admin/actions?player_id=admin&limit=20`
- `GET /api/manifest`
- `POST /api/combat/simulate`
- `POST /api/combat/odds`
- `POST /api/combat/engage`
- `POST /api/combat/auto-resolve`
- `POST /api/research/start`
- `POST /api/research/claim`
- `POST /api/admin/login`
- `POST /api/admin/players/moderate`
- `POST /api/admin/crafting/jackpot`
- `POST /api/covert/steal`
- `POST /api/covert/sabotage`
- `POST /api/covert/hack`
- `POST /api/player/login`
- `POST /api/worlds/project-structure`
- `POST /api/profile/save`
- `POST /api/profile/memory`
- `POST /api/worlds/claim`
- `POST /api/worlds/build-structure`
- `POST /api/worlds/harvest`
- `POST /api/factions/align`
- `POST /api/factions/leave`
- `POST /api/legions/create`
- `POST /api/legions/join`
- `POST /api/legions/requests/respond`
- `POST /api/legions/leave`
- `POST /api/legions/members/role`
- `POST /api/legions/governance/propose`
- `POST /api/legions/governance/vote`
- `POST /api/legions/governance/finalize`
- `POST /api/market/buy`
- `POST /api/market/sell`
- `POST /api/market/exchange`
- `POST /api/market/listings/create`
- `POST /api/market/listings/cancel`
- `POST /api/market/listings/buy`
- `POST /api/crafting/build`
- `POST /api/inventory/storage/upgrade`
- `POST /api/assets/smuggle/move`
- `POST /api/inventory/trash`
- `POST /api/assets/instances/level-up`
- `POST /api/fitting/simulate`
- `POST /api/manufacturing/start`
- `POST /api/manufacturing/claim`
- `POST /api/manufacturing/cancel`
- `POST /api/reverse-engineering/start`
- `POST /api/reverse-engineering/claim`
- `POST /api/contracts/accept`
- `POST /api/contracts/complete`
- `POST /api/contracts/abandon`
- `POST /api/missions/accept`
- `POST /api/missions/progress`
- `POST /api/missions/claim`

## Crafting Substitutions

Crafting and research quotes/builds accept an optional `substitution_id`:

```json
{
  "player_id": "player.commander",
  "item_id": "module.special_cognitive_battle_core_mk6",
  "quantity": 1,
  "substitution_id": "sub.cognitive_core_palladium_saver"
}
```

Use `GET /api/crafting/substitutions` to discover available alternatives per item.

## Item Levels

Crafted module/hull instances start at `item_level: 1` and can be upgraded:

```json
{
  "player_id": "player.commander",
  "instance_id": "inst.1234abcd",
  "levels": 3
}
```

via `POST /api/assets/instances/level-up`.

`POST /api/fitting/simulate` supports mixed-level loadouts on the same hull:

```json
{
  "hull_id": "hull.settler_scout",
  "hull_level": 6,
  "modules": [
    { "id": "module.armor_titanium_plating_mk1", "quantity": 1, "level": 2 },
    { "id": "module.weapon_laser_bank_mk1", "quantity": 1, "level": 9 },
    { "id": "module.reactor_fission_core_mk1", "quantity": 1, "level": 5 }
  ]
}
```

## Behavior

- Returns JSON for all responses.
- Includes CORS headers on all responses.
- Supports `OPTIONS` preflight.
- Validates query parameters and returns JSON `400` for invalid input.
- Returns JSON `404` for unknown routes.
- Loads seed files once at startup and fails fast with a clear message if data is missing/invalid.

## Combat Simulation

`POST /api/combat/simulate` accepts a minimal payload:

```json
{
  "attacker": {
    "name": "Hansolo",
    "stats": {
      "attack": 220,
      "defense": 190,
      "hull": 640,
      "shield": 280,
      "energy": 420,
      "scan": 90,
      "cloak": 40
    }
  },
  "defender": {
    "name": "Raid Entity",
    "stats": {
      "attack": 180,
      "defense": 210,
      "hull": 700,
      "shield": 320,
      "energy": 390,
      "scan": 55,
      "cloak": 60
    }
  },
  "context": {
    "mode": "pvp",
    "max_rounds": 8,
    "seed": 42,
    "counterfire_enabled": true
  }
}
```

Response includes deterministic simulation output:
- `winner`
- `rounds_fought`
- `damage_totals`
- `energy_used`
- `remaining`
- `post_battle_log[]` (event-by-event combat log suitable for battle report UI)

Tactical command actions supported in `context.tactical_commands`:
- `main_ability`
- `boost_thrust`
- `evade`
- `stealth_burst` (high-cloak burst for evasion/ambush windows)

`POST /api/combat/engage` also returns engagement-balance metadata:
- `engagement_balance.risk_profile` (relative combat levels, threat ratio, projected win probability)
- `engagement_balance.reward_scaling` (anti-gank penalty and underdog bonus scaler)
- `victory_rewards` on successful wins (credits, salvage, optional underdog voidcoin)
- `player_stats_source` / `player_loadout` (attacker stat authority metadata)

For authenticated combat requests, attacker stats are resolved from persisted player loadout
when available (profile memory loadout + fleet active hull + owned module assets/crafted instances),
with automatic fallback to legacy inventory-based combat projection when no persisted selection exists.

`POST /api/combat/odds` accepts optional `player_id`; when present, attacker stats are overridden
by the same authoritative persisted-loadout resolver and the response includes:
- `attacker_source`
- `attacker_loadout`

## Covert Ops Runtime

Covert operations are live via:
- `POST /api/covert/steal`
- `POST /api/covert/sabotage`
- `POST /api/covert/hack`

Each operation:
- consumes action-energy
- runs success + detection probability rolls
- applies an operation cooldown (`GET /api/covert/cooldowns`)
- writes persistent operation logs (`GET /api/covert/logs`)

Fair-play protections apply:
- high-level attackers against significantly weaker targets (without target high-risk opt-in)
  are blocked from meaningful gains/effects.

## Discovery + Mining Projection

`GET /api/discovery/scan` returns discoverable worlds from scientifically inspired templates
(asteroids, comets, moons, planets, gas giants, stars) with element lode estimates.
When `player_id` is provided, scan responses include progression-aware metadata:
- `effective_scan_power`
- `target_difficulty`
- `discovery_profile`
- per-world `detection_confidence`, `rarity_score`, `environment_hazard`, `hidden_signature`, and `traits`

Core action endpoints consume player action-energy when `player_id` is provided:
- discovery scan
- combat engage / auto-resolve
- world harvest

Use `GET /api/energy?player_id=...` to inspect current action-energy state.

Example:

```bash
curl 'http://127.0.0.1:8000/api/discovery/scan?player_id=player.commander&body_class=asteroid&count=4&scan_power=130'
```

`POST /api/worlds/project-structure` estimates hourly extraction after applying structures:

```json
{
  "world": {
    "world_id": "world.abc123",
    "body_class": "asteroid",
    "element_lodes": [
      { "symbol": "Fe", "estimated_units": 2200, "atomic_number": 26 },
      { "symbol": "Ni", "estimated_units": 450, "atomic_number": 28 }
    ]
  },
  "structure_ids": [
    "structure.asteroid_mass_driver_rig",
    "structure.orbital_logistics_node"
  ]
}
```

Response includes:
- selected structures
- cumulative modifiers
- upkeep per hour
- top output materials and projected hourly units

## Persistent Prototype State

The server now uses SQLite for persistent prototype state:
- profile save/load
- claimed worlds
- built structures on worlds

Default DB location:
- `SpaceShiftGame/Data/state/spaceshift_state.sqlite3`
