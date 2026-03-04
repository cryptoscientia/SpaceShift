# SpaceShiftGame

This folder contains active SpaceShift implementation files.

## Structure
- `Assets/Concepts/` - UI and flow concept images used as build references.
- `Data/` - data-driven content system (schemas, seed content, source datasets, tools).
- `Design/` - implementation docs (blueprint, architecture, roadmap, combat math).
- `Code/` - game/client/server code.
  - `Code/client/` - web-first browser client (static deployment ready) wired to mock APIs.
  - `Code/server/` - Python stdlib mock backend (includes combat simulation endpoint).
  - `Code/mobile/react-native/` - Expo/React Native scaffold.
  - `Code/mobile/unity/` - Unity scaffold with API client scripts.

## Web-First Quick Start
- Start backend:
  - `python3 SpaceShiftGame/Code/server/mock_server.py`
- Optional backend selection:
  - SQLite (default): `SPACESHIFT_DB_BACKEND=sqlite`
  - Postgres: `SPACESHIFT_DB_BACKEND=postgres` + `SPACESHIFT_POSTGRES_DSN='postgresql://user:pass@host:5432/dbname'`
- Optional auth selection:
  - Local session mode (default): `SPACESHIFT_AUTH_MODE=local`
  - IdP JWT mode: `SPACESHIFT_AUTH_MODE=jwt` + `SPACESHIFT_JWT_ISSUER` + `SPACESHIFT_JWT_AUDIENCE` + signing source (`SPACESHIFT_JWT_JWKS_URL` for RS256 or `SPACESHIFT_JWT_HS256_SECRET` for HS256 tests)
- Start web client:
  - `python3 -m http.server 8081 --directory SpaceShiftGame/Code/client`
- Open:
  - `http://127.0.0.1:8081`

For hosted web deploy guidance, see:
- `SpaceShiftGame/Code/client/README.md`
- `SpaceShiftGame/Code/server/README.md`
- `SpaceShiftGame/Deploy/README.md`

## Implemented Prototype Loops
- Mobile onboarding flow (signin/name/race/profession/planet/summary).
- Discovery scan loop for asteroids/comets/moons/planets/gas giants/stars with progression-aware scan scaling.
- Structure projection loop for element-driven mining output.
- Persistent prototype state for profile + claimed worlds + built structures (runtime switch: SQLite default, Postgres supported).
- Ship combat simulation with post-battle logs.
- Tech tree snapshot and lore codex snapshot.
- Action-energy loop for scans/combat/harvest with module-driven max/regen bonuses.
- Discovery catalog persistence + guaranteed homeworld bootstrap.
- Storage and logistics loop: personal inventory slots + hidden smuggle slots with upgrade flow.
- Inventory disposal loop with confirmation warnings for high-value assets.
- Admin operations loop: player roster visibility, moderation kick/clear controls, and forced jackpot craft testing flow.
- Covert operations loop: steal/sabotage/hack runtime with cooldowns, detection risk, and fair-play guardrails.

## Data Validation
- Run content validation from repository root:
  - `python3 SpaceShiftGame/Data/Tools/validate_content.py`
- Run backend smoke checks:
  - `python3 SpaceShiftGame/Code/server/smoke_test.py`
- Run simulation suite + report export:
  - `python3 SpaceShiftGame/Code/server/run_simulation_suite.py --tag 2026-03-02_celestial_r14 --seed 20260302`
  - `python3 SpaceShiftGame/Code/server/run_simulation_suite.py --profile long --tag 2026-03-02_celestial_long_r14 --seed 20260302`
- Run regression-threshold checks on latest simulation output:
  - `python3 SpaceShiftGame/Code/server/check_simulation_regression.py`
- Run the full backend CI gate locally (same sequence used in GitHub Actions):
  - `bash SpaceShiftGame/Code/server/run_ci_gate.sh`
- Run the backend endurance/soak gate locally (multi-seed aggregate):
  - `bash SpaceShiftGame/Code/server/run_ci_endurance_gate.sh`
- Run endurance aggregate regression checks:
  - `python3 SpaceShiftGame/Code/server/check_endurance_regression.py --report SpaceShiftGame/Reports/latest_endurance_standard.json --thresholds SpaceShiftGame/Reports/endurance_thresholds_v1.json --output SpaceShiftGame/Reports/endurance_regression_latest_standard.json`
- CI workflow entrypoint:
  - `.github/workflows/backend-ci-gate.yml`
  - `.github/workflows/backend-endurance-gate.yml`
  - `.github/workflows/backend-alert-smoke.yml`
- Aggregate multiple long runs into one endurance summary:
  - `python3 SpaceShiftGame/Code/server/aggregate_simulation_endurance.py --label 2026-03-02_long_endurance --inputs SpaceShiftGame/Reports/simulation_suite_2026-03-02_long.json SpaceShiftGame/Reports/simulation_suite_2026-03-02_long_s2.json SpaceShiftGame/Reports/simulation_suite_2026-03-02_long_s3.json --output-json SpaceShiftGame/Reports/simulation_endurance_summary_2026-03-02.json --output-md SpaceShiftGame/Reports/simulation_endurance_summary_2026-03-02.md`
- Optional GitHub Actions failure webhook secret: `SPACESHIFT_CI_ALERT_WEBHOOK`
- GitHub hardening helpers (run in your actual GitHub repo context):
  - `bash SpaceShiftGame/Deploy/github_apply_branch_protection.sh`
  - `python3 SpaceShiftGame/Deploy/github_set_actions_secret.py`
- Alert ops runbook:
  - `SpaceShiftGame/Design/ci_alert_ops_runbook_2026-03-04.md`

## Notes
- `SpaceShift/` is now research-only.
- Keep new gameplay code and runtime assets in `SpaceShiftGame/`.
- Element cost coverage report (all 118 elements used in gameplay costs): `SpaceShiftGame/Reports/element_cost_coverage_2026-03-02.json`.
- Latest simulation report artifacts (including celestial economy chain): `SpaceShiftGame/Reports/simulation_suite_2026-03-02_celestial_r14.{json,md}` and `SpaceShiftGame/Reports/latest_simulation_report.json`.
- Long-run simulation and regression artifacts: `SpaceShiftGame/Reports/simulation_suite_2026-03-02_celestial_long_r14.{json,md}` and `SpaceShiftGame/Reports/simulation_regression_latest.json`.
- Endurance aggregate artifacts: `SpaceShiftGame/Reports/simulation_endurance_summary_2026-03-02.{json,md}`.
