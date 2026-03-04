# SpaceShift Not Fully Done Checklist (2026-03-04)

This file is the explicit list of items that are still incomplete.

## 1) Full UI Parity With Galaxy Legion + SpaceShift Concepts

Status: `Not fully done`

Still missing:
1. Full Ship tab parity (complete fitting/equipment/inventory/effects/log UX parity).
2. Full Battle tab parity (target details, detailed combat log/action sequencing UI parity).
3. Full Trade tab parity (all artifact/special/trade-center views and filters).
4. Full Research tab parity (multi-page tree browsing, detailed right-side info panels).
5. Advanced Legion tab parity (alliance/treaty UX, treasury governance controls, and full member-admin polish).
6. Full Settings/About/Support style parity from concept references.

## 2) Multiplayer + Live Services

Status: `Not fully done`

Implemented now:
1. Auth/session baseline hardening:
- dev player login now defaults disabled unless explicitly enabled by env.
- bearer sessions include configurable TTL (`SPACESHIFT_SESSION_TTL_SECONDS`) and expire at lookup.
2. Optional identity-provider JWT verification mode:
- `SPACESHIFT_AUTH_MODE=local|jwt` (default `local`).
- JWT claim checks for `exp`/`nbf`/`iss`/`aud` with stable IdP subject -> player-key mapping.
- supports `RS256` via JWKS (`SPACESHIFT_JWT_JWKS_URL`) and deterministic `HS256` test mode (`SPACESHIFT_JWT_HS256_SECRET`).
3. SQLite concurrency hardening:
- WAL mode, busy timeout, synchronous tuning, WAL autocheckpoint, and immediate transactions.
4. Postgres migration path artifacts:
- `Code/server/sqlite_to_postgres_bundle.py`
- `Design/postgres_migration_path_2026-03-04.md`
5. Native runtime backend switch for persistent state:
- `SPACESHIFT_DB_BACKEND=sqlite|postgres` (default `sqlite`)
- `SPACESHIFT_POSTGRES_DSN` for live Postgres connections via `psycopg`
- startup schema bootstrapping and core state read/write paths now run through backend abstraction.

Still missing:
1. Full managed auth operations (IdP integration baseline now exists, but rotation/revocation/runbook/compliance controls are still incomplete).
2. Multi-user realtime sync and state conflict resolution.
3. Postgres production operations hardening (managed migrations, connection pooling, and staged failover runbooks).
4. Anti-cheat and authoritative validation hardening for competitive loops.

## 3) Missions + Campaign Depth

Status: `Partially done`

Still missing:
1. Full mission progression UX (mission board, chain state, completion/replay controls).
2. Narrative campaign integration of lore arcs into mission chains.
3. Event scheduling and rotating mission pools.

## 4) Economy + Market Depth

Status: `Partially done`

Implemented now:
1. Dynamic element pricing.
2. Buy/sell element market.
3. Credit/voidcoin exchange.
4. Craft/research cost deductions with elements + currency.
5. Alternative recipe substitutions for selected high-tier items/tech.
6. Track B2 economy model specification with formulas, guardrails, and tuning constants (`Design/economy_trade_track_b2.md`).
7. Regional market pricing modifiers (`region_id`, liquidity, risk premium, spread multipliers).
8. Player market listings API (prototype listing + browse + purchase flow).
9. Runtime life-support economy loop (`AIR`/`H2O`/`FOOD`) with persistent tick state, world+module production, crew+population consumption, shortage penalties, and commodity market rows/trading support.

Still missing:
1. Deep route-risk simulation (travel-time/escort/interdiction) feeding into all listings.
2. Long-horizon economy balancing toolchain and telemetry dashboards.
3. Full escrow and fraud-safe player contract UX with dispute handling.

## 5) Science Realism Depth

Status: `Partially done`

Implemented now:
1. Full 118-element table in gameplay.
2. Body-class composition templates including comets.
3. Heat/energy combat constraints.
4. Speculative high-tier tech branch with plausible framing.
5. Material catalog with real-world anchors (TiAu/metamaterial/ceramic/superalloy paths).
6. Synthetic AI race/faction + command-core tech/module integration.
7. Discovery progression model (player progression + scan strength scale discovery difficulty/results).
8. NASA-style procedural designation formatting for stars/planets/comets/asteroids.
9. Element-cost coverage report with all 118 elements represented in gameplay costs (`Reports/element_cost_coverage_2026-03-04_materials_r1.json`).
10. Celestial composition realism pass: all `23` templates recalibrated with broader source-backed major/trace ratios across planets, moons, gas giants, stars, asteroids, and comets (`Data/Seeds/celestial_templates.json`).
11. Discovery output broadened to retain up to `24` element lodes per scanned world (was `14`) in `Code/server/mock_server.py`.

Still missing:
1. Orbital mechanics simulation and transfer windows.
2. Detailed propulsion/chassis physics simulation.
3. Radiation/environmental hazard simulation depth beyond scalar hazard scores.

## 6) Content Volume

Status: `Partially done`

Implemented now:
1. 210 tech nodes.
2. 174 modules.
3. 23 hulls.
4. 38 structures.
5. 58 lore entries.
6. 10 material definitions.
7. 13 races and 6 factions.
8. 125 missions total, including faction-branching artifact arcs and race-focused progression hooks.
9. 50 substitution recipes.
10. 1 reverse-engineering consumable + 3 reverse-engineering recipes.
11. New race-focused branch content pack includes cat-like `Vesper Felari`, dog-like `Vargr Lykans`, avian `Caelith Avari`, and cetid `Thaloran Cetids`.

Still missing:
1. Full late-game content breadth comparable to mature live game scale.
2. Additional faction-specific content packs and event chains.

## 7) QA + Automation

Status: `Partially done`

Implemented now:
1. 22-check backend smoke suite passing end-to-end (`Code/server/smoke_test.py`).
2. Reproducible simulation harness added (`Code/server/run_simulation_suite.py`) with `standard` and `long` profiles.
3. Baseline simulation artifacts generated:
- `Reports/simulation_suite_2026-03-02.json`
- `Reports/simulation_suite_2026-03-02.md`
- `Reports/latest_simulation_report.json`
4. Long-run simulation artifacts generated:
- `Reports/simulation_suite_2026-03-02_long.json`
- `Reports/simulation_suite_2026-03-02_long.md`
5. Celestial discovery->extraction->market->craft->research simulation artifacts generated:
- `Reports/simulation_suite_2026-03-02_celestial.json`
- `Reports/simulation_suite_2026-03-02_celestial.md`
- `Reports/simulation_suite_2026-03-02_celestial_long.json`
- `Reports/simulation_suite_2026-03-02_celestial_long.md`
6. Baseline RN UI tests expanded to 4 suites / 17 tests:
- `Code/mobile/react-native/__tests__/App.flow.test.tsx`
- `Code/mobile/react-native/__tests__/App.research.integration.test.tsx`
- `Code/mobile/react-native/__tests__/App.crafting.integration.test.tsx`
- `Code/mobile/react-native/src/screens/__tests__/SocialCommandTab.test.tsx`
7. Regression threshold checker implemented:
- `Code/server/check_simulation_regression.py`
- `Reports/simulation_thresholds_v1.json`
- `Reports/simulation_regression_latest.json`
8. Multi-seed endurance aggregate tooling implemented:
- `Code/server/aggregate_simulation_endurance.py`
- `Reports/simulation_endurance_summary_2026-03-02.json`
- `Reports/simulation_endurance_summary_2026-03-02.md`
9. SimPy timeflow simulation integrated into suite:
- `Code/server/simpy_timeflow.py`
- integrated output key: `simpy_timeflow` in `Code/server/run_simulation_suite.py`
- covers event-driven queue dynamics, market microstructure, and extraction-logistics flows.
10. Standalone SimPy sweep tooling added for multi-seed/multi-scenario balancing:
- `Code/server/run_simpy_sweep.py`
- artifacts:
  - `Reports/simpy_sweep_2026-03-04_full_sweep_r1.json`
  - `Reports/simpy_sweep_2026-03-04_full_sweep_r1.md`
11. Simulation wallet-fetch hardening added in suite market/celestial passes so intermittent wallet probe failures are recorded in-report instead of aborting the entire run.
12. Long-profile simulation hardening pass completed in `run_simulation_suite.py`:
- profile-level HTTP retry/timeout policy controls (`http_timeout_scale`, `http_max_attempts`, backoff controls).
- long-profile load tuning (reduced burst factors while preserving long-horizon depth).
- quote-path graceful fallbacks and scan/trade pacing.
- verified pass artifacts:
  - `Reports/simulation_suite_2026-03-04_harden_std_r1.json`
  - `Reports/simulation_suite_2026-03-04_harden_long_r1.json`
  - `Reports/simulation_regression_latest.json` (pass)
13. Element lore completeness audit generated:
- `Reports/elements_lore_audit_2026-03-04.json`
- `Reports/elements_lore_audit_2026-03-04.md`
14. New content pass verification artifacts generated:
- `Reports/quality_integrity_stress_2026-03-04_content_pass3.json`
- `Reports/quality_integrity_stress_2026-03-04_content_pass3.md`
- `Reports/simulation_suite_2026-03-04_content_pass3_std.json`
- `Reports/simulation_suite_2026-03-04_content_pass3_long.json`
15. Extended simulation matrix pass completed (`12/12` task battery):
- quality-integrity stress rerun at `20,000` samples:
  - `Reports/quality_integrity_stress_2026-03-04_content_pass3_r2.json`
  - `Reports/quality_integrity_stress_2026-03-04_content_pass3_r2.md`
- multi-seed suite runs:
  - `Reports/simulation_suite_2026-03-04_matrix_std_s1.json`
  - `Reports/simulation_suite_2026-03-04_matrix_std_s2.json`
  - `Reports/simulation_suite_2026-03-04_matrix_std_s3.json`
  - `Reports/simulation_suite_2026-03-04_matrix_long_s1.json`
  - `Reports/simulation_suite_2026-03-04_matrix_long_s2.json`
- SimPy sweep:
  - `Reports/simpy_sweep_2026-03-04_matrix_sweep.json`
  - `Reports/simpy_sweep_2026-03-04_matrix_sweep.md`
- endurance aggregates:
  - `Reports/simulation_endurance_summary_2026-03-04_matrix_std.json`
  - `Reports/simulation_endurance_summary_2026-03-04_matrix_long.json`
- mining aggregate:
  - `Reports/mining_matrix_2026-03-04.json`
  - `Reports/mining_matrix_2026-03-04.md`
16. Chemistry-focused simulation pass completed after ratio updates:
- `Reports/simulation_suite_2026-03-04_chemistry_std_r1.json`
- `Reports/simulation_suite_2026-03-04_chemistry_long_r1.json`
- `Reports/simpy_sweep_2026-03-04_chemistry_sweep_r1.json`
- `Reports/mining_matrix_2026-03-04_chemistry_r1.json`
17. Materials-realism + life-support/additive-manufacturing pass completed with green revalidation:
- `Reports/simulation_suite_2026-03-04_materials_std_r2.json`
- `Reports/simulation_suite_2026-03-04_materials_long_r2.json`
- `Reports/simulation_regression_latest.json`
18. Life-support runtime economy pass completed with green validation:
- `Reports/simulation_suite_2026-03-04_lifesupport_std_r1.json`
- `Reports/simulation_suite_2026-03-04_lifesupport_std_r1.md`
- `Reports/simpy_sweep_2026-03-04_lifesupport_sweep_r1.json`
- `Reports/simpy_sweep_2026-03-04_lifesupport_sweep_r1.md`
- `Reports/simulation_regression_latest.json`
19. CI-wired backend gate automation implemented:
- local reusable gate script: `Code/server/run_ci_gate.sh`
- GitHub Actions workflow: `.github/workflows/backend-ci-gate.yml`
- gate executes compile + smoke + simulation suite (`standard`) + regression checks on `push` + `pull_request`.
20. CI depth expansion pass implemented:
- backend gate now runs matrix legs across `ubuntu-latest` + `macos-latest` and Python `3.11` + `3.12`.
- per-run simulation/regression artifacts are uploaded from CI.
- optional failure webhook notification hook added via `SPACESHIFT_CI_ALERT_WEBHOOK` secret.
- endurance/soak CI workflow + script added:
  - `.github/workflows/backend-endurance-gate.yml`
  - `Code/server/run_ci_endurance_gate.sh`
  - scheduled + manual multi-seed aggregate run path.
21. GitHub hardening helper automation added:
- `Deploy/github_apply_branch_protection.sh` for branch-protection API setup.
- `Deploy/github_set_actions_secret.py` for encrypted Actions secret updates (e.g., `SPACESHIFT_CI_ALERT_WEBHOOK`).
22. Live GitHub branch protection is now enabled on `cryptoscientia/SpaceShift:main`:
- strict status checks required for matrix CI jobs:
  - `Backend CI Gate (ubuntu-latest, py3.11)`
  - `Backend CI Gate (ubuntu-latest, py3.12)`
  - `Backend CI Gate (macos-latest, py3.11)`
  - `Backend CI Gate (macos-latest, py3.12)`
- stale reviews dismissed, `1` required approval, admins enforced, and conversation resolution required.
23. Alert-ops baseline completed:
- repository secret `SPACESHIFT_CI_ALERT_WEBHOOK` is configured in `cryptoscientia/SpaceShift`.
- manual smoke workflow added: `.github/workflows/backend-alert-smoke.yml`.
- runbook added with setup/rotation/escalation steps:
  - `Design/ci_alert_ops_runbook_2026-03-04.md`
24. Endurance CI breadth hardening completed:
- endurance workflow now runs profile matrix (`standard`, `long`) with broader default multi-seed sets.
- endurance gate now enforces aggregate regression thresholds via:
  - `Code/server/check_endurance_regression.py`
  - `Reports/endurance_thresholds_v1.json`
- per-profile latest endurance snapshots and regression outputs are emitted:
  - `Reports/latest_endurance_<profile>.json|md`
  - `Reports/endurance_regression_latest_<profile>.json`
- workflow summary now publishes key endurance signals (run count, smoke mean, regression pass rate, worlds scanned, market delta).

Still missing:
1. Full automated UI test coverage for RN flows (baseline social tab tests exist; most tabs still uncovered).
2. Long-horizon historical trend analytics/visualization for endurance metrics (current state provides per-run summary + threshold gating, but not multi-month dashboards).

## 8) Delivery/Platform

Status: `Not fully done`

Still missing:
1. Production mobile build/release pipelines (TestFlight/Play internal track setup).
2. Crash/analytics instrumentation.
3. Patch/content delivery system for live updates.

## 9) Additional Audit Gaps (Current Pass)

Status: `Partially done`

Implemented in current pass:
1. Mission runtime state machine (`/api/missions/accept|progress|claim`) plus `/api/missions/jobs`.
2. Manifest integrity refresh and seed hash coverage (including `ai_opponents.json`).
3. Schema coverage expansion for ability/artifact/blueprint/event plus element-tier and periodic-source schema domains.
4. Deterministic simulation mode controls (`SPACESHIFT_DETERMINISTIC`) and stable-hash replacement in simulation-critical paths.
5. Endurance aggregate regression linkage fixed to per-report regression fields (no global latest-file leakage).
6. Regression-threshold expansion (required module checks, guard checks, executed-trades minimum).
7. Auth hardening baseline: bearer sessions, role checks, reserved `admin` handling, explicit CORS allowlist, and world-claim authority checks.
8. Simulation harness auth-token routing fixes and world-claim flow alignment with authoritative discovery records.
9. Element-cost coverage generator added (`Data/Tools/generate_element_cost_coverage.py`) with fresh 118/118 report output.
10. Faction + legion social-governance runtime (persistent affiliations, legion creation/membership, join approvals, role governance, proposal/vote/finalize flow, and event log APIs).
11. Mobile Social command tab baseline wired in React Native (`App.tsx`): faction alignment, legion create/join, join-request moderation, governance proposal/vote/finalize, and event feed sync.
12. Combat authority pass: player combat paths now resolve attacker stats from persisted equipped loadouts with backward-compatible fallback when loadout data is missing.

Still missing:
1. CI depth completion beyond current backend/endurance gates (coverage enforcement, policy enforcement, and alert operations maturity).
2. Production auth hardening completion (IdP token verification path is implemented; token rotation, secret/JWKS ops, and secure credential policy hardening remain).
3. Content-model consistency cleanup (duplicate unlock pathways in constellation missions; substitution dynamic/static doc mismatch).
4. Advanced legion systems beyond MVP (alliance treaties, shared projects, treasury spending permissions, seasonal governance cycles).
5. CI failure-alert secret still pending in live repo: `SPACESHIFT_CI_ALERT_WEBHOOK`.

## 10) Cross-Game Gap Pass (2026-03-03)

Status: `Not fully done`

Detailed report:
1. `Design/cross_game_missing_features_2026-03-03.md`

Top missing gameplay loops identified from Galaxy Legion, Space Arena, Second Galaxy, CoaDE, and Orion's Arm-inspired captures:
1. Artifact lifecycle runtime (acquire/use/scrap/send, cooldown/effect windows, daily cadence).
2. Ability runtime depth (active/passive triggers, energy cost, cooldown, duration, stacking rules).
3. Deep PvP verb expansion (baseline covert steal/sabotage/hack now implemented; raid/invade classes and counter-intelligence depth still missing).
4. Tactical mine systems and area denial combat behavior.
5. Fuel-backed maneuver constraints in combat actions.
6. Pilot license/proficiency progression and doctrine specialization depth.
7. Event cadence runtime and mission/market seasonal rotation.
8. Layered armor + subsystem damage states for science-grounded combat consequences.

## 11) Consolidated Gap Inventory (2026-03-03)

Status: `Newly added`

Detailed report:
1. `Design/missing_gaps_master_2026-03-03.md`

Highlights:
1. P0 launch blockers now explicitly consolidated across security, auth, RNG authority, release engineering, and legal/compliance.
2. Simulation coverage is documented with explicit untested domains (concurrency/adversarial/mobile-device/perf).
3. Execution order is prioritized for productionization.

## 12) Production iPhone Checklist (2026-03-03)

Status: `Newly added`

Detailed report:
1. `Design/production_iphone_readiness_checklist_2026-03-03.md`

Highlights:
1. End-to-end iPhone release checklist added with status tags (`Done/Partial/Missing/Unverified`).
2. Includes release gates that must all be true before App Store submission.
3. Covers Apple distribution, security, privacy, backend readiness, gameplay authority, QA, observability, and support/incident ops.

## 13) AI-Generated Content Brainstorm (2026-03-03)

Status: `Newly added`

Detailed report:
1. `Design/ai_generated_art_and_content_brainstorm_2026-03-03.md`

Highlights:
1. Hybrid strategy defined: deterministic gameplay visuals + generated cosmetics.
2. 2D battle behavior preserved (Space Arena-like readability) while allowing unique player ship/world/captain art.
3. LLM flavor-text model scoped with deterministic mission rules and moderation guardrails.

## 14) Web-First Pivot (2026-03-03)

Status: `Partially done`

Detailed report:
1. `Design/web_first_migration_plan_2026-03-03.md`
2. `Design/production_web_readiness_checklist_2026-03-03.md`

Implemented now:
1. Static web client runtime config file (`Code/client/config.js`) for hosted API targets.
2. Web client API base persistence + URL override support (`?api=...`) in `Code/client/app.js`.
3. Wake-up retry UX for sleeping free-tier backends in `Code/client/app.js`.
4. Backend CORS wildcard support for temporary public testing (`SPACESHIFT_ALLOWED_ORIGINS=*`) in `Code/server/mock_server.py`.
5. Updated web-first run/deploy docs in:
  - `SpaceShiftGame/README.md`
  - `Code/client/README.md`
  - `Code/server/README.md`

Still missing:
1. Managed Postgres migration for multi-user web concurrency (prototype still SQLite).
2. Production-grade deployment manifests/CI for front-end + backend auto-release.
3. Account/auth hardening for public web launch (dev login defaults must be off in production).

## 15) Admin + Storage Control Pass (2026-03-03)

Status: `Partially done`

Implemented now:
1. Admin roster and moderation runtime:
  - `GET /api/admin/players`
  - `POST /api/admin/players/moderate` (`kick|suspend|ban|clear`) with reason/duration logging.
2. Admin action auditing:
  - `GET /api/admin/actions`
  - server-side action log persistence in `admin_action_log`.
3. Admin forced jackpot crafting for test/control workflows:
  - `POST /api/admin/crafting/jackpot`.
4. Inventory disposal runtime with high-value confirmation warnings:
  - `POST /api/inventory/trash` for crafted instances, asset stacks, and element disposal.
5. Inventory capacity model and smuggle storage scaffolding:
  - `GET /api/inventory/storage`
  - `POST /api/inventory/storage/upgrade`
  - `GET /api/assets/smuggled`
  - `POST /api/assets/smuggle/move`.
6. Craft/manufacture storage gating integrated so overflow is prevented before reward grant.
7. Smoke suite expanded to validate new admin/storage/trash flows (`21/21` passing).

Still missing:
1. Full UI screens for admin moderation and storage management (RN/web UI still API-first).
2. Fine-grained contraband detection simulation and anti-exploit balancing for smuggle systems.
3. Admin dashboard polish for moderation evidence, appeal notes, and escalation workflow.

## 16) Covert Ops Runtime Pass (2026-03-03)

Status: `Partially done`

Implemented now:
1. New covert ops API surface:
  - `GET /api/covert/policy`
  - `GET /api/covert/cooldowns`
  - `GET /api/covert/logs`
  - `POST /api/covert/steal`
  - `POST /api/covert/sabotage`
  - `POST /api/covert/hack`
2. Persistent cooldown and operation log tables:
  - `covert_op_cooldowns`
  - `covert_op_log`
3. Runtime mechanics:
  - Action-energy costs, success/detection probability rolls, and per-op cooldown timers.
  - Fair-play guardrails to suppress high-level farming against weaker non-opt-in targets.
  - Steal outcomes (asset/element transfer), sabotage outcomes (durability/module/inventory disruption), and hack outcomes (intel + energy disruption + bounty credits).
4. Smoke and simulation coverage:
  - `Code/server/smoke_test.py` now validates covert runtime endpoints.
  - `Code/server/run_simulation_suite.py` now includes a covert-ops stress section.

Still missing:
1. Full client UX for covert operations (target selection/intel views/log review/counterplay controls).
2. Counter-intelligence systems (decoys, active defense automation, detection broadcasts, retaliatory contracts).
3. Territory-aware covert risk routing and alliance-level covert doctrine controls.
