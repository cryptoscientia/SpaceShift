#!/usr/bin/env python3
"""Preflight checks for SpaceShift web production environment variables."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


TRUE_SET = {"1", "true", "yes", "on"}


@dataclass
class CheckResult:
    level: str  # PASS | WARN | FAIL
    message: str


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().casefold() in TRUE_SET


def parse_allowed_origins() -> list[str]:
    raw = os.getenv("SPACESHIFT_ALLOWED_ORIGINS", "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def run_checks() -> list[CheckResult]:
    results: list[CheckResult] = []

    auth_required = env_flag("SPACESHIFT_AUTH_REQUIRED", default=True)
    if auth_required:
        results.append(CheckResult("PASS", "SPACESHIFT_AUTH_REQUIRED=true"))
    else:
        results.append(CheckResult("FAIL", "SPACESHIFT_AUTH_REQUIRED=false"))

    player_dev_login = env_flag("SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN", default=True)
    if player_dev_login:
        results.append(CheckResult("WARN", "SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN=true (demo mode only)"))
    else:
        results.append(CheckResult("PASS", "SPACESHIFT_ENABLE_PLAYER_DEV_LOGIN=false"))

    admin_dev_login = env_flag("SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN", default=False)
    if admin_dev_login:
        results.append(CheckResult("FAIL", "SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN=true"))
    else:
        results.append(CheckResult("PASS", "SPACESHIFT_ENABLE_ADMIN_DEV_LOGIN=false"))

    admin_god_mode = env_flag("SPACESHIFT_ENABLE_ADMIN_GOD_MODE", default=False)
    if admin_god_mode:
        results.append(CheckResult("FAIL", "SPACESHIFT_ENABLE_ADMIN_GOD_MODE=true"))
    else:
        results.append(CheckResult("PASS", "SPACESHIFT_ENABLE_ADMIN_GOD_MODE=false"))

    admin_username = os.getenv("SPACESHIFT_ADMIN_USERNAME", "admin").strip()
    admin_password = os.getenv("SPACESHIFT_ADMIN_PASSWORD", "admin").strip()
    if admin_username == "admin" and admin_password == "admin":
        results.append(CheckResult("FAIL", "Admin credentials still default admin/admin"))
    else:
        results.append(CheckResult("PASS", "Admin credentials not default"))

    allowed = parse_allowed_origins()
    if not allowed:
        results.append(CheckResult("FAIL", "SPACESHIFT_ALLOWED_ORIGINS is empty"))
    elif "*" in allowed:
        results.append(CheckResult("WARN", "SPACESHIFT_ALLOWED_ORIGINS contains '*' (temporary testing only)"))
    else:
        bad_http = [origin for origin in allowed if origin.startswith("http://")]
        if bad_http:
            results.append(CheckResult("WARN", f"Non-HTTPS origins found: {', '.join(bad_http)}"))
        else:
            results.append(CheckResult("PASS", "SPACESHIFT_ALLOWED_ORIGINS uses explicit HTTPS origins"))

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="SpaceShift web production preflight")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat WARN as failure exit code",
    )
    args = parser.parse_args()

    results = run_checks()

    fail_count = 0
    warn_count = 0
    for row in results:
        print(f"[{row.level}] {row.message}")
        if row.level == "FAIL":
            fail_count += 1
        elif row.level == "WARN":
            warn_count += 1

    print(f"[SUMMARY] fail={fail_count} warn={warn_count} total={len(results)}")

    if fail_count > 0:
        return 1
    if args.strict and warn_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
