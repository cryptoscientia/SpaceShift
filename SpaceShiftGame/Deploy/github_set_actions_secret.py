#!/usr/bin/env python3
"""Set a GitHub Actions repository secret using the REST API.

Required env:
  GITHUB_TOKEN   - token with Actions/admin repo permissions
  GITHUB_REPO    - owner/repo
  SECRET_NAME    - secret name (e.g. SPACESHIFT_CI_ALERT_WEBHOOK)
  SECRET_VALUE   - secret value

This script uses PyNaCl for sealed-box encryption:
  pip install pynacl
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request


def fail(message: str, code: int = 1) -> int:
    print(f"[FAIL] {message}", file=sys.stderr)
    return code


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def request_json(url: str, token: str) -> dict:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def put_json(url: str, token: str, payload: dict) -> int:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        method="PUT",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
        data=data,
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return int(getattr(resp, "status", 0))


def main() -> int:
    try:
        token = env_required("GITHUB_TOKEN")
        repo = env_required("GITHUB_REPO")
        secret_name = env_required("SECRET_NAME")
        secret_value = env_required("SECRET_VALUE")
    except ValueError as exc:
        return fail(str(exc))

    try:
        from nacl import encoding, public
    except Exception:
        return fail("PyNaCl is required. Install with: pip install pynacl")

    key_url = f"https://api.github.com/repos/{repo}/actions/secrets/public-key"
    secret_url = f"https://api.github.com/repos/{repo}/actions/secrets/{secret_name}"

    try:
        key_payload = request_json(key_url, token)
        key_id = str(key_payload.get("key_id", "")).strip()
        key_b64 = str(key_payload.get("key", "")).strip()
        if not key_id or not key_b64:
            return fail("GitHub key payload missing key_id/key")

        public_key = public.PublicKey(key_b64.encode("utf-8"), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key)
        encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
        encrypted_value = base64.b64encode(encrypted).decode("utf-8")

        status = put_json(
            secret_url,
            token,
            {"encrypted_value": encrypted_value, "key_id": key_id},
        )
        if status not in (201, 204):
            return fail(f"Unexpected status while writing secret: {status}")
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        return fail(f"GitHub API error {exc.code}: {detail}".strip())
    except Exception as exc:
        return fail(f"Secret set failed: {exc}")

    print(f"[PASS] Secret '{secret_name}' updated for {repo}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
