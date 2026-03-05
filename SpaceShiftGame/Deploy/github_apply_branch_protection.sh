#!/usr/bin/env bash
set -euo pipefail

# Applies baseline branch protection for SpaceShift CI gates.
#
# Required env:
#   GITHUB_TOKEN   - GitHub token with repository administration permissions
#   GITHUB_REPO    - owner/repo (e.g. acme/spaceshift)
#
# Optional env:
#   GITHUB_BRANCH  - branch name (default: main)
#   PR_APPROVALS   - required approvals count (default: 1)

if ! command -v curl >/dev/null 2>&1; then
  echo "[FAIL] curl is required." >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "[FAIL] jq is required." >&2
  exit 1
fi

GITHUB_TOKEN="${GITHUB_TOKEN:-}"
GITHUB_REPO="${GITHUB_REPO:-}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"
PR_APPROVALS="${PR_APPROVALS:-1}"

if [[ -z "${GITHUB_TOKEN}" ]]; then
  echo "[FAIL] GITHUB_TOKEN is required." >&2
  exit 1
fi
if [[ -z "${GITHUB_REPO}" ]]; then
  echo "[FAIL] GITHUB_REPO is required (owner/repo)." >&2
  exit 1
fi
if ! [[ "${PR_APPROVALS}" =~ ^[0-9]+$ ]]; then
  echo "[FAIL] PR_APPROVALS must be a non-negative integer." >&2
  exit 1
fi

api_base="https://api.github.com/repos/${GITHUB_REPO}/branches/${GITHUB_BRANCH}/protection"

payload="$(jq -n --argjson approvals "${PR_APPROVALS}" '{
  required_status_checks: {
    strict: true,
    contexts: [
      "Backend CI Gate (ubuntu-latest, py3.11)",
      "Backend CI Gate (ubuntu-latest, py3.12)",
      "Backend CI Gate (macos-latest, py3.11)",
      "Backend CI Gate (macos-latest, py3.12)"
    ]
  },
  enforce_admins: true,
  required_pull_request_reviews: {
    dismiss_stale_reviews: true,
    require_code_owner_reviews: false,
    required_approving_review_count: $approvals
  },
  restrictions: null,
  required_conversation_resolution: true
}')"

echo "[INFO] Applying branch protection to ${GITHUB_REPO}:${GITHUB_BRANCH}"
http_code="$(curl -sS -o /tmp/spaceshift_branch_protection.json -w "%{http_code}" \
  -X PUT \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "${api_base}" \
  -d "${payload}")"

if [[ "${http_code}" != "200" ]]; then
  echo "[FAIL] Branch protection API failed (HTTP ${http_code})." >&2
  cat /tmp/spaceshift_branch_protection.json >&2
  exit 1
fi

echo "[PASS] Branch protection applied."
