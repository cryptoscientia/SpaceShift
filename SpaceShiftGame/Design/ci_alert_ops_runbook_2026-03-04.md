# SpaceShift CI Alert Ops Runbook (2026-03-04)

## Scope

This runbook covers CI failure alerts for:

1. `.github/workflows/backend-ci-gate.yml`
2. `.github/workflows/backend-endurance-gate.yml`
3. `.github/workflows/backend-alert-smoke.yml` (manual health check)

Alert transport is configured via repository secret:

- `SPACESHIFT_CI_ALERT_WEBHOOK`

## Trigger Behavior

1. `backend-ci-gate.yml`:
- Sends webhook notification when any matrix leg fails.

2. `backend-endurance-gate.yml`:
- Sends webhook notification when a non-skipped endurance profile job fails.

3. `backend-alert-smoke.yml`:
- Manual dispatch sends a test alert and writes workflow summary.

## Setup Checklist

1. Ensure branch protection is enabled on `main` and required checks include matrix CI contexts.
2. Ensure `SPACESHIFT_CI_ALERT_WEBHOOK` exists under repository Actions secrets.
3. Run manual smoke workflow:
- GitHub Actions -> `Backend Alert Smoke` -> `Run workflow`.
4. Confirm alert received in destination channel.

## Rotation Policy

1. Rotate webhook endpoint/credential quarterly, or immediately after suspected exposure.
2. Update `SPACESHIFT_CI_ALERT_WEBHOOK` secret in repository settings.
3. Re-run `Backend Alert Smoke` and verify receipt.
4. Record rotation date and actor in team ops notes.

## Incident Triage

1. Acknowledge alert within 15 minutes.
2. Open failing workflow run and identify failing job/step.
3. If failure is deterministic, create/assign fix PR.
4. If failure is flaky, label as flaky and add follow-up hardening issue.
5. Re-run failed jobs after fix or mitigation.

## Escalation

1. Critical break (required checks blocked on `main`): escalate immediately to repo owner.
2. Endurance-only break with CI gate still green: triage same day; escalation if repeated.

## Ownership

1. Primary owner: repository owner (`cryptoscientia`).
2. Backup owner: assign a second maintainer when available.

## Audit Cadence

1. Weekly:
- Review CI failures and false-positive rate.
- Verify webhook destination still valid.
2. Monthly:
- Review endurance summaries and trend direction for regressions.
3. Quarterly:
- Rotate webhook credential and validate via smoke run.
