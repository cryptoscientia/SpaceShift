# SpaceShift Deploy Templates

These templates are for the web-first deployment path.

## Included

1. `render.yaml`
- Backend web service template.
- Runs Python mock server with hosted `PORT` binding.

2. `netlify.toml`
- Static web front-end template.
- Publishes `SpaceShiftGame/Code/client` with no build command.

3. `github_apply_branch_protection.sh`
- Applies baseline branch protection for SpaceShift CI checks.
- Requires `GITHUB_TOKEN` and `GITHUB_REPO`.

4. `github_set_actions_secret.py`
- Sets repository Actions secrets through GitHub API (encrypted).
- Useful for `SPACESHIFT_CI_ALERT_WEBHOOK`.
- Requires `PyNaCl` (`pip install pynacl`).

5. `.github/workflows/backend-alert-smoke.yml`
- Manual workflow to verify the alert webhook path is alive and receiving payloads.
- Use after setting or rotating `SPACESHIFT_CI_ALERT_WEBHOOK`.

## Notes

- Before deploying, set `apiBase` in `SpaceShiftGame/Code/client/config.js` to your backend URL.
- For production, disable dev logins and set strict `SPACESHIFT_ALLOWED_ORIGINS`.

## GitHub Hardening Setup

Set branch protection:

```bash
GITHUB_TOKEN=<token> \
GITHUB_REPO=<owner/repo> \
GITHUB_BRANCH=main \
bash SpaceShiftGame/Deploy/github_apply_branch_protection.sh
```

Set failure-alert webhook secret:

```bash
pip install pynacl
GITHUB_TOKEN=<token> \
GITHUB_REPO=<owner/repo> \
SECRET_NAME=SPACESHIFT_CI_ALERT_WEBHOOK \
SECRET_VALUE='https://example.webhook.url' \
python3 SpaceShiftGame/Deploy/github_set_actions_secret.py
```

Run alert smoke test:

1. Open GitHub Actions for your repo.
2. Select `Backend Alert Smoke`.
3. Click `Run workflow`.
4. Confirm alert appears in your webhook destination.

Operational runbook:

- `SpaceShiftGame/Design/ci_alert_ops_runbook_2026-03-04.md`
