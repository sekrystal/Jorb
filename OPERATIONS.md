# Opportunity Scout Operations

This runbook assumes the system is deployed on Ubuntu with systemd.

## Check Service Status

```bash
sudo systemctl status opportunity-scout-api.service --no-pager
sudo systemctl status opportunity-scout-worker.service --no-pager
sudo systemctl status opportunity-scout-ui.service --no-pager
```

## Tail Logs

```bash
sudo journalctl -u opportunity-scout-api.service -f
sudo journalctl -u opportunity-scout-worker.service -f
sudo journalctl -u opportunity-scout-ui.service -f
```

## Restart Services

```bash
sudo systemctl restart opportunity-scout-api.service
sudo systemctl restart opportunity-scout-worker.service
sudo systemctl restart opportunity-scout-ui.service
```

## Disable All Autonomy

Edit `/etc/opportunity-scout/opportunity-scout.env`:

```bash
AUTONOMY_ENABLED=false
```

Then reload:

```bash
sudo systemctl restart opportunity-scout-worker.service
```

The worker runtime state in the database should also be left at `paused` until you intentionally resume it from the UI or API.

## Disable Greenhouse Only

Edit `/etc/opportunity-scout/opportunity-scout.env`:

```bash
GREENHOUSE_ENABLED=false
```

Then reload:

```bash
sudo systemctl restart opportunity-scout-worker.service
```

## Inspect Connector Health

```bash
curl -s http://127.0.0.1:8000/autonomy-status
```

Look for:

- `status`
- `circuit_state`
- `last_success_at`
- `last_failure_at`
- `trust_score`
- `approved_for_unattended`

## Inspect Alerts

Alerts are:

- sent to Slack when configured
- persisted in `alert_events`

Example Postgres query:

```bash
sudo -u postgres psql opportunity_scout
```

```sql
SELECT created_at, alert_key, severity, status, summary
FROM alert_events
ORDER BY created_at DESC
LIMIT 50;
```

## Manual Demo Reset

Only use this on a local or explicit demo instance:

```bash
cd /opt/opportunity-scout
sudo -u oppscout env $(grep -v '^#' /etc/opportunity-scout/opportunity-scout.env | xargs) /opt/opportunity-scout/.venv/bin/python scripts/reset_demo.py
```

## Morning Checks After An Overnight Run

1. Confirm worker is still active.
2. Check `autonomy-status` for Greenhouse:
   - `status=healthy` or clearly explainable `degraded`
   - `circuit_state=closed`
   - recent `last_success_at`
3. Check `alert_events` for any overnight critical alerts.
4. Confirm no duplicate explosion:
   - duplicate alerts absent
   - lead counts look stable
5. Confirm no visible stale rows by default in the UI.
6. Review latest digest:
   - it should show either a clear change summary or a stable no-op summary
7. Review worker logs for repeated exceptions or transport failures.
8. Confirm runtime state is what you expect:
   - `paused` for cautious bring-up
   - `running` only when you intentionally want unattended cycles
