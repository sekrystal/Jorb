# Deploy Opportunity Scout On Ubuntu

This guide targets a small Ubuntu VM with:

- FastAPI API
- dedicated worker loop
- Streamlit UI
- Postgres
- systemd services
- Greenhouse-first unattended operation

## 1. VM Prerequisites

Recommended baseline:

- Ubuntu 24.04 LTS
- 2 vCPU
- 4 GB RAM
- 20+ GB disk

## 2. Install System Packages

```bash
sudo apt update
sudo apt install -y python3.11 python3.11-venv python3-pip git curl postgresql postgresql-contrib nginx
```

## 3. Create App User

```bash
sudo useradd --system --create-home --shell /bin/bash oppscout
sudo mkdir -p /opt/opportunity-scout /etc/opportunity-scout
sudo chown -R oppscout:oppscout /opt/opportunity-scout /etc/opportunity-scout
```

## 4. Configure Postgres

```bash
sudo -u postgres psql
```

Inside `psql`:

```sql
CREATE USER oppscout WITH PASSWORD 'CHANGE_ME';
CREATE DATABASE opportunity_scout OWNER oppscout;
\q
```

## 5. Clone Repo

```bash
sudo -u oppscout git clone <YOUR_REPO_URL> /opt/opportunity-scout
cd /opt/opportunity-scout
```

## 6. Create Virtualenv And Install Dependencies

```bash
sudo -u oppscout python3.11 -m venv /opt/opportunity-scout/.venv
sudo -u oppscout /opt/opportunity-scout/.venv/bin/pip install --upgrade pip
sudo -u oppscout /opt/opportunity-scout/.venv/bin/pip install -r /opt/opportunity-scout/requirements.txt
```

## 7. Create Production Env File

```bash
sudo cp /opt/opportunity-scout/.env.production.example /etc/opportunity-scout/opportunity-scout.env
sudo chown oppscout:oppscout /etc/opportunity-scout/opportunity-scout.env
sudo nano /etc/opportunity-scout/opportunity-scout.env
```

Set at minimum:

- `DATABASE_URL`
- `DEMO_MODE=false`
- `AUTONOMY_ENABLED=false`
- `GREENHOUSE_ENABLED=false`
- `WORKER_INTERVAL_SECONDS=900`
- `GREENHOUSE_BOARD_TOKENS=stripe,airtable`
- `ALERTS_ENABLED=true`
- `SLACK_WEBHOOK_URL=...`

For first Linux bring-up, keep autonomy and Greenhouse disabled until:

1. `scripts/init_db.py` succeeds
2. API and UI both boot
3. `/autonomy-status` returns clean health state
4. you are ready to watch the first worker cycle

Then enable `AUTONOMY_ENABLED=true` and `GREENHOUSE_ENABLED=true`.

## 8. Initialize The Database

```bash
cd /opt/opportunity-scout
sudo -u oppscout env $(grep -v '^#' /etc/opportunity-scout/opportunity-scout.env | xargs) /opt/opportunity-scout/.venv/bin/python scripts/init_db.py
```

## 9. Install systemd Units

```bash
sudo cp /opt/opportunity-scout/deploy/systemd/opportunity-scout-api.service /etc/systemd/system/
sudo cp /opt/opportunity-scout/deploy/systemd/opportunity-scout-worker.service /etc/systemd/system/
sudo cp /opt/opportunity-scout/deploy/systemd/opportunity-scout-ui.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 10. Start Services

```bash
sudo systemctl enable opportunity-scout-api.service
sudo systemctl enable opportunity-scout-worker.service
sudo systemctl enable opportunity-scout-ui.service
sudo systemctl start opportunity-scout-api.service
sudo systemctl start opportunity-scout-worker.service
sudo systemctl start opportunity-scout-ui.service
```

## 11. First-Run Validation

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/autonomy-status
curl -I http://127.0.0.1:8500
sudo systemctl status opportunity-scout-api.service --no-pager
sudo systemctl status opportunity-scout-worker.service --no-pager
sudo systemctl status opportunity-scout-ui.service --no-pager
```

## 12. Optional Nginx Reverse Proxy

Example:

- proxy `/` to `127.0.0.1:8500`
- proxy `/api/` or direct API subdomain to `127.0.0.1:8000`

If you keep the API private, only expose Streamlit publicly and protect it with basic auth or VPN.
