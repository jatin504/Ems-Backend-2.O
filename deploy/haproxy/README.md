# Ubuntu: the same setup for local verification and deployment

## Ports and processes

```text
Frontend :4445 or :5000
        |
        v
HAProxy 0.0.0.0:5179        <-- public API listener; separate Linux service
        |
        +-- Quart api1 127.0.0.1:5180 (primary)
        +-- Quart api2 127.0.0.1:5181 (optional read worker)
        +-- Quart api3 127.0.0.1:5182 (optional read worker)

Existing main.py --> http://127.0.0.1:5180/stream/internal/publish
All APIs use the existing shared DB/cache/data folder.
```

`python run_api_cluster.py` starts Quart only, not HAProxy. Its log now prints
the configured frontend endpoint and explicitly says HAProxy is not started.
`cluster.json` is the only proxy/worker settings file. Default count remains 1.
`generated.cfg` is generated from it; there is no second handwritten HAProxy config.
Worker ports bind to localhost. Do not expose them publicly.

Production frontend can retain:

```dotenv
VITE_API_BASE=http://75.119.146.115:5179
```

For local Ubuntu testing use `http://localhost:5179` when the browser runs on that
machine, otherwise `http://UBUNTU_TEST_IP:5179`. Never point a local test frontend
at the real production IP. Use the same proxy base URL for SSE/auth/download URLs.

## Before starting

1. Use existing LOCAL/TEST DB/cache/services and a test EMS data directory. Confirm
   root `.env`: API startup can seed users and update status JSON. No DB migration
   or duplicate database is required. Keep the existing main.py role arrangement;
   do not start another controller just to scale HTTP.
2. Remove stale shell DB overrides. PowerShell archive-demo cleanup, if needed:
   `Remove-Item Env:PRIMARY_DB_NAME -ErrorAction SilentlyContinue`. Bash equivalent:
   `unset PRIMARY_DB_NAME`. This restores .env precedence for that variable.
3. Stop the old standalone API/service occupying 5179 during the planned cutover.
   HAProxy will own 5179; Quart must move to 5180+. Do not run the old standalone
   `python api_server.py` alongside this cluster (its default is still 5179).
4. Add to the actual runtime `.env`:

```dotenv
EMS_INTERNAL_API_URL=http://127.0.0.1:5180
EMS_CORS_ORIGINS=http://localhost:4445,http://localhost:5000,http://127.0.0.1:4445,http://127.0.0.1:5000
```

Set exact real frontend origins for LAN/production, including their frontend
ports. CORS origins are browser origins, not the API destination URL. No trailing
slash. `ALLOWED_ORIGINS` in `app/middleware/cors_config.py` provides the same local
defaults if the environment override is absent.

Do NOT set EMS_API_BIND, EMS_API_ROLE or EMS_API_INSTANCE in .env for the cluster:
the launcher supplies them per process. The internal publisher setting is needed
because the public proxy blocks internal publish paths. Restart the existing
runtime under its normal controlled procedure after changing its publisher URL;
don't create a second runtime. Its default remains localhost:5179 for legacy
standalone deployments when EMS_INTERNAL_API_URL is unset.

## Local Ubuntu test: terminal 1, Quart

Use the existing Linux Python environment; a Windows venv cannot run on Ubuntu.
From the project root with dependencies already installed:

```bash
.venv/bin/python run_api_cluster.py --instances 2
```

Adjust `.venv/bin/python` to the working environment path. If creating a clean
Linux environment, use `python3 -m venv .venv` and install `requirements.txt` first;
resolve native dependencies/version compatibility before continuing.

This starts api1 :5180 and api2 :5181, prints PIDs/roles and generates
`deploy/haproxy/generated.cfg`. Keep terminal 1 open. Confirm both are ready:

```bash
curl -i http://127.0.0.1:5180/health/ready
curl -i http://127.0.0.1:5181/health/ready
```

Expect 200, READY, X-EMS-Instance api1/api2. Readiness checks required config
presence and bounded PostgreSQL SELECT 1, not all telemetry dependencies. Fix the
first startup error if they fail. The launcher stops after three repeated short
failures instead of restarting indefinitely. It doesn't create missing DBs.

## Local Ubuntu test: terminal 2, actual HAProxy

```bash
sudo apt-get update
sudo apt-get install haproxy
sudo haproxy -c -f deploy/haproxy/generated.cfg
sudo haproxy -db -f deploy/haproxy/generated.cfg
```

Run the last two commands from the project root. Validation must succeed before
starting. The final command starts the real proxy in foreground on 5179; leave
terminal 2 open. If 5179 is occupied, find the process with `sudo ss -ltnp` and
stop only your old test API/proxy. Do not kill unrelated services. Native Ubuntu
can run both APIs and HAProxy; WSL/VM networking is unnecessary in this topology.

## Local Ubuntu test: terminal 3, frontend endpoint

```bash
curl -i http://127.0.0.1:5179/health/ready
for i in 1 2 3 4 5 6 7 8; do
  curl -s -D - http://127.0.0.1:5179/health -o /dev/null
done
sudo ss -ltnp
```

Expected listeners: HAProxy :5179; Python localhost :5180/:5181. Repeated health
responses should include api1 and api2 in X-EMS-Instance (leastconn does not
promise exact alternation). `/health/ready` through the proxy checks primary.
Then verify actual frontend query responses, configuration reads/writes and SSE,
not only health. For SSE use a real topic:

```bash
curl -N 'http://127.0.0.1:5179/stream?topics=REAL_DEVICE_ID'
curl -i -H 'Origin: http://localhost:5000' http://127.0.0.1:5179/health
```

Expect stream data/heartbeats and the matching allowed-origin CORS header. Verify
both frontends in browser DevTools. Public /stream/internal/... and /app/... must
return 403. Direct read-worker control requests must return 503 without executing.
Test worker restart only in this isolated environment using a test PID printed
by the supervisor. Do not stop shared services or a live controller to test it.

## One, two, three instances

Persistent count: set `instances` in cluster.json to 1/2/3 and start the same
supervisor command without an override. Temporary test: `--instances 2` or `3`.
Stop the old supervisor before the next run. Restart foreground HAProxy using
the regenerated config, or validate/deploy/reload it under systemd. This has a
brief planned interruption; hot worker addition/removal is not implemented.
The public frontend port stays 5179 for every count.

## Deployment after local acceptance

Use `/opt/ems` (or adjust all paths) and a dedicated `ems` account with required
runtime file permissions. Provision the existing service credentials/data safely.
The supplied ems-api.service runs the supervisor; cluster.json supplies count.
In the maintenance window, stop the old API service before handing 5179 to HAProxy.

```bash
.venv/bin/python run_api_cluster.py --generate-only
sudo haproxy -c -f deploy/haproxy/generated.cfg
sudo install -m 644 deploy/haproxy/generated.cfg /etc/haproxy/haproxy.cfg
sudo install -m 644 deploy/haproxy/ems-api.service /etc/systemd/system/ems-api.service
sudo systemctl daemon-reload
sudo systemctl enable --now ems-api
sudo systemctl enable --now haproxy
sudo systemctl reload haproxy
sudo systemctl status ems-api haproxy
sudo journalctl -u ems-api -u haproxy -n 100 --no-pager
```

These commands assume foreground tests are stopped, paths/accounts are provisioned
and package/service access has been verified. Validate source config before
replacing live config, retain its previous copy for rollback, and verify readiness
and browser requests after every restart/reload. Use systemd for ongoing startup,
recovery and logging, not a manually open SSH terminal. Don't run standalone API
and supervisor services simultaneously. After changing count, restart ems-api and
validate/copy/reload the newly generated HAProxy config.

HTTP :5179 preserves the current URL but is not encryption. For public production
traffic provision DNS/certificates and TLS (HAProxy tls_pem setting or an approved
upstream TLS proxy), then update the frontend URL to HTTPS. Configure certificate
renewal, backups, monitoring, dependency security and failure/load testing before
acceptance. HTTPS frontend pages cannot call a plain HTTP API.

## Scope and code review

Current behavior is read scaling: GET/HEAD /latest/* and /history/* plus POST
/api/timeseries-data and /api/online_devices are balanced. /health and /ping are
balanced for verification. All other routes, including mutations/control/SSE,
remain primary-owned. A primary failure temporarily interrupts those routes.
Full stateful failover and SSE scaling require additional coordination; sharing
one DB alone doesn't solve process-local broadcasters or JSON writer conflicts.

The DB connection/pool code remains unchanged. API startup avoids duplicate user
seeding/status writes on read workers; device JSON replacement is atomic and
timeseries queries refresh configuration. Primary mutations are serialized across
awaits. This does not make background/controller file writes transactional or fix
the unknown existing crash. Read guards are not SQL sandboxes; existing input,
authorization, hardcoded internal secrets and JWT fallback need separate review.

Removed the duplicate handwritten haproxy.cfg and obsolete Windows-to-WSL guide;
there is one generated proxy config and this Ubuntu procedure. Removed an unused
CORS import. Windows service support remains optional for the earlier platform
requirement; this runbook targets Ubuntu only.

Tests: `.venv/bin/python -m unittest discover -s tests -p test_api_scaling.py -v`.
They use isolated apps/fake DB and don't replace actual Ubuntu HAProxy syntax,
real EMS routing, load, recovery, reboot and restore tests. One machine/shared
database/proxy remains a failure domain; this is not whole-system HA.

Official HAProxy health checks:
https://www.haproxy.com/documentation/haproxy-configuration-tutorials/reliability/health-checks/
