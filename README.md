# Railway DNS Multi-Region Probe

Two minimal services for testing how Railway private DNS behaves with replicas and regions.

- `service-a`: target service that returns identity and DNS snapshots.
- `service-b`: probing service that calls `service-a`, runs repeated internal sampling, and aggregates by `service + region + replica`.

## Why this exists

You can hit one endpoint on `service-b` and see a combined report without physically testing from different geographies.

`/combined` runs internal probes and reports what regions/replicas were actually observed.

## Endpoints

### service-a

- `GET /whoami`: service identity (`RAILWAY_REPLICA_ID`, `RAILWAY_REPLICA_REGION`, etc.).
- `GET /dig`: runs `dig` and DNS lookup from inside `service-a`.
- `GET /report`: same as `/dig` with defaults.

### service-b

- `GET /whoami`: service identity.
- `GET /dig`: runs `dig` and DNS lookup from inside `service-b`.
- `GET /probe-once`: one request from `service-b` to `service-a`.
- `GET /combined`: aggregated report for both services.
  - Query params:
    - `targetRegions` (default `3`)
    - `rounds` (default `8`)
    - `samplesPerRound` (default `20`)
    - `concurrency` (default `10`)
    - `timeoutMs` (default `2000`)
    - `expectedRegions` (comma-separated list)
    - `includeRaw=1` (include all raw probe results)

## Manual Railway setup

1. Create one Railway project/environment.
2. Create `service-a` from `/service-a` and deploy.
3. Create `service-b` from `/service-b` and deploy.
4. In `service-b` variables, set:
   - `SERVICE_A_URL=http://${{service-a.RAILWAY_PRIVATE_DOMAIN}}:${{service-a.PORT}}`
   - `SELF_URL=http://${{service-b.RAILWAY_PRIVATE_DOMAIN}}:${{service-b.PORT}}`
   - `SERVICE_A_EXPECTED_NAME=service-a`
   - `EXPECTED_REGIONS=us-west2,us-east4,europe-west4` (edit to your regions)
5. Scale each service to your desired replicas across 3 regions (for example total `10` replicas).
6. Open `service-b` public URL at `/combined` to see aggregated output.

## What to check

- `service-a /dig` and `service-b /dig` give you per-service DNS views.
- `service-b /combined` gives one combined payload with:
  - per-service/per-region hit counts,
  - discovered replica IDs,
  - missing regions versus `EXPECTED_REGIONS`,
  - DNS snapshots from both services.

## Notes on interpretation

- Railway documents public traffic behavior: nearest region, then random replica within that region.
- Railway documents that private networking (`*.railway.internal`) bypasses edge and resolves to internal IPs.
- This probe is intended to empirically observe private-routing behavior for your exact deployment.
