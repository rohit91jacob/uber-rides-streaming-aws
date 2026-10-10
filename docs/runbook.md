# Runbook

Exit codes of `uber-rides`: `0` ok, `1` unexpected error, `2` usage/config/input error,
`3` a data-quality gate failed, `4` stream reconciliation failed, `5` the state machine failed
for a non-quality reason. Logs are JSON lines on stderr (`UBER_LOG_FORMAT=text` for humans).

## Daily batch (state machine)

Locally: `uber-rides batch --month YYYY-MM` runs `src/uber_rides/orchestration/daily_batch.asl.json`
through the local interpreter; on AWS, EventBridge Scheduler starts the same definition in
Step Functions every day at 06:30 New York time. The path taken is printed and saved to
`<data>/ops/batch_runs/latest.json` (`path`, per-state `result` and `errors`).

| Failed state | Meaning | What to do |
|---|---|---|
| `ResolveMonth` | No HVFHV file in the last 8 months, or the CDN is down | `uber-rides latest-month`; check <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> |
| `IngestMonth` -> `BatchFailed` | Download failed after retries, truncated file, or `SchemaError` (TLC changed columns) | Rerun (downloads are atomic and conditional). For a schema change update `EXPECTED_COLUMNS` in `sources/tlc.py` and the loaders |
| `QualityGateFailed` | An error-level check failed | `uber-rides dq --month M --layer <layer>` prints the failing checks; details in `<data>/ops/dq/<layer>.json` and `uber_lake_ops.dq_results`. Nothing downstream of the gate ran, so readers still see the previous snapshot |
| `LoadLake` | Iceberg commit failed | Rerun: a month is replaced in one transaction, so a failed load leaves the previous snapshot intact |
| `Transform` | `dbt build` model or test failed | `uber-rides transform build` and read the dbt log; `dbt/target/run_results.json` lists the failing node |

Rerunning is always safe:

* `ingest`: conditional GET / sha256 manifest; an unchanged file is not re-copied.
* `load`: deletes and appends `source_month = M` in one Iceberg transaction (same rows every time).
* `load-events`: skips landing objects already in `ride_events`.
* `batch` without `--force` skips ingest + load for months already in `ops/batch_state.json`.
* `stream`: the dedupe table rejects every event it has seen; counters do not move.

## Data-quality gates

| Layer | Error-level checks (stop the run) | Warnings |
|---|---|---|
| landing | licensee present, `request_datetime` never null | on-scene nulls > 1%, pickups outside the month > 0.1%, missing days |
| lake | trips + quarantine = raw licensee rows; unique `trip_id`; non-null keys; zones 1-265; quarantine <= 1%; 265 zones; full month of weather | unknown zones > 10%, fares <= 0 > 1%, pickup before request > 2% |
| stream | DynamoDB counters = batch recount; accepted = distinct events; DLQ = injected malformed; every event's trip exists; known event types | (skipped when no replay has run) |
| marts | additive marts sum to the trip count; rates in [0, 1]; every non-negative wait in one bucket | driver pay 40-120% of fare |

Observed in June 2026 (real data): 724,322 unknown-zone trips (4.7%), 7,920 fares <= 0
(0.05%), 185,244 pickups before the request time (1.21%, scheduled rides), 0 quarantined.
`dbt build` also warns on 2 trips logged at more than 1,000 miles.

Set `UBER_DQ_FAIL_ON_ERROR=false` to record failures without stopping (not recommended).

## Streaming path

* **Records in the DLQ** (`reason` field): `malformed_json`, `schema_violation`,
  `zone_out_of_range`, `event_id_mismatch`, `unsupported_schema_version` are producer bugs,
  fix the producer and replay; `batch_retries_exhausted` means a whole batch failed
  `MaximumRetryAttempts` times (DynamoDB throttling or an outage). Replay the shard range from
  the message's `sequence_number`: the dedupe table makes that safe.
* **Iterator age alarm**: the consumer is behind. Check throttles on the two DynamoDB tables and
  the Lambda's reserved concurrency; raise `parallelization_factor` or shards.
* **Counters disagree with batch** (`stream_counters_match_batch` fails): look for a
  `failed_from` in the consumer logs without a later successful retry, or a TTL that expired
  dedupe items before a very late replay (`UBER_DEDUPE_TTL_DAYS`).

Local emulation note: in `UBER_AWS_MODE=moto` a tiny shim (`streaming/moto_shim.py`) makes
moto's `TransactWriteItems` evaluate conditions before writing instead of deep-copying every
table per item. Semantics are unchanged. Even so the emulated path runs at roughly 100 events/s
on a laptop, so replays are sampled; this says nothing about AWS throughput.

## Scheduled refresh (GitHub Actions)

`refresh.yml` runs on the 25th of each month (and on demand). It resolves the newest TLC month,
runs the batch with a 2% sampled replay of one hour, verifies against the TLC reports for that
month and the month before, and publishes `site/` to GitHub Pages. A failed scheduled run opens
(or comments on) the issue **"Scheduled refresh is failing"**.

1. Open the failed run, find the first red step.
2. `Daily-batch state machine` exit 3: a gate failed on new data. Read the step log for the
   failing check. If the TLC changed something legitimately (for example a new fee column),
   adjust the check and document it here; otherwise wait for the TLC to republish.
3. Download errors: rerun the workflow; the TLC CDN and nyc.gov occasionally return 403/5xx.
   nyc.gov rejects user agents that do not start with `Mozilla/5.0` (`UBER_HTTP_USER_AGENT`).
4. Close the issue once a run is green.

`keepalive.yml` re-enables both scheduled workflows twice a month so GitHub does not disable
them after 60 days of inactivity.

## Infrastructure

`infra/` has never been applied by the author. To deploy into your own account: create a state
bucket, `terraform init -backend-config=environments/dev.s3.tfbackend`, review
`terraform plan -var-file=environments/dev.tfvars`, push the batch image
(`docker build -t <ecr-url>:<tag> .`) and set `batch_image_uri`. The `deploy` workflow does the
same through OIDC once the repository variables `AWS_DEPLOY_ROLE_ARN` and `TF_STATE_BUCKET` are
set. The state bucket must be named `uber-rides-tfstate-*` (the deploy role's state access is
scoped to that prefix). The first apply, and any later change to the deploy role or to the
workload permissions boundary (`<name>-workload-boundary`), must be run by an account
administrator: the deploy role is explicitly denied from modifying its own policies or trust,
from attaching managed policies, from creating, editing or deleting the boundary policy, and from
removing a permissions boundary from any role.

Workload roles (consumer, batch-step, Step Functions, Scheduler) live under the IAM path
`/<name>/` and all carry that boundary. The deploy role can create a role or change its inline
policies only under that path and only when the role has the boundary
(`iam:PermissionsBoundary` condition), and can pass each role only to its own service
(`iam:PassedToService`: Lambda, Step Functions, Scheduler). So a widened inline policy is still
capped at the boundary: this project's stream, tables, queues, lake and Athena buckets, Glue
databases, Athena workgroup, KMS key, log groups and X-Ray, with no IAM actions. The boundary does
not stop the deploy role from using the project resources it already manages directly, and none of
this has been exercised against a real account.
Activate the `project` cost-allocation tag so the budget filter matches.
