# Notesnook HA final pass

## Required correctness fixes

- [x] Require completed initial discovery (`InitDone`) before `PROMOTED_WITH_BACKLOG`; regression covers an offline source and outstanding event with discovery incomplete. `go test ./service/worker/handler` passed.
- [x] Permit recovery replication from a prior switch target after either `PROMOTED_WITH_BACKLOG` or `DONE`, retaining source storage and bucket safety checks. Tests cover B→A with the DONE record retained, a mutation during handoff, and B→C. Focused policy and replication tests passed.
- [x] Offline provider registry construction and persisted route reconstruction tests passed in `pkg/objstore` and `pkg/policy`. [ ] Actual Proxy/Worker process restart while A is offline remains unverified; the repo has no Compose file in `chorus-ha-test` for this lab run.
- [x] Focused routing, worker, and proxy tests passed for promoted-backlog reads and writes, source-only 503 handling, durable blocked events, and multipart retry behavior. Compose restarts remain unverified.
- [x] Version-vector regression tests passed: delayed old-source writes are rejected after a B write, and stale/equal deletes are skipped while a newer source delete and explicit delete without metadata remain actionable. Full end-to-end worker race/replay testing remains unverified.
- [ ] Preserve durable explicit delete intent; never infer deletion from missing objects or empty version metadata. Test delete at B while A is offline, A recovery, and stale-copy removal.
- [x] Preserve multipart completion ambiguity recovery: S3 completion, enqueue failure, retry `NoSuchUpload`, HEAD reconciliation, event persistence, idempotent success. Focused proxy/router tests passed.
- [x] Notesnook `AWSSDK.S3 3.7.310.8` presigned GET, PUT, and multipart UploadPart regression tests passed using `ServiceURL`, `AuthenticationRegion`, path style, and SigV4 in `go test ./test/minio -count=1`.

## Required simplifications

- [x] Removed cached `HealthReporter`/atomic/background-probe machinery; retained live `HealthChecker.Probe` and offline-tolerant provider construction. Focused startup/health tests passed.
- [x] Removed unused `ObjectSyncPayload.UploadStorage`; retained the multipart upload marker and `UploadID`. Task and router tests passed.
- [x] Deduplicated switch mutation/delete handling with a small helper. Replication and delete tests passed.

## Aggressive adversarial review

- [x] Promotion predicate requires completed discovery, pending work, in-progress status, and an offline source; focused regression passed.
- [ ] Audit mutations immediately before/during/after promotion, while DONE, during B→A setup, and before switch metadata removal; verify a durable path exists.
- [ ] Attack delayed old-source PUT/DELETE events after newer B PUT/DELETE/PUT; prove they cannot corrupt B.
- [ ] Check delete resurrection across outages, restarts, reverse replication, and old queued event retries.
- [ ] Walk every multipart completion crash/enqueue boundary and verify committed objects retain recoverable replication intent.
- [ ] Check Proxy, Worker, and Redis independent restarts with durable backlog/delete/recovery state.
- [x] B→C unit coverage verifies writes fan out to the new follower while retaining reverse A repair; full A→B→C drain test remains unverified.
- [x] Reviewed retry policy. Object events retain `MaxInt32` retries; permanent errors can remain queued under Asynq backoff (accepted operational limitation, no classifier added).

## Test log / blockers

- Initial inventory: existing patch is committed; working tree initially contained only untracked `chorus-notesnook-ha.patch` (preserve it). No `TODO.md` existed.
- `go version`: `go1.27.1 darwin/arm64` (official latest stable at time of verification).
- `gofmt` and `git diff --check` completed cleanly.
- Focused command passed: `go test ./pkg/s3client ./pkg/objstore ./pkg/tasks ./pkg/policy ./pkg/replication ./service/worker/handler ./service/proxy/router`.
- Notesnook SDK integration passed: `go test ./test/minio -count=1` (the .NET 8 executable was available, so AWSSDK.S3 3.7.310.8 GET, PUT, and UploadPart cases ran).
- Broad suite command `go test ./... -timeout=180s` passed unit/service packages through `test`, but failed in `test/versioned` after 180 seconds while Testcontainers attempted to pull `ghcr.io/arttor/ceph-test:v19`.
- Docker daemon is reachable to Testcontainers when elevated. Compose HA restart/outage scenarios remain unrun because `chorus-ha-test/` in this repo has no Compose configuration; `docker compose ps` reports no configuration file.
- Final adversarial review findings: incomplete discovery is in the promotion predicate; DONE metadata remains through recovery-policy creation; mutations with stale `IN_PROGRESS` context queue reverse events; explicit delayed deletes compare known target versions; old switch vectors advance on B mutations. Unit coverage passed for the targeted boundaries. Multipart crash/restart boundary permutations, real Proxy/Worker/Redis restarts, and full A→B→C draining remain unverified.
- Accepted retry limitation: object events use `MaxInt32` retries, so permanent provider/configuration errors can remain queued and continue retrying under Asynq backoff. No permanent/transient classifier was introduced; monitor/repair policy remains an operational follow-up.
