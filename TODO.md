# Notesnook HA final pass

## Required correctness fixes

- [x] Give explicit delete tasks the exact source mutation version. Actual ordering test queues DELETE v2, processes PUT v3 first, then retries DELETE v2 and old PUT v1; target remains v3. Matching current delete reaches a local HTTP S3 stub and updates the target vector. Stale, equal, legacy-versionless, and newer-source cases are also covered. Focused worker/replication tests passed.
- [x] Retain successful multipart completion markers through bounded TTL. Tests cover enqueue failure then `NoSuchUpload`+HEAD recovery, a successfully enqueued completion whose response is treated as lost and retried, abort cleanup, TTL preservation, initiating-provider routing, and upload/key/ETag/size isolation. Focused proxy/storage/store tests passed.
- [ ] A process crash after S3 completes multipart but before Redis persists the completion receipt remains inherently ambiguous. New markers fail conservatively without the receipt; they do not treat an arbitrary HEAD object as this upload. No S3/Redis atomic commit or provider-side idempotency token exists in this architecture. The narrow case can require manual resolution; do not claim end-to-end recovery for it.

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

- [x] Final review invariant: a delayed delete can only remove the exact source version it represents. Attack: PUT v1 is copied, DELETE v2 queues, PUT v3 is physically on A and processed to B before DELETE v2 retries. Handler rejects v2 because task `FromVersion` no longer equals durable vector `From=3`; worker scenario test proves B retains v3. Matching v3 delete advances target vector, making duplicates no-ops. Legacy versionless tasks are allowed only with empty version state; with known metadata they no-op. By inspection, the same task version guard is used for forward, reverse recovery, and B→C events because each replication event has its own vector. Limit: if Redis vector state is lost, an explicit legacy task with zero version and empty vector can still execute to preserve compatibility.
- [x] Final review invariant: completed multipart retries after receipt persistence survive response loss; the receipt is Redis state and thus survives ordinary Proxy/Worker restarts. Attack: provider Complete succeeds, HEAD receipt is stored, event enqueue succeeds, response is dropped, and retry receives `NoSuchUpload`; retained marker matches HEAD ETag/size/last-modified and re-enqueues idempotently. Proxy retry test exercises both enqueue-failure recovery and a separate successful-completion/lost-response path; storage tests prove atomic marker replacement does not extend TTL or resurrect an aborted marker. Limit: crash after S3 Complete but before Redis receipt persistence cannot be distinguished from another object at the same key; new markers fail conservatively and may need manual resolution. This is not covered by a process-kill integration test.
- [ ] Concurrency review limit: a worker can read a version vector, then copy while a Proxy mutation to the same object is physically committed but its vector update has not yet run. Current object locks serialize workers but are not shared with Proxy writes. Sequential delayed-event tests pass; overlapping Proxy-write/worker-copy safety is not established. Fixing it would require extending shared object locking across Proxy and Worker and is outside this two-fix pass.
- Failure matrix: provider Complete + receipt saved + event enqueue fails => Proxy returns retryable error; receipt remains and retry gets `NoSuchUpload`, HEAD match, then re-enqueues. Provider Complete + receipt + event enqueue succeeds + HTTP response lost => same marker/HEAD path returns idempotent success. Provider Complete + crash/Redis failure before receipt => durable state cannot prove which object was completed; new markers fail conservatively. Worker delete + Redis target-vector update failure => Asynq retry repeats idempotent S3 delete then advances vector. No live process-kill test covers these crash rows.

- [x] Promotion predicate requires completed discovery, pending work, in-progress status, and an offline source; focused regression passed.
- [ ] Audit mutations immediately before/during/after promotion, while DONE, during B→A setup, and before switch metadata removal; verify a durable path exists.
- [x] Scenario test attacks delayed old-source DELETE v2 and PUT v1 after PUT v3 reaches B; B retains v3. [ ] Full PUT/DELETE/PUT/DELETE permutations across promotion, recovery, and process restarts remain inspection-only/unverified.
- [ ] Check delete resurrection across outages, restarts, reverse replication, and old queued event retries.
- [ ] Walk every multipart completion crash/enqueue boundary and verify committed objects retain recoverable replication intent.
- [ ] Check Proxy, Worker, and Redis independent restarts with durable backlog/delete/recovery state.
- [x] B→C unit coverage verifies writes fan out to the new follower while retaining reverse A repair; full A→B→C drain test remains unverified.
- [x] Reviewed retry policy. Object events retain `MaxInt32` retries; permanent errors can remain queued under Asynq backoff (accepted operational limitation, no classifier added).

## Test log / blockers

- Current pass started from committed prior patch; preserve existing untracked `chorus-notesnook-ha-review.patch` and `chorus-notesnook-ha.patch` artifacts. Added `service/worker/handler/object_handlers_order_test.go` as an untracked new regression file.

- Initial inventory: existing patch is committed; working tree initially contained only untracked `chorus-notesnook-ha.patch` (preserve it). No `TODO.md` existed.
- `go version`: `go1.27.1 darwin/arm64` (official latest stable at time of verification).
- `gofmt` and `git diff --check` completed cleanly.
- Focused command passed after final changes: `go test ./pkg/tasks ./pkg/replication ./service/worker/handler ./service/proxy/router ./pkg/storage ./pkg/store ./pkg/policy`.
- Notesnook SDK/minio command passed after final changes: `go test ./test/minio -count=1`; the AWSSDK.S3 3.7.310.8 GET, PUT, and multipart UploadPart presigned cases ran.
- Final broad command: `go test ./... -timeout=180s`. Unit/service packages through `test` passed. The full command failed in `test/versioned` after 188 seconds while pulling `ghcr.io/arttor/ceph-test:v19`; the earlier broad attempt also had `test/agent` killed after four minutes while pulling the same image. Docker itself is reachable, but the GHCR pull did not complete. No environment-backed Compose outage/restart lab was run because the repository's `chorus-ha-test/` directory has no Compose configuration.
- Focused command passed: `go test ./pkg/s3client ./pkg/objstore ./pkg/tasks ./pkg/policy ./pkg/replication ./service/worker/handler ./service/proxy/router`.
- Notesnook SDK integration passed: `go test ./test/minio -count=1` (the .NET 8 executable was available, so AWSSDK.S3 3.7.310.8 GET, PUT, and UploadPart cases ran).
- Broad suite command `go test ./... -timeout=180s` passed unit/service packages through `test`, but failed in `test/versioned` after 180 seconds while Testcontainers attempted to pull `ghcr.io/arttor/ceph-test:v19`.
- Docker daemon is reachable to Testcontainers when elevated. Compose HA restart/outage scenarios remain unrun because `chorus-ha-test/` in this repo has no Compose configuration; `docker compose ps` reports no configuration file.
- Final adversarial review findings: reordered DELETE v2 after PUT v3 is rejected by its durable mutation version; matching deletes update the target vector after physical deletion. A B→A or B→C task uses its own replication vector, while old A→B work cannot regress a newer version; other provider/restart permutations are inspection-only and the previously listed live tests remain unresolved. Multipart markers retain their origin and receipt through TTL, abort removes them, and a response-loss retry revalidates the exact completed object and persists replication intent again. Crash between provider Complete and receipt persistence remains an accepted/unresolved cross-system atomicity limitation and is deliberately conservative. Broad suite remains blocked by unavailable GHCR test image pulls.
- Accepted retry limitation: object events use `MaxInt32` retries, so permanent provider/configuration errors can remain queued and continue retrying under Asynq backoff. No permanent/transient classifier was introduced; monitor/repair policy remains an operational follow-up.
