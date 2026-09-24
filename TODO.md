# Notesnook HA final pass

## Required correctness fixes

- [ ] Require completed initial discovery (`InitDone`) before `PROMOTED_WITH_BACKLOG`; add a regression proving an offline source plus outstanding work does not promote while initialization is incomplete. Run the focused switch-handler test.
- [ ] Permit recovery replication from a prior switch target after either `PROMOTED_WITH_BACKLOG` or `DONE`, retaining source storage and bucket safety checks. Test B→A recovery without deleting the switch record, a mutation during handoff, and B→C replication. Run focused policy and replication tests.
- [ ] Preserve offline-tolerant Proxy and Worker startup and persisted routing across restart. Run offline startup tests; environment-backed Compose restarts are not run unless the lab is available.
- [ ] Preserve promoted-backlog behavior: PUT to B, GET/HEAD for target-present objects, source-only GET as retryable 503, durable blocked repair, and restart behavior. Run focused routing, worker, and proxy tests.
- [ ] Preserve version safety for delayed A→B writes and deletes after newer B mutations. Review found delete tasks were unconditional; stale source deletes are now gated by the known version vector while explicit deletes with no metadata still run. Tests cover stale/equal/newer/empty vectors and a switch-time B PUT advancing the old vector.
- [ ] Preserve durable explicit delete intent; never infer deletion from missing objects or empty version metadata. Test delete at B while A is offline, A recovery, and stale-copy removal.
- [ ] Preserve multipart completion ambiguity recovery: S3 completion, enqueue failure, retry `NoSuchUpload`, HEAD reconciliation, event persistence, idempotent success. Run focused multipart retry tests.
- [ ] Keep Notesnook `AWSSDK.S3 3.7.310.8` presigned GET, PUT, and multipart UploadPart regression tests (ServiceURL, AuthenticationRegion, path style, SigV4). Run the available test harness; record environment limitations.

## Required simplifications

- [ ] Remove cached `HealthReporter`/atomic/background-probe machinery if no correctness-critical consumer exists; retain live `HealthChecker.Probe` and offline-tolerant provider construction. Adjust tests and run focused startup/health tests.
- [ ] Remove unused `ObjectSyncPayload.UploadStorage`; retain multipart upload marker and `UploadID`. Run task, router, and multipart tests.
- [ ] Deduplicate switch mutation/delete handling with one small helper where it makes state transitions easier to audit. Run replication and delete tests.

## Aggressive adversarial review

- [ ] Promotion cannot hide objects during incomplete discovery; inspect switch state and target availability boundaries.
- [ ] Audit mutations immediately before/during/after promotion, while DONE, during B→A setup, and before switch metadata removal; verify a durable path exists.
- [ ] Attack delayed old-source PUT/DELETE events after newer B PUT/DELETE/PUT; prove they cannot corrupt B.
- [ ] Check delete resurrection across outages, restarts, reverse replication, and old queued event retries.
- [ ] Walk every multipart completion crash/enqueue boundary and verify committed objects retain recoverable replication intent.
- [ ] Check Proxy, Worker, and Redis independent restarts with durable backlog/delete/recovery state.
- [ ] Check A→B→C behavior and ensure old A repair cannot corrupt/block B→C.
- [ ] Review retry policy for permanent provider errors and record any accepted limitation.

## Test log / blockers

- Initial inventory: existing patch is committed; working tree initially contained only untracked `chorus-notesnook-ha.patch` (preserve it). No `TODO.md` existed.
- `gofmt -w` completed on changed Go files.
- `git diff --check` completed cleanly.
- Focused command attempted: `go test ./pkg/s3client ./pkg/objstore ./pkg/tasks ./pkg/policy ./pkg/replication ./service/worker/handler ./service/proxy/router`; blocked before compilation because installed Go is 1.23.4 and `go.mod` requires Go 1.26.6 with a `tool` block (`go.mod:396: unknown block type: tool`).
- Broader command attempted: `go test ./... -timeout=180s`; blocked by the same Go parser error before any tests ran. Focused worker/policy/replication command was retried after the promotion guard change and blocked identically.
- Not run in this pass: broader `go test ./...`, Compose restart/outage scenarios, and the .NET 8 / AWSSDK.S3 3.7.310.8 compatibility harness. Docker is unavailable in this shell because its daemon is not running. Keep all test-related checklist items unchecked until run with the required environment.
- Final adversarial review findings: incomplete discovery is explicitly part of the promotion predicate; DONE switch metadata remains while recovery policy is installed; mutations with stale `IN_PROGRESS` request context queue reverse events; explicit delayed deletes are compared with known target versions; old switch vectors are advanced on B mutations. These boundaries have regression tests written but remain unverified because Go tests could not run. Multipart crash boundaries, real Proxy/Worker/Redis restarts, actual A→B→C draining, and SigV4 integration remain unverified in this pass.
- Accepted retry limitation: object events use `MaxInt32` retries, so permanent provider/configuration errors can remain queued and continue retrying under Asynq backoff. No permanent/transient classifier was introduced; monitor/repair policy remains an operational follow-up.
