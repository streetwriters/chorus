# Notesnook HA changes

This patch keeps the existing provider registry, Redis switch records, version vectors, and Asynq event queues.

- S3 client creation no longer probes synchronously or fails startup when a provider is down. A bounded background probe initializes optional `HealthReporter` state; Proxy HTTP requests refresh that state without changing the `Client` interface. The persisted routing policy is loaded from Redis after restart.
- During a zero-downtime switch, reads use the switch record's replication ID even though the replication policy is archived. A known source-only object is routed to its source; provider network failures return S3 `ServiceUnavailable` / HTTP 503. Object sync events retain a high Asynq retry limit through extended outages.
- Deletes on the promoted target increment its version and enqueue an explicit reverse-direction delete event. Completed switch records remain available in request context for this purpose, and existing replication policies are still honored. Workers act only on explicit delete events and no longer infer deletion from missing version metadata or a missing source object. If an object sync event cannot be stored, Proxy returns retryable HTTP 503 instead of silently acknowledging it.
- Query-string SigV4 GET, PUT, and multipart UploadPart are covered by the MinIO integration test. A test-only helper uses the exact AWSSDK.S3 3.7.310.8 package and Notesnook settings (`ServiceURL`, `AuthenticationRegion`, path-style, SigV4, `GetPreSignedURLAsync`).

## Verification and limits

Run Go commands with the repository's Go 1.26.6 toolchain. The MinIO integration test also needs .NET 8 and the AWSSDK.S3 3.7.310.8 NuGet package; it skips the SDK-specific case when `dotnet` is unavailable. Affected unit packages pass, and `go test ./test/minio -count=1` passes, including both MinIO-Go and Notesnook-style presigned GET, PUT, and UploadPart. `go test ./...` reaches the Go packages but the container integration packages cannot all complete because Docker cannot resolve/pull Docker Hub and GHCR images in this environment.

Asynq object events use a retry cap of `MaxInt32`, which is operationally very large but finite. Health state is an initial probe plus Proxy request feedback; it is not a periodic Worker-side health service.
