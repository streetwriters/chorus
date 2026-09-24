package router

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

type multipartRetryReplicator struct {
	calls     atomic.Int32
	failFirst bool
}

func (r *multipartRetryReplicator) Replicate(context.Context, string, tasks.ReplicationTask) error {
	if r.calls.Add(1) == 1 && r.failFirst {
		return fmt.Errorf("simulated Redis enqueue failure")
	}
	return nil
}

func TestMultipartRetryRequiresTheCompletedObjectReceipt(t *testing.T) {
	completedAt := time.Now().UTC()
	tracked := &entity.UserUploadObject{
		Object: "object", UploadID: "upload-1", Storage: "b", CompletionTracking: true, CompletionRecorded: true,
		StartedAt: completedAt.Add(-time.Minute), CompletedETag: "original-etag", CompletedSize: 5,
		CompletedLastModified: completedAt,
	}
	valid := mclient.ObjectInfo{ETag: "original-etag", Size: 5, LastModified: completedAt}
	require.True(t, multipartRetryMatches(tracked, valid))
	valid.ETag = "newer-upload-etag"
	require.False(t, multipartRetryMatches(tracked, valid), "a later overwrite cannot be mistaken for this completed upload")
	valid.ETag = "original-etag"
	valid.Size = 6
	require.False(t, multipartRetryMatches(tracked, valid), "different object size cannot satisfy the receipt")
	tracked.CompletionRecorded = false
	require.False(t, multipartRetryMatches(tracked, valid), "new markers without a durable completion receipt cannot claim success")
	tracked.CompletedLastModified = completedAt.Add(time.Second)
	tracked.CompletionRecorded = true
	valid.Size = 5
	require.False(t, multipartRetryMatches(tracked, valid), "a different object timestamp cannot satisfy the receipt")
	legacy := &entity.UserUploadObject{StartedAt: completedAt}
	require.False(t, multipartRetryMatches(legacy, mclient.ObjectInfo{LastModified: completedAt.Add(-2 * time.Second)}))
	require.True(t, multipartRetryMatches(legacy, valid), "legacy markers retain their timestamp-based retry behavior")
}

func TestCompleteMultipartUploadCanRetryAfterQueueFailure(t *testing.T) {
	r := require.New(t)
	var completeCalls atomic.Int32
	var headCalls atomic.Int32
	completeByUpload := map[string]int{}
	completedAt := time.Now().UTC().Truncate(time.Second)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Has("location") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<LocationConstraint/>`))
			return
		}
		if req.Method == http.MethodPost && req.URL.Query().Has("uploads") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`))
			return
		}
		if req.Method == http.MethodHead {
			headCalls.Add(1)
			w.Header().Set("ETag", `"complete-etag"`)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Last-Modified", completedAt.Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return
		}
		if req.Method == http.MethodDelete && req.URL.Query().Get("uploadId") == "upload-1" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if req.Method != http.MethodPost || req.URL.Query().Get("uploadId") != "upload-1" {
			http.NotFound(w, req)
			return
		}
		completeCalls.Add(1)
		completeByUpload[req.URL.Path]++
		if completeByUpload[req.URL.Path] == 1 {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchUpload</Code><Message>already completed</Message></Error>`))
	}))
	defer backend.Close()

	redis := testutil.SetupRedis(t)
	conf := &objstore.Config{
		Main: "b",
		Storages: map[string]objstore.Storage{
			"b": {
				CommonConfig: objstore.CommonConfig{Type: dom.S3},
				S3: &s3.Storage{
					StorageAddress: s3.StorageAddress{Address: backend.URL, Provider: s3.ProviderMinIO},
					Credentials:    map[string]s3.CredentialsV4{"user": {AccessKeyID: "access", SecretAccessKey: "secret"}},
				},
			},
		},
	}
	creds, err := objstore.NewCredsSvc(t.Context(), conf, nil)
	r.NoError(err)
	clients, err := objstore.NewRegistry(t.Context(), creds, metrics.NewS3Service(false))
	r.NoError(err)
	uploads := storage.NewUploadSvc(redis)
	uploadID := entity.NewUserUploadObjectID("user", "bucket")
	limit := ratelimit.New(redis, nil)
	router := NewS3Router(clients, nil, uploads, limit).(*s3Router)
	replicator := &multipartRetryReplicator{failFirst: true}
	handler := serve(router, replicator)

	replicationID := entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User: "user", FromStorage: "a", ToStorage: "b",
	})
	initRequest := httptest.NewRequest(http.MethodPost, backend.URL+"/bucket/object?uploads", nil)
	initCtx := xctx.SetUser(initRequest.Context(), "user")
	initCtx = xctx.SetBucket(initCtx, "bucket")
	initCtx = xctx.SetObject(initCtx, "object")
	initCtx = xctx.SetMethod(initCtx, s3.CreateMultipartUpload)
	initCtx = xctx.SetRoutingPolicy(initCtx, "b")
	initCtx = xctx.SetReplications(initCtx, []entity.UniversalReplicationID{replicationID})
	initRequest = initRequest.WithContext(initCtx)
	initResponse := httptest.NewRecorder()
	handler.ServeHTTP(initResponse, initRequest)
	r.Equal(http.StatusOK, initResponse.Code, initResponse.Body.String())
	tracked, err := uploads.GetUpload(t.Context(), uploadID, "object", "upload-1")
	r.NoError(err)
	r.NotNil(tracked, "multipart initiation during ordinary replication must be recorded for completion retry recovery")
	r.Equal("b", tracked.Storage)

	requestFor := func(objectName string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, backend.URL+"/bucket/"+objectName+"?uploadId=upload-1", strings.NewReader(`<CompleteMultipartUpload/>`))
		ctx := xctx.SetUser(req.Context(), "user")
		ctx = xctx.SetBucket(ctx, "bucket")
		ctx = xctx.SetObject(ctx, objectName)
		ctx = xctx.SetMethod(ctx, s3.CompleteMultipartUpload)
		ctx = xctx.SetRoutingPolicy(ctx, "a")
		ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{replicationID})
		return req.WithContext(ctx)
	}
	request := func() *http.Request { return requestFor("object") }
	routed, _, err := router.routeMultipart(request())
	r.NoError(err)
	r.Equal("b", routed, "multipart retries stay pinned to the initiating provider")

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request())
	r.Equal(http.StatusServiceUnavailable, first.Code, "backend completion succeeds, but failed event storage must be retryable")
	tracked, err = uploads.GetUpload(t.Context(), uploadID, "object", "upload-1")
	r.NoError(err)
	r.NotNil(tracked, "keep the upload marker until the object event is durably queued")

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request())
	r.Equal(http.StatusOK, second.Code, second.Body.String())
	var completed completeMultipartUploadResult
	r.NoError(xml.Unmarshal(second.Body.Bytes(), &completed))
	r.Equal(`"complete-etag"`, completed.ETag)
	r.Equal(int32(2), completeCalls.Load(), "the retry receives NoSuchUpload after the first completion")
	r.Greater(headCalls.Load(), int32(0), "a retry must verify the final object with HEAD before treating completion as successful")
	r.Equal(int32(4), replicator.calls.Load(), "retry re-enqueues object, ACL, and tag work")
	tracked, err = uploads.GetUpload(t.Context(), uploadID, "object", "upload-1")
	r.NoError(err)
	r.NotNil(tracked, "successful completion metadata remains available through its bounded TTL")
	r.True(tracked.CompletionRecorded)
	r.Equal("complete-etag", tracked.CompletedETag)
	r.Equal(int64(5), tracked.CompletedSize)
	key, err := store.NewUserUploadStore(redis).MakeKey(uploadID)
	r.NoError(err)
	ttl := redis.TTL(t.Context(), key).Val()
	r.Greater(ttl, time.Duration(0))
	r.LessOrEqual(ttl, multipartUploadTrackingTTL)
	wrongUpload, err := uploads.GetUpload(t.Context(), uploadID, "object", "another-upload")
	r.NoError(err)
	r.Nil(wrongUpload, "a different upload ID cannot use the completion marker")
	wrongKey, err := uploads.GetUpload(t.Context(), uploadID, "another-object", "upload-1")
	r.NoError(err)
	r.Nil(wrongKey, "a different object key cannot use the completion marker")

	// Treat the successful retry response as lost. The next retry again sees
	// NoSuchUpload, verifies the same committed object, and remains idempotent.
	lostResponse := httptest.NewRecorder()
	handler.ServeHTTP(lostResponse, request())
	r.Equal(http.StatusOK, lostResponse.Code)
	r.Equal(int32(3), completeCalls.Load())
	r.Equal(int32(7), replicator.calls.Load(), "response-loss retry recreates durable replication intent")
	tracked, err = uploads.GetUpload(t.Context(), uploadID, "object", "upload-1")
	r.NoError(err)
	r.NotNil(tracked)

	abort := httptest.NewRequest(http.MethodDelete, backend.URL+"/bucket/object?uploadId=upload-1", nil)
	abortCtx := xctx.SetUser(abort.Context(), "user")
	abortCtx = xctx.SetBucket(abortCtx, "bucket")
	abortCtx = xctx.SetObject(abortCtx, "object")
	abortCtx = xctx.SetRoutingPolicy(abortCtx, "a")
	abort = abort.WithContext(abortCtx)
	_, _, _, err = router.abortMultipartUpload(abort)
	r.NoError(err)
	tracked, err = uploads.GetUpload(t.Context(), uploadID, "object", "upload-1")
	r.NoError(err)
	r.Nil(tracked, "explicit abort removes the retained completion marker")

	// Exercise the exact response-loss sequence independently: provider
	// completion and replication enqueue succeed, then the client retries.
	init2 := httptest.NewRequest(http.MethodPost, backend.URL+"/bucket/object-2?uploads", nil)
	init2Ctx := xctx.SetUser(init2.Context(), "user")
	init2Ctx = xctx.SetBucket(init2Ctx, "bucket")
	init2Ctx = xctx.SetObject(init2Ctx, "object-2")
	init2Ctx = xctx.SetMethod(init2Ctx, s3.CreateMultipartUpload)
	init2Ctx = xctx.SetRoutingPolicy(init2Ctx, "b")
	init2Ctx = xctx.SetReplications(init2Ctx, []entity.UniversalReplicationID{replicationID})
	init2 = init2.WithContext(init2Ctx)
	init2Response := httptest.NewRecorder()
	handler.ServeHTTP(init2Response, init2)
	r.Equal(http.StatusOK, init2Response.Code, init2Response.Body.String())

	firstCompletionResponseLost := httptest.NewRecorder()
	handler.ServeHTTP(firstCompletionResponseLost, requestFor("object-2"))
	r.Equal(http.StatusOK, firstCompletionResponseLost.Code)
	// Discard firstCompletionResponseLost to simulate the network dropping 200.

	retryAfterLostResponse := httptest.NewRecorder()
	handler.ServeHTTP(retryAfterLostResponse, requestFor("object-2"))
	r.Equal(http.StatusOK, retryAfterLostResponse.Code, retryAfterLostResponse.Body.String())
	r.Equal(int32(5), completeCalls.Load())
	r.Equal(int32(13), replicator.calls.Load())
	tracked, err = uploads.GetUpload(t.Context(), uploadID, "object-2", "upload-1")
	r.NoError(err)
	r.NotNil(tracked, "successful completion receipt survives event enqueue and a lost client response")
}
