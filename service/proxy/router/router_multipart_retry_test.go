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

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/storage"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

type multipartRetryReplicator struct{ calls atomic.Int32 }

func (r *multipartRetryReplicator) Replicate(context.Context, string, tasks.ReplicationTask) error {
	if r.calls.Add(1) == 1 {
		return fmt.Errorf("simulated Redis enqueue failure")
	}
	return nil
}

func TestCompleteMultipartUploadCanRetryAfterQueueFailure(t *testing.T) {
	r := require.New(t)
	var completeCalls atomic.Int32
	var headCalls atomic.Int32
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
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			return
		}
		if req.Method != http.MethodPost || req.URL.Query().Get("uploadId") != "upload-1" {
			http.NotFound(w, req)
			return
		}
		if completeCalls.Add(1) == 1 {
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
	router := NewS3Router(clients, nil, uploads, limit)
	replicator := &multipartRetryReplicator{}
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

	request := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, backend.URL+"/bucket/object?uploadId=upload-1", strings.NewReader(`<CompleteMultipartUpload/>`))
		ctx := xctx.SetUser(req.Context(), "user")
		ctx = xctx.SetBucket(ctx, "bucket")
		ctx = xctx.SetObject(ctx, "object")
		ctx = xctx.SetMethod(ctx, s3.CompleteMultipartUpload)
		ctx = xctx.SetRoutingPolicy(ctx, "b")
		ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{replicationID})
		return req.WithContext(ctx)
	}

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
	r.Nil(tracked, "upload metadata is cleared after the event is durably queued")
}
