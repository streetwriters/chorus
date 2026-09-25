package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	proxyr "github.com/clyso/chorus/service/proxy/router"
)

type mutationGateCreds struct {
	objstore.CredsService
	storages map[string]dom.StorageType
}

func (c mutationGateCreds) ValidateReplicationID(entity.UniversalReplicationID) error { return nil }
func (c mutationGateCreds) Storages() map[string]dom.StorageType                      { return c.storages }
func (mutationGateCreds) HasUser(string, string) error                                { return nil }

type mutationGateCommon struct{ objstore.Common }

func (mutationGateCommon) BucketExists(context.Context, string) (bool, error) { return true, nil }

type mutationGateClients struct {
	objstore.Clients
	common objstore.Common
}

func (c mutationGateClients) AsCommon(context.Context, string, string) (objstore.Common, error) {
	return c.common, nil
}

type mutationGateQueue struct {
	tasks.QueueService
	mu          sync.Mutex
	items       []any
	blockCreate bool
	createStart chan struct{}
	createGo    chan struct{}
}

func (q *mutationGateQueue) EnqueueTask(ctx context.Context, task any) error {
	if _, ok := task.(*tasks.BucketCreatePayload); ok && q.blockCreate {
		close(q.createStart)
		select {
		case <-q.createGo:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	q.mu.Lock()
	q.items = append(q.items, task)
	q.mu.Unlock()
	return nil
}

type mutationGateProxyRouter struct {
	started chan struct{}
	finish  chan struct{}
	block   bool
}

func (r *mutationGateProxyRouter) Route(req *http.Request) (*http.Response, []tasks.ReplicationTask, string, bool, error) {
	if r.block {
		close(r.started)
		<-r.finish
		r.block = false
	}
	object := dom.Object{Bucket: xctx.GetBucket(req.Context()), Name: xctx.GetObject(req.Context())}
	var taskList []tasks.ReplicationTask
	if xctx.GetMethod(req.Context()) == s3.DeleteObject {
		taskList = []tasks.ReplicationTask{&tasks.ObjectSyncPayload{Object: object, Deleted: true}}
	}
	return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody},
		taskList, xctx.GetRoutingPolicy(req.Context()), false, nil
}

func TestTopologyPolicyChangeConflictsWithActiveDeleteAndBlocksNewDelete(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	gate := store.NewBucketMutationGate(redis, time.Second)
	policySvc := policy.NewService(redis, nil, "b")
	queue := &mutationGateQueue{createStart: make(chan struct{}), createGo: make(chan struct{})}
	creds := mutationGateCreds{storages: map[string]dom.StorageType{"b": dom.S3, "c": dom.S3}}
	common := mutationGateCommon{}
	h := &policyHandlers{
		credsSvc: creds, clients: mutationGateClients{common: common}, queueSvc: queue,
		policySvc: policySvc, userLocker: store.NewUserLocker(redis, 0), mutationGate: gate,
	}
	add := func(bucket string) error {
		_, err := h.AddReplication(t.Context(), &pb.AddReplicationRequest{Id: &pb.ReplicationID{
			User: "user", FromStorage: "b", ToStorage: "c", FromBucket: &bucket, ToBucket: &bucket,
		}})
		return err
	}
	proxyRouter := &mutationGateProxyRouter{started: make(chan struct{}), finish: make(chan struct{}), block: true}
	identity := func(next http.Handler) http.Handler { return next }
	proxy, err := proxyr.New(proxyr.Config{
		Storages: map[dom.StorageType]proxyr.StorageProxy{
			dom.S3: {
				Router: proxyRouter, Replicator: replication.NewS3(queue, meta.NewVersionService(redis), policySvc),
				ObjectLocker: store.NewObjectLocker(redis, 0), ReqParseMiddleware: identity,
			},
		},
		LogMiddleware: identity, PolicyMiddleware: proxyr.PolicyMiddleware(policySvc, gate),
	})
	r.NoError(err)
	deleteRequest := func(bucket string) *http.Request {
		req := httptest.NewRequest(http.MethodDelete, "http://chorus/"+bucket+"/foo", nil)
		ctx := xctx.SetUser(req.Context(), "user")
		ctx = xctx.SetBucket(ctx, bucket)
		ctx = xctx.SetObject(ctx, "foo")
		ctx = xctx.SetMethod(ctx, s3.DeleteObject)
		return req.WithContext(ctx)
	}

	deleteDone := make(chan struct{})
	go func() {
		proxy.ServeHTTP(httptest.NewRecorder(), deleteRequest("bucket-active"))
		close(deleteDone)
	}()
	select {
	case <-proxyRouter.started:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not acquire its mutation lease and enter the provider")
	}
	err = add("bucket-active")
	r.ErrorIs(err, dom.ErrBucketHasActiveMutations)
	r.Equal(codes.Aborted, status.Code(convertApiError(t.Context(), err)))
	if _, err := policySvc.GetReplicationPolicyInfoExtended(t.Context(), entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "b", FromBucket: "bucket-active", ToStorage: "c", ToBucket: "bucket-active",
	})); !errors.Is(err, dom.ErrNotFound) {
		r.FailNow("conflicting topology must not install its replication policy", "%v", err)
	}
	close(proxyRouter.finish)
	select {
	case <-deleteDone:
	case <-time.After(5 * time.Second):
		t.Fatal("DELETE did not finish")
	}
	r.NoError(add("bucket-active"), "retry after active mutation completes must install B->C")

	queue.blockCreate = true
	queue.createStart = make(chan struct{})
	queue.createGo = make(chan struct{})
	addDone := make(chan error, 1)
	go func() { addDone <- add("bucket-topology") }()
	select {
	case <-queue.createStart:
	case <-time.After(5 * time.Second):
		t.Fatal("replication policy add did not reach its blocked enqueue")
	}
	blocked := httptest.NewRecorder()
	proxy.ServeHTTP(blocked, deleteRequest("bucket-topology"))
	r.Equal(http.StatusServiceUnavailable, blocked.Code)
	r.NotContains(blocked.Body.String(), "TopologyChangeInProgress", "S3 clients should not see control-plane details")
	readReq := deleteRequest("bucket-topology")
	readCtx := xctx.SetMethod(readReq.Context(), s3.GetObject)
	readReq = readReq.WithContext(readCtx)
	read := httptest.NewRecorder()
	proxy.ServeHTTP(read, readReq)
	r.Equal(http.StatusNoContent, read.Code, "reads continue while topology owns the gate")
	close(queue.createGo)
	r.NoError(<-addDone)
	queue.blockCreate = false
	retried := httptest.NewRecorder()
	proxy.ServeHTTP(retried, deleteRequest("bucket-topology"))
	r.Equal(http.StatusNoContent, retried.Code, retried.Body.String())

	queue.mu.Lock()
	defer queue.mu.Unlock()
	found := false
	for _, item := range queue.items {
		if objectTask, ok := item.(*tasks.ObjectSyncPayload); ok && objectTask.ID.FromStorage() == "b" && objectTask.ID.ToStorage() == "c" {
			found = true
		}
	}
	r.True(found, "retry after topology installation must enqueue the delete for the new follower")
}

func TestTopologyConflictErrorIsManagementConflict(t *testing.T) {
	err := convertApiError(t.Context(), fmt.Errorf("%w: retry when the bucket is idle", dom.ErrBucketHasActiveMutations))
	r := require.New(t)
	r.Equal(codes.Aborted, status.Code(err))
	r.Contains(status.Convert(err).Message(), "BucketHasActiveMutations")
	r.Contains(status.Convert(err).Message(), "retry when the bucket is idle")
}
