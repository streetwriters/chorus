package router

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/clyso/chorus/service/worker/copy"
	workerhandler "github.com/clyso/chorus/service/worker/handler"
)

type deleteRaceQueue struct {
	mu    sync.Mutex
	tasks []any
}

func (q *deleteRaceQueue) EnqueueTask(_ context.Context, task any) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.tasks = append(q.tasks, task)
	return nil
}
func (*deleteRaceQueue) UnprocessedCount(context.Context, bool, ...string) (int, error) {
	return 0, nil
}
func (*deleteRaceQueue) IsPaused(context.Context, string) (bool, error) { return false, nil }
func (*deleteRaceQueue) Resume(context.Context, string) error           { return nil }
func (*deleteRaceQueue) Pause(context.Context, string) error            { return nil }
func (*deleteRaceQueue) Delete(context.Context, string, bool) error     { return nil }
func (*deleteRaceQueue) Stats(context.Context, string) (*tasks.QueueStats, error) {
	return &tasks.QueueStats{}, nil
}

type deleteRaceCopy struct {
	mu          *sync.Mutex
	objects     map[string]string
	started     chan struct{}
	allowFinish chan struct{}
	calls       int
}

func (c *deleteRaceCopy) GetVersionInfo(context.Context, string, copy.File) ([]entity.ObjectVersionInfo, error) {
	return nil, nil
}
func (c *deleteRaceCopy) ClearDestination(context.Context, string, copy.File, bool) error { return nil }
func (c *deleteRaceCopy) GetLastMigratedVersionInfo(context.Context, string, copy.File) (entity.ObjectVersionInfo, error) {
	return entity.ObjectVersionInfo{}, nil
}
func (c *deleteRaceCopy) CopyObject(_ context.Context, _ string, from, to copy.File) error {
	close(c.started)
	<-c.allowFinish
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.objects[to.Storage+"/"+to.Bucket+"/"+to.Name] = c.objects[from.Storage+"/"+from.Bucket+"/"+from.Name]
	return nil
}
func (*deleteRaceCopy) CopyACLs(context.Context, string, copy.File, copy.File) error { return nil }

type deleteRaceRouter struct {
	objects          map[string]string
	mu               *sync.Mutex
	deleteCommitted  chan struct{}
	allowReturn      chan struct{}
	blockAfterDelete bool
}

func (r *deleteRaceRouter) Route(_ *http.Request) (*http.Response, []tasks.ReplicationTask, string, bool, error) {
	r.mu.Lock()
	delete(r.objects, "b/bucket/key")
	r.mu.Unlock()
	close(r.deleteCommitted)
	if r.blockAfterDelete {
		<-r.allowReturn
	}
	return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))},
		[]tasks.ReplicationTask{&tasks.ObjectSyncPayload{Object: dom.Object{Bucket: "bucket", Name: "key"}, Deleted: true}}, "b", false, nil
}

func TestProxyDeleteSerializesWithWorkerObjectCopy(t *testing.T) {
	t.Run("worker owns lock first", func(t *testing.T) {
		runProxyDeleteCopyRace(t, true)
	})
	t.Run("proxy delete owns lock first", func(t *testing.T) {
		runProxyDeleteCopyRace(t, false)
	})
}

func runProxyDeleteCopyRace(t *testing.T, workerFirst bool) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	versions := meta.NewVersionService(redis)
	object := dom.Object{Bucket: "bucket", Name: "key"}
	replID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	_, err := versions.IncrementObj(t.Context(), replID, object, meta.Destination{Storage: "a", Bucket: "bucket"})
	r.NoError(err)

	objectsMu := &sync.Mutex{}
	objects := map[string]string{"a/bucket/key": "stale", "b/bucket/key": "old"}
	copySvc := &deleteRaceCopy{mu: objectsMu, objects: objects, started: make(chan struct{}), allowFinish: make(chan struct{})}
	locker := store.NewObjectLocker(redis, time.Second)
	limiter := ratelimit.New(redis, nil)
	worker := workerhandler.New(nil, nil, nil, versions, copySvc, nil, nil, limiter, nil, locker, nil, nil, nil)
	workerTask := &tasks.ObjectSyncPayload{Object: object}
	workerTask.SetReplicationID(replID)
	workerPayload, err := json.Marshal(workerTask)
	r.NoError(err)
	workerContext := t.Context()
	startWorker := func() chan error {
		done := make(chan error, 1)
		go func() {
			done <- worker.HandleObjectSync(workerContext, asynq.NewTask(tasks.TypeObjectSync, workerPayload))
		}()
		return done
	}

	queue := &deleteRaceQueue{}
	proxyRouter := &deleteRaceRouter{
		objects: objects, mu: objectsMu, deleteCommitted: make(chan struct{}), allowReturn: make(chan struct{}),
		blockAfterDelete: !workerFirst,
	}
	proxy := serve(proxyRouter, replication.NewS3(queue, versions, nil), locker)
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusDone,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	switchInfo.SetReplicationID(replID)
	proxyCtx := xctx.SetUser(context.Background(), "user")
	proxyCtx = xctx.SetBucket(proxyCtx, "bucket")
	proxyCtx = xctx.SetObject(proxyCtx, "key")
	proxyCtx = xctx.SetMethod(proxyCtx, s3.DeleteObject)
	proxyCtx = xctx.SetRoutingPolicy(proxyCtx, "b")
	proxyCtx = xctx.SetCompletedZeroDowntime(proxyCtx, switchInfo)
	locks := deleteObjectLockIDs(proxyCtx)
	lockedStorages := map[string]bool{}
	for _, id := range locks {
		lockedStorages[id.Storage] = true
	}
	r.True(lockedStorages["a"], "completed-switch DELETE locks the potential reverse target even before B->A policy is visible")
	r.True(lockedStorages["b"], "completed-switch DELETE locks the active/old-repair destination")
	proxyReq := httptest.NewRequest(http.MethodDelete, "http://chorus/bucket/key", nil).WithContext(proxyCtx)
	proxyDone := make(chan struct{})
	startProxy := func() {
		go func() {
			proxy.ServeHTTP(httptest.NewRecorder(), proxyReq)
			close(proxyDone)
		}()
	}

	if workerFirst {
		workerDone := startWorker()
		select {
		case <-copySvc.started:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not enter physical copy")
		}
		startProxy()
		select {
		case <-proxyRouter.deleteCommitted:
			t.Fatal("Proxy reached S3 DELETE while Worker still owned the object lock")
		case <-time.After(100 * time.Millisecond):
		}
		close(copySvc.allowFinish)
		select {
		case <-proxyRouter.deleteCommitted:
		case <-time.After(5 * time.Second):
			t.Fatal("Proxy DELETE did not proceed after Worker released the object lock")
		}
		r.NoError(<-workerDone)
	} else {
		startProxy()
		select {
		case <-proxyRouter.deleteCommitted:
		case <-time.After(5 * time.Second):
			t.Fatal("Proxy DELETE did not commit")
		}
		workerDone := startWorker()
		select {
		case <-copySvc.started:
			t.Fatal("Worker began copying while Proxy DELETE owned the object lock")
		case <-time.After(100 * time.Millisecond):
		}
		close(proxyRouter.allowReturn)
		r.NoError(<-workerDone)
	}
	select {
	case <-proxyDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Proxy DELETE did not complete")
	}
	objectsMu.Lock()
	r.NotContains(objects, "b/bucket/key", "physical target must remain deleted after both operations settle")
	objectsMu.Unlock()
	vector, err := versions.GetObj(t.Context(), replID, object)
	r.NoError(err)
	r.Equal(meta.Version{From: 1, To: 2}, vector)
	queue.mu.Lock()
	defer queue.mu.Unlock()
	r.Len(queue.tasks, 1, "the delete has a durable reverse repair intent")
	deleteTask := queue.tasks[0].(*tasks.ObjectSyncPayload)
	r.True(deleteTask.Deleted)
	r.Equal("b", deleteTask.ID.FromStorage())
	r.Equal("a", deleteTask.ID.ToStorage())
	if !workerFirst {
		copySvc.mu.Lock()
		defer copySvc.mu.Unlock()
		r.Zero(copySvc.calls, "Worker must read post-delete version state and skip stale copy")
	}
}
