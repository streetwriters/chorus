package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/clyso/chorus/service/worker/copy"
)

type orderedObjectQueue struct{ tasks []any }

func (q *orderedObjectQueue) EnqueueTask(_ context.Context, task any) error {
	q.tasks = append(q.tasks, task)
	return nil
}
func (*orderedObjectQueue) UnprocessedCount(context.Context, bool, ...string) (int, error) {
	return 0, nil
}
func (*orderedObjectQueue) IsPaused(context.Context, string) (bool, error) { return false, nil }
func (*orderedObjectQueue) Resume(context.Context, string) error           { return nil }
func (*orderedObjectQueue) Pause(context.Context, string) error            { return nil }
func (*orderedObjectQueue) Delete(context.Context, string, bool) error     { return nil }
func (*orderedObjectQueue) Stats(context.Context, string) (*tasks.QueueStats, error) {
	return &tasks.QueueStats{}, nil
}

type orderedCopySvc struct {
	mu      sync.Mutex
	objects map[string]string
}

type switchRepairPolicies struct {
	policies map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended
}

func (p *switchRepairPolicies) ListBucketReplicationsInfo(context.Context, string) (map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended, error) {
	return p.policies, nil
}

func (c *orderedCopySvc) GetVersionInfo(context.Context, string, copy.File) ([]entity.ObjectVersionInfo, error) {
	return nil, nil
}
func (c *orderedCopySvc) ClearDestination(context.Context, string, copy.File, bool) error { return nil }
func (c *orderedCopySvc) GetLastMigratedVersionInfo(context.Context, string, copy.File) (entity.ObjectVersionInfo, error) {
	return entity.ObjectVersionInfo{}, nil
}
func (c *orderedCopySvc) CopyObject(_ context.Context, _ string, from, to copy.File) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.objects[to.Storage+"/"+to.Bucket+"/"+to.Name] = c.objects[from.Storage+"/"+from.Bucket+"/"+from.Name]
	return nil
}
func (*orderedCopySvc) CopyACLs(context.Context, string, copy.File, copy.File) error { return nil }

func TestDelayedDeleteV2CannotDeletePutV3CopiedFirst(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	versions := meta.NewVersionService(redis)
	replID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	ctx := xctx.SetBucket(context.Background(), "bucket")
	ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{replID})
	queue := &orderedObjectQueue{}
	replicator := replication.NewS3(queue, versions, nil)
	object := dom.Object{Bucket: "bucket", Name: "key"}

	copySvc := &orderedCopySvc{objects: map[string]string{"a/bucket/key": "v1"}}
	worker := &svc{
		versionSvc:   versions,
		copySvc:      copySvc,
		limit:        ratelimit.New(redis, nil),
		objectLocker: store.NewObjectLocker(redis, time.Second),
	}
	process := func(task *tasks.ObjectSyncPayload) {
		t.Helper()
		task.SetReplicationID(replID)
		payload, err := json.Marshal(task)
		r.NoError(err)
		r.NoError(worker.HandleObjectSync(ctx, asynq.NewTask(tasks.TypeObjectSync, payload)))
	}

	r.NoError(replicator.Replicate(ctx, "a", &tasks.ObjectSyncPayload{Object: object})) // PUT v1
	putV1 := queue.tasks[len(queue.tasks)-1].(*tasks.ObjectSyncPayload)
	process(putV1)
	r.Equal("v1", copySvc.objects["b/bucket/key"])

	r.NoError(replicator.Replicate(ctx, "a", &tasks.ObjectSyncPayload{Object: object, Deleted: true})) // DELETE v2
	deleteV2 := queue.tasks[len(queue.tasks)-1].(*tasks.ObjectSyncPayload)
	r.Equal(int64(2), deleteV2.FromVersion)
	r.NoError(replicator.Replicate(ctx, "a", &tasks.ObjectSyncPayload{Object: object})) // PUT v3
	putV3 := queue.tasks[len(queue.tasks)-1].(*tasks.ObjectSyncPayload)
	r.Equal(int64(3), putV3.FromVersion)
	copySvc.objects["a/bucket/key"] = "v3"
	process(putV3)
	process(deleteV2) // delayed retry after newer put reached target
	process(putV1)    // delayed old PUT retry after both newer mutations

	r.Equal("v3", copySvc.objects["b/bucket/key"], "old DELETE and PUT events must not erase or overwrite v3")
	vector, err := versions.GetObj(ctx, replID, object)
	r.NoError(err)
	r.Equal(meta.Version{From: 3, To: 3}, vector)
}

func TestPromotedSourceRepairFansOutToActiveTargetFollower(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	versions := meta.NewVersionService(redis)
	aToB := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	bToCPolicy := entity.BucketReplicationPolicy{
		User: "user", FromStorage: "b", FromBucket: "bucket", ToStorage: "c", ToBucket: "bucket",
	}
	bToC := entity.UniversalFromBucketReplication(bToCPolicy)
	object := dom.Object{Bucket: "bucket", Name: "late-repair"}
	ctx := xctx.SetBucket(context.Background(), "bucket")
	_, err := versions.IncrementObj(ctx, aToB, object, meta.Destination{Storage: "a", Bucket: "bucket"})
	r.NoError(err)
	queue := &orderedObjectQueue{}
	copySvc := &orderedCopySvc{objects: map[string]string{"a/bucket/late-repair": "repaired"}}
	switchInfo := entity.ReplicationSwitchInfo{LastStatus: entity.StatusPromotedWithBacklog}
	switchInfo.SetReplicationID(aToB)
	aToBPolicy := entity.BucketReplicationPolicy{User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket"}
	worker := &svc{
		versionSvc:   versions,
		copySvc:      copySvc,
		queueSvc:     queue,
		limit:        ratelimit.New(redis, nil),
		objectLocker: store.NewObjectLocker(redis, time.Second),
		replicationPolicySvc: &switchRepairPolicies{
			policies: map[entity.BucketReplicationPolicy]entity.ReplicationStatusExtended{
				aToBPolicy: {ReplicationStatus: &entity.ReplicationStatus{IsArchived: true}, Switch: &switchInfo},
				bToCPolicy: {ReplicationStatus: &entity.ReplicationStatus{}, Switch: &switchInfo},
			},
		},
	}

	aToBTask := &tasks.ObjectSyncPayload{Object: object, FromVersion: 1}
	aToBTask.SetReplicationID(aToB)
	payload, err := json.Marshal(aToBTask)
	r.NoError(err)
	r.NoError(worker.HandleObjectSync(ctx, asynq.NewTask(tasks.TypeObjectSync, payload)))
	r.Equal("repaired", copySvc.objects["b/bucket/late-repair"])
	r.Len(queue.tasks, 1, "repair must durably enqueue the active B-to-C edge")
	bToCTask, ok := queue.tasks[0].(*tasks.ObjectSyncPayload)
	r.True(ok)
	r.Equal(bToC.AsString(), bToCTask.ID.AsString())
	r.Equal(int64(1), bToCTask.FromVersion)

	payload, err = json.Marshal(bToCTask)
	r.NoError(err)
	r.NoError(worker.HandleObjectSync(ctx, asynq.NewTask(tasks.TypeObjectSync, payload)))
	r.Equal("repaired", copySvc.objects["c/bucket/late-repair"], "downstream follower receives delayed repaired object")
}

func TestMatchingVersionedDeleteRemovesTarget(t *testing.T) {
	r := require.New(t)
	var deletes int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Has("location") {
			_, _ = w.Write([]byte(`<LocationConstraint/>`))
			return
		}
		if req.Method == http.MethodDelete {
			deletes++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<DeleteResult/>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	redis := testutil.SetupRedis(t)
	conf := &objstore.Config{Main: "a", Storages: map[string]objstore.Storage{}}
	for _, name := range []string{"a", "b"} {
		conf.Storages[name] = objstore.Storage{
			CommonConfig: objstore.CommonConfig{Type: dom.S3},
			S3: &s3.Storage{
				StorageAddress: s3.StorageAddress{Address: backend.URL, Provider: s3.ProviderMinIO},
				Credentials:    map[string]s3.CredentialsV4{"user": {AccessKeyID: "access", SecretAccessKey: "secret"}},
			},
		}
	}
	creds, err := objstore.NewCredsSvc(t.Context(), conf, nil)
	r.NoError(err)
	clients, err := objstore.NewRegistry(t.Context(), creds, metrics.NewS3Service(false))
	r.NoError(err)
	versions := meta.NewVersionService(redis)
	replID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	object := dom.Object{Bucket: "bucket", Name: "key"}
	ctx := xctx.SetBucket(context.Background(), "bucket")
	_, err = versions.IncrementObj(ctx, replID, object, meta.Destination{Storage: "a", Bucket: "bucket"})
	r.NoError(err)
	version, err := versions.IncrementObj(ctx, replID, object, meta.Destination{Storage: "a", Bucket: "bucket"})
	r.NoError(err)
	r.NoError(versions.UpdateIfGreater(ctx, replID, object, meta.Destination{Storage: "b", Bucket: "bucket"}, 1))

	worker := &svc{
		clients:      clients,
		versionSvc:   versions,
		limit:        ratelimit.New(redis, nil),
		objectLocker: store.NewObjectLocker(redis, time.Second),
	}
	payload := &tasks.ObjectSyncPayload{Object: object, Deleted: true, FromVersion: int64(version)}
	payload.SetReplicationID(replID)
	bytes, err := json.Marshal(payload)
	r.NoError(err)
	r.NoError(worker.HandleObjectSync(ctx, asynq.NewTask(tasks.TypeObjectSync, bytes)))
	r.Equal(1, deletes)
	vector, err := versions.GetObj(ctx, replID, object)
	r.NoError(err)
	r.Equal(meta.Version{From: 2, To: 2}, vector)
}
