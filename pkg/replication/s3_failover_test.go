package replication

import (
	"context"
	"testing"
	"time"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type taskRecorder struct{ tasks []any }

func (q *taskRecorder) EnqueueTask(_ context.Context, task any) error {
	q.tasks = append(q.tasks, task)
	return nil
}
func (*taskRecorder) UnprocessedCount(context.Context, bool, ...string) (int, error) { return 0, nil }
func (*taskRecorder) IsPaused(context.Context, string) (bool, error)                 { return false, nil }
func (*taskRecorder) Resume(context.Context, string) error                           { return nil }
func (*taskRecorder) Pause(context.Context, string) error                            { return nil }
func (*taskRecorder) Delete(context.Context, string, bool) error                     { return nil }
func (*taskRecorder) Stats(context.Context, string) (*tasks.QueueStats, error) {
	return &tasks.QueueStats{}, nil
}

func TestDeleteDuringZeroDowntimeSwitchQueuesDurableReverseIntent(t *testing.T) {
	r := require.New(t)
	queue := &taskRecorder{}
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	original := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	activeSwitch := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	activeSwitch.SetReplicationID(original)
	ctx := xctx.SetInProgressZeroDowntime(context.Background(), activeSwitch)
	ctx = xctx.SetBucket(ctx, "bucket")
	object := &tasks.ObjectSyncPayload{Object: dom.Object{Bucket: "bucket", Name: "deleted"}, Deleted: true}

	err := NewS3(queue, versions, nil).Replicate(ctx, "b", object)
	r.NoError(err)
	r.Len(queue.tasks, 1, "a delete on B must persist an intent to delete A even while A is offline")
	deleteTask, ok := queue.tasks[0].(*tasks.ObjectSyncPayload)
	r.True(ok)
	r.True(deleteTask.Deleted)
	replicationID := deleteTask.GetReplicationID()
	r.Equal("user:b:a:bucket:bucket", replicationID.AsString())
}

func TestDeleteAfterCompletedSwitchQueuesReverseIntent(t *testing.T) {
	r := require.New(t)
	queue := &taskRecorder{}
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	original := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	completedSwitch := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusDone,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	completedSwitch.SetReplicationID(original)
	ctx := xctx.SetCompletedZeroDowntime(context.Background(), completedSwitch)
	ctx = xctx.SetBucket(ctx, "bucket")
	err := NewS3(queue, versions, nil).Replicate(ctx, "b", &tasks.ObjectSyncPayload{
		Object: dom.Object{Bucket: "bucket", Name: "deleted-after-switch"}, Deleted: true,
	})
	r.NoError(err)
	r.Len(queue.tasks, 1)
	deleteTask := queue.tasks[0].(*tasks.ObjectSyncPayload)
	r.True(deleteTask.Deleted)
	replicationID := deleteTask.GetReplicationID()
	r.Equal("user:b:a:bucket:bucket", replicationID.AsString())
}

func TestDeleteAfterSwitchKeepsCurrentReplicationAndOldReplicaIntent(t *testing.T) {
	r := require.New(t)
	queue := &taskRecorder{}
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	original := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	current := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "b", FromBucket: "bucket", ToStorage: "c", ToBucket: "bucket",
	})
	completedSwitch := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusDone,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	completedSwitch.SetReplicationID(original)
	ctx := xctx.SetCompletedZeroDowntime(context.Background(), completedSwitch)
	ctx = xctx.SetBucket(ctx, "bucket")
	ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{current})
	err := NewS3(queue, versions, nil).Replicate(ctx, "b", &tasks.ObjectSyncPayload{
		Object: dom.Object{Bucket: "bucket", Name: "deleted"}, Deleted: true,
	})
	r.NoError(err)
	r.Len(queue.tasks, 2)
	ids := make([]string, 0, len(queue.tasks))
	for _, task := range queue.tasks {
		objectTask := task.(*tasks.ObjectSyncPayload)
		r.True(objectTask.Deleted)
		replicationID := objectTask.GetReplicationID()
		ids = append(ids, replicationID.AsString())
	}
	r.ElementsMatch([]string{"user:b:a:bucket:bucket", "user:b:c:bucket:bucket"}, ids)
}

func TestDeleteDoesNotDuplicateReverseReplicationEvent(t *testing.T) {
	r := require.New(t)
	queue := &taskRecorder{}
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	original := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	reverse := original.Swap()
	completedSwitch := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusDone,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	completedSwitch.SetReplicationID(original)
	ctx := xctx.SetCompletedZeroDowntime(context.Background(), completedSwitch)
	ctx = xctx.SetBucket(ctx, "bucket")
	ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{reverse})
	err := NewS3(queue, versions, nil).Replicate(ctx, "b", &tasks.ObjectSyncPayload{
		Object: dom.Object{Bucket: "bucket", Name: "deleted"}, Deleted: true,
	})
	r.NoError(err)
	r.Len(queue.tasks, 1)
	deleteTask := queue.tasks[0].(*tasks.ObjectSyncPayload)
	queuedID := deleteTask.GetReplicationID()
	r.Equal(reverse.AsString(), queuedID.AsString())
}

func TestWriteAfterPromotionAdvancesOldVectorAndReplicatesToNewTarget(t *testing.T) {
	r := require.New(t)
	queue := &taskRecorder{}
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	original := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	current := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "b", FromBucket: "bucket", ToStorage: "c", ToBucket: "bucket",
	})
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusPromotedWithBacklog,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	switchInfo.SetReplicationID(original)
	ctx := xctx.SetInProgressZeroDowntime(context.Background(), switchInfo)
	ctx = xctx.SetBucket(ctx, "bucket")
	ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{current})
	object := dom.Object{Bucket: "bucket", Name: "new-write"}
	err := NewS3(queue, versions, nil).Replicate(ctx, "b", &tasks.ObjectSyncPayload{Object: object})
	r.NoError(err)

	oldVector, err := versions.GetObj(t.Context(), original, object)
	r.NoError(err)
	r.Equal(1, oldVector.To, "a late A->B event must see that B has newer data and skip its overwrite")
	r.Len(queue.tasks, 1, "the promoted B->C relationship must receive new writes")
	queuedTask := queue.tasks[0].(*tasks.ObjectSyncPayload)
	queuedID := queuedTask.GetReplicationID()
	r.Equal(current.AsString(), queuedID.AsString())
}
