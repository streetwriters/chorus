package tasks

import (
	"math"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestObjectSyncTasksRetryThroughProviderOutages(t *testing.T) {
	ctx := t.Context()
	redis := testutil.SetupRedis(t)
	client := asynq.NewClientFromRedisClient(redis)
	inspector := asynq.NewInspectorFromRedisClient(redis)
	t.Cleanup(func() {
		_ = client.Close()
		_ = inspector.Close()
	})

	for _, payload := range []ObjectSyncPayload{
		{Object: dom.Object{Bucket: "bucket", Name: "blocked-copy"}},
		{Object: dom.Object{Bucket: "bucket", Name: "deleted"}, Deleted: true},
	} {
		payload.SetReplicationID(entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User: "user", FromStorage: "b", FromBucket: "bucket", ToStorage: "a", ToBucket: "bucket",
		}))
		task, err := objectSync.Encode(ctx, payload)
		require.NoError(t, err)
		queued, err := client.EnqueueContext(ctx, task)
		require.NoError(t, err)
		info, err := inspector.GetTaskInfo(queued.Queue, queued.ID)
		require.NoError(t, err)
		require.Equal(t, math.MaxInt32, info.MaxRetry, "object replication event must survive long provider outages")
	}
}
