package router

import (
	"context"
	"testing"
	"time"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestAdjustObjReadRouteUsesSwitchReplicationWithBlockedEvents(t *testing.T) {
	r := require.New(t)
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	switchID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	object := dom.Object{Bucket: "bucket", Name: "exists-only-on-a"}
	_, err := versions.IncrementObj(t.Context(), switchID, object, meta.Destination{Storage: "a", Bucket: "bucket"})
	r.NoError(err)

	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	switchInfo.SetReplicationID(switchID)
	baseCtx := xctx.SetInProgressZeroDowntime(context.Background(), switchInfo)
	baseCtx = xctx.SetBucket(baseCtx, "bucket")
	baseCtx = xctx.SetObject(baseCtx, object.Name)
	routes := [][]entity.UniversalReplicationID{
		{},
		{entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
			User: "other", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
		})},
	}
	for _, replications := range routes {
		readCtx := baseCtx
		if len(replications) > 0 {
			readCtx = xctx.SetReplications(readCtx, replications)
		}
		got, err := (&s3Router{versionSvc: versions}).adjustObjReadRoute(readCtx, "b")
		r.NoError(err)
		r.Equal("a", got, "only the unavailable object should route to the old source")
	}
}

func TestAdjustObjReadRouteKeepsReplicatedObjectOnActiveTarget(t *testing.T) {
	r := require.New(t)
	versions := meta.NewVersionService(testutil.SetupRedis(t))
	switchID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	object := dom.Object{Bucket: "bucket", Name: "replicated"}
	_, err := versions.IncrementObj(t.Context(), switchID, object, meta.Destination{Storage: "a", Bucket: "bucket"})
	r.NoError(err)
	_, err = versions.IncrementObj(t.Context(), switchID, object, meta.Destination{Storage: "b", Bucket: "bucket"})
	r.NoError(err)

	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	switchInfo.SetReplicationID(switchID)
	readCtx := xctx.SetInProgressZeroDowntime(context.Background(), switchInfo)
	readCtx = xctx.SetBucket(readCtx, "bucket")
	readCtx = xctx.SetObject(readCtx, object.Name)
	got, err := (&s3Router{versionSvc: versions}).adjustObjReadRoute(readCtx, "b")
	r.NoError(err)
	r.Equal("b", got)
}
