package policy

import (
	"context"
	"testing"
	"time"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPromoteZeroDowntimeSwitchWithBacklogReleasesActiveIndexAndCompletesAfterRepair(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	svc := NewService(testutil.SetupRedis(t), nil, "a")
	policyID := entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	}
	replID := entity.UniversalFromBucketReplication(policyID)
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	r.NoError(svc.bucketReplicationSwitchStore.Create(ctx, policyID, switchInfo))
	r.NoError(svc.bucketRoutingStore.SetOp(ctx, entity.NewBucketRoutingPolicyID("user", "bucket"), "b").Get())

	r.NoError(svc.PromoteZeroDowntimeReplicationSwitch(ctx, replID))
	info, err := svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.Equal(entity.StatusPromotedWithBacklog, info.LastStatus)
	active, err := svc.bucketReplicationSwitchStore.IsZeroDowntimeActiveOp(ctx, policyID.LookupID()).Get()
	r.NoError(err)
	r.False(active, "promotion must release the active-switch index so another replication relationship can be configured")

	newPolicy := entity.BucketReplicationPolicy{
		User: "user", FromStorage: "b", FromBucket: "bucket", ToStorage: "c", ToBucket: "bucket",
	}
	r.NoError(svc.AddBucketReplicationPolicy(ctx, newPolicy, entity.ReplicationOptions{}))
	requestCtx := xctx.SetMethod(context.Background(), s3.PutObject)
	requestCtx, err = svc.BuildProxyContext(requestCtx, "user", "bucket")
	r.NoError(err)
	backlog := xctx.GetInProgressZeroDowntime(requestCtx)
	r.NotNil(backlog, "Proxy must retain the original vector while forwarding writes to the new replica")
	currentReplications := xctx.GetReplications(requestCtx)
	r.Len(currentReplications, 1)
	currentID := currentReplications[0]
	r.Equal("user:b:c:bucket:bucket", currentID.AsString())

	r.NoError(svc.CompleteZeroDowntimeReplicationSwitch(ctx, replID))
	info, err = svc.GetReplicationSwitchInfo(ctx, replID)
	r.NoError(err)
	r.Equal(entity.StatusDone, info.LastStatus, "the retained switch record can complete once its repair event drains")
}

func TestUserReplicationCanContinueFromPromotedTargetWhileOldBacklogRemains(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	svc := NewService(testutil.SetupRedis(t), nil, "a")
	oldPolicy := entity.UserReplicationPolicy{User: "user", FromStorage: "a", ToStorage: "b"}
	oldID := entity.UniversalFromUserReplication(oldPolicy)
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	r.NoError(svc.userReplicationSwitchStore.Create(ctx, oldPolicy, switchInfo))
	r.NoError(svc.userRoutingStore.SetOp(ctx, "user", "b").Get())
	r.NoError(svc.PromoteZeroDowntimeReplicationSwitch(ctx, oldID))

	newPolicy := entity.UserReplicationPolicy{User: "user", FromStorage: "b", ToStorage: "c"}
	r.NoError(svc.AddUserReplicationPolicy(ctx, newPolicy, entity.ReplicationOptions{}))
	requestCtx := xctx.SetMethod(context.Background(), s3.PutObject)
	requestCtx, err := svc.BuildProxyNoBucketContext(requestCtx, "user")
	r.NoError(err)
	r.NotNil(xctx.GetInProgressZeroDowntime(requestCtx))
	currentReplications := xctx.GetReplications(requestCtx)
	r.Len(currentReplications, 1)
	currentID := currentReplications[0]
	r.Equal("user:b:c", currentID.AsString())
}
