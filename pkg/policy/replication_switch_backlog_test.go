package policy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/testutil"
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

func TestDoneSwitchAllowsRecoveryReplicationWithoutDeletingSwitch(t *testing.T) {
	r := require.New(t)
	ctx := t.Context()
	svc := NewService(testutil.SetupRedis(t), nil, "a")
	oldPolicy := entity.BucketReplicationPolicy{User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket"}
	oldID := entity.UniversalFromBucketReplication(oldPolicy)
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	r.NoError(svc.bucketReplicationSwitchStore.Create(ctx, oldPolicy, switchInfo))
	r.NoError(svc.bucketReplicationSwitchStore.UpdateStatusOp(ctx, oldPolicy, entity.StatusInProgress, entity.StatusDone, "complete").Get())
	r.NoError(svc.bucketRoutingStore.SetOp(ctx, entity.NewBucketRoutingPolicyID("user", "bucket"), "b").Get())

	// A mutation routed before recovery setup still receives the completed
	// switch context so Replicate can persist its reverse event.
	requestCtx := xctx.SetMethod(context.Background(), s3.PutObject)
	requestCtx, err := svc.BuildProxyContext(requestCtx, "user", "bucket")
	r.NoError(err)
	completed := xctx.GetCompletedZeroDowntime(requestCtx)
	r.NotNil(completed)
	completedID := completed.ReplicationID()
	r.Equal(oldID.AsString(), completedID.AsString())

	recovery := entity.BucketReplicationPolicy{User: "user", FromStorage: "b", FromBucket: "bucket", ToStorage: "a", ToBucket: "bucket"}
	r.NoError(svc.AddBucketReplicationPolicy(ctx, recovery, entity.ReplicationOptions{}))
	info, err := svc.GetReplicationSwitchInfo(ctx, oldID)
	r.NoError(err)
	r.Equal(entity.StatusDone, info.LastStatus, "recovery setup keeps the old switch record until replication protection exists")

	requestCtx = xctx.SetMethod(context.Background(), s3.PutObject)
	requestCtx, err = svc.BuildProxyContext(requestCtx, "user", "bucket")
	r.NoError(err)
	replications := xctx.GetReplications(requestCtx)
	r.Len(replications, 1)
	r.Equal("user:b:a:bucket:bucket", replications[0].AsString())
	completed = xctx.GetCompletedZeroDowntime(requestCtx)
	r.NotNil(completed)
}

func TestDoneSwitchAllowsAddingAnotherFollower(t *testing.T) {
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
	r.NoError(svc.userReplicationSwitchStore.UpdateStatusOp(ctx, oldPolicy, entity.StatusInProgress, entity.StatusDone, "complete").Get())
	r.NoError(svc.userRoutingStore.SetOp(ctx, "user", "b").Get())

	newPolicy := entity.UserReplicationPolicy{User: "user", FromStorage: "b", ToStorage: "c"}
	r.NoError(svc.AddUserReplicationPolicy(ctx, newPolicy, entity.ReplicationOptions{}))
	info, err := svc.GetReplicationSwitchInfo(ctx, oldID)
	r.NoError(err)
	r.Equal(entity.StatusDone, info.LastStatus)
}
