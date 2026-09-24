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

func TestProxyRestartRestoresSwitchedRouteFromRedis(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	writePolicy := NewService(redis, nil, "a")
	r.NoError(writePolicy.userRoutingStore.SetOp(t.Context(), "user", "b").Get())

	// Constructing a fresh policy service models a Proxy process restart. The
	// routing decision must be restored from Redis while A remains offline.
	restartedProxyPolicy := NewService(redis, nil, "a")
	requestCtx, err := restartedProxyPolicy.BuildProxyNoBucketContext(context.Background(), "user")
	r.NoError(err)
	r.Equal("b", xctx.GetRoutingPolicy(requestCtx))
}

func TestCompletedSwitchIdentitySurvivesProxyRestartForDeletePropagation(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	policyBeforeRestart := NewService(redis, nil, "a")
	userReplication := entity.UserReplicationPolicy{User: "user", FromStorage: "a", ToStorage: "b"}
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusInProgress,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	r.NoError(policyBeforeRestart.userReplicationSwitchStore.Create(t.Context(), userReplication, switchInfo))
	r.NoError(policyBeforeRestart.userReplicationSwitchStore.UpdateStatusOp(t.Context(), userReplication, entity.StatusInProgress, entity.StatusDone, "complete").Get())
	r.NoError(policyBeforeRestart.userRoutingStore.SetOp(t.Context(), "user", "b").Get())

	policyAfterRestart := NewService(redis, nil, "a")
	baseCtx := xctx.SetMethod(context.Background(), s3.DeleteObject)
	requestCtx, err := policyAfterRestart.BuildProxyNoBucketContext(baseCtx, "user")
	r.NoError(err)
	completed := xctx.GetCompletedZeroDowntime(requestCtx)
	r.NotNil(completed)
	var replicationID = completed.ReplicationID()
	r.Equal("user:a:b", replicationID.AsString())
}
