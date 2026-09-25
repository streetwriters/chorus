package log

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/entity"
)

func TestStartNewCopiesCompletedSwitchForDeleteReplication(t *testing.T) {
	switchInfo := entity.ReplicationSwitchInfo{
		LastStatus:                        entity.StatusDone,
		ReplicationSwitchZeroDowntimeOpts: entity.ReplicationSwitchZeroDowntimeOpts{MultipartTTL: time.Minute},
	}
	switchInfo.SetReplicationID(entity.UniversalFromUserReplication(entity.UserReplicationPolicy{
		User: "user", FromStorage: "a", ToStorage: "b",
	}))
	source := xctx.SetCompletedZeroDowntime(context.Background(), switchInfo)
	source = xctx.SetRoutingPolicy(source, "b")

	background, cancel := StartNew(source)
	defer cancel()
	completed := xctx.GetCompletedZeroDowntime(background)
	require.NotNil(t, completed)
	replicationID := completed.ReplicationID()
	require.Equal(t, "user:a:b", replicationID.AsString())
}
