package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/testutil"
)

func TestLockDoRetainsOwnershipUntilCanceledWorkExits(t *testing.T) {
	r := require.New(t)
	client := testutil.SetupRedis(t)
	locker := NewObjectLocker(client, 0)
	id := entity.NewVersionedObjectLockID("b", "bucket", "key", "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lock, err := locker.Lock(ctx, id)
	r.NoError(err)

	started := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- lock.Do(ctx, 20*time.Millisecond, func() error {
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	cancel()
	_, err = locker.Lock(t.Context(), id)
	r.Error(err, "a canceled caller must keep the distributed lock while its work is still running")
	close(finish)
	r.NoError(<-done)
	lock.Release(t.Context())
	competing, err := locker.Lock(t.Context(), id)
	r.NoError(err, "the lock becomes available after the canceled work exits")
	competing.Release(t.Context())
}
