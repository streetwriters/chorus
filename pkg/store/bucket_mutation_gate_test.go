package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func gateForTest(t *testing.T, ttl time.Duration) (*BucketMutationGate, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewBucketMutationGate(client, ttl), server
}

func TestBucketMutationGateActiveLeases(t *testing.T) {
	r := require.New(t)
	gate, _ := gateForTest(t, time.Second)
	mutationA, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err)
	mutationB, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err)
	if _, err = gate.BeginTopologyChange(t.Context(), "user", "bucket"); !errors.Is(err, ErrBucketHasActiveMutations) {
		r.FailNow("expected active mutation conflict", "got %v", err)
	}
	r.NoError(mutationA.Release(t.Context()))
	if _, err = gate.BeginTopologyChange(t.Context(), "user", "bucket"); !errors.Is(err, ErrBucketHasActiveMutations) {
		r.FailNow("topology must remain blocked while mutation B is active", "got %v", err)
	}
	r.NoError(mutationB.Release(t.Context()))
	topology, err := gate.BeginTopologyChange(t.Context(), "user", "bucket")
	r.NoError(err)
	blockedMutation, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.Nil(blockedMutation)
	r.ErrorIs(err, ErrTopologyChangeInProgress)
	r.NoError(topology.Release(t.Context()))
	r.NoError(topology.Release(t.Context()), "lease release must be idempotent")
}

func TestBucketMutationGateLeaseExpiry(t *testing.T) {
	r := require.New(t)
	gate, server := gateForTest(t, time.Second)
	mutation, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err)
	server.FastForward(2 * time.Second)
	topology, err := gate.BeginTopologyChange(t.Context(), "user", "bucket")
	r.NoError(err, "expired crashed mutation lease must be pruned")
	r.NoError(mutation.Release(t.Context()))
	r.NoError(topology.Release(t.Context()))

	topology, err = gate.BeginTopologyChange(t.Context(), "user", "bucket")
	r.NoError(err)
	server.FastForward(2 * time.Second)
	mutation, err = gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err, "expired crashed topology lease must no longer block mutations")
	r.NoError(topology.Release(t.Context()))
	r.NoError(mutation.Release(t.Context()))
}

func TestBucketMutationGateDifferentBucketsAndUserRouting(t *testing.T) {
	r := require.New(t)
	gate, _ := gateForTest(t, time.Second)
	topology, err := gate.BeginTopologyChange(t.Context(), "user", "bucket-a")
	r.NoError(err)
	mutation, err := gate.BeginMutation(t.Context(), "user", "bucket-b")
	r.NoError(err, "a bucket topology change must not block another bucket")
	r.NoError(mutation.Release(t.Context()))
	r.NoError(topology.Release(t.Context()))

	mutation, err = gate.BeginMutation(t.Context(), "user", "bucket-a")
	r.NoError(err)
	if _, err = gate.BeginTopologyChange(t.Context(), "user", ""); !errors.Is(err, ErrBucketHasActiveMutations) {
		r.FailNow("user-wide routing must wait for every bucket mutation", "got %v", err)
	}
	r.NoError(mutation.Release(t.Context()))
	topology, err = gate.BeginTopologyChange(t.Context(), "user", "")
	r.NoError(err)
	if _, err = gate.BeginMutation(t.Context(), "user", "bucket-b"); !errors.Is(err, ErrTopologyChangeInProgress) {
		r.FailNow("user-wide routing must block new mutations for that user", "got %v", err)
	}
	r.NoError(topology.Release(t.Context()))
}

func TestBucketAndUserTopologyLeasesConflict(t *testing.T) {
	r := require.New(t)
	gate, _ := gateForTest(t, time.Second)
	bucketTopology, err := gate.BeginTopologyChange(t.Context(), "user", "bucket-a")
	r.NoError(err)
	if _, err := gate.BeginTopologyChange(t.Context(), "user", ""); !errors.Is(err, ErrBucketHasActiveMutations) {
		r.FailNow("user-wide topology must wait for bucket topology work", "got %v", err)
	}
	r.NoError(bucketTopology.Release(t.Context()))

	userTopology, err := gate.BeginTopologyChange(t.Context(), "user", "")
	r.NoError(err)
	if _, err := gate.BeginTopologyChange(t.Context(), "user", "bucket-a"); !errors.Is(err, ErrTopologyChangeInProgress) {
		r.FailNow("bucket topology must wait for user-wide routing changes", "got %v", err)
	}
	r.NoError(userTopology.Release(t.Context()))
}

func TestBucketMutationGateAtomicRace(t *testing.T) {
	gate, _ := gateForTest(t, time.Second)
	for i := 0; i < 100; i++ {
		bucket := fmt.Sprintf("bucket-%d", i)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var mutation *BucketGateLease
		var topology *BucketGateLease
		var mutationErr, topologyErr error
		go func() {
			defer wg.Done()
			<-start
			mutation, mutationErr = gate.BeginMutation(t.Context(), "user", bucket)
		}()
		go func() {
			defer wg.Done()
			<-start
			topology, topologyErr = gate.BeginTopologyChange(t.Context(), "user", bucket)
		}()
		close(start)
		wg.Wait()
		mutationWon := mutationErr == nil
		topologyWon := topologyErr == nil
		if mutationWon == topologyWon {
			t.Fatalf("iteration %d: exactly one gate must win; mutation=%v topology=%v", i, mutationErr, topologyErr)
		}
		if mutationWon {
			require.ErrorIs(t, topologyErr, ErrBucketHasActiveMutations)
			require.NoError(t, mutation.Release(t.Context()))
		} else {
			require.ErrorIs(t, mutationErr, ErrTopologyChangeInProgress)
			require.NoError(t, topology.Release(t.Context()))
		}
	}
}

func TestBucketMutationLeaseRenewalAndErrorCleanup(t *testing.T) {
	r := require.New(t)
	gate, _ := gateForTest(t, 90*time.Millisecond)
	lease, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err)
	started := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- lease.Run(t.Context(), func(context.Context) error {
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	time.Sleep(250 * time.Millisecond)
	if _, err := gate.BeginTopologyChange(t.Context(), "user", "bucket"); !errors.Is(err, ErrBucketHasActiveMutations) {
		r.FailNow("a long mutation must renew its lease", "got %v", err)
	}
	close(finish)
	r.NoError(<-done)
	topology, err := gate.BeginTopologyChange(t.Context(), "user", "bucket")
	r.NoError(err, "Run must release mutation state on completion")
	r.NoError(topology.Release(t.Context()))

	lease, err = gate.BeginTopologyChange(t.Context(), "user", "bucket")
	r.NoError(err)
	wantErr := errors.New("injected management failure")
	r.ErrorIs(lease.Run(t.Context(), func(context.Context) error { return wantErr }), wantErr)
	mutation, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err, "Run must release topology state on callback error")
	r.NoError(mutation.Release(t.Context()))
}

func TestBucketGateLeaseCancellationWaitsForWork(t *testing.T) {
	r := require.New(t)
	gate, _ := gateForTest(t, 90*time.Millisecond)
	lease, err := gate.BeginMutation(t.Context(), "user", "bucket")
	r.NoError(err)
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- lease.Run(ctx, func(context.Context) error {
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	cancel()
	if _, err := gate.BeginTopologyChange(t.Context(), "user", "bucket"); !errors.Is(err, ErrBucketHasActiveMutations) {
		r.FailNow("canceled request must keep its lease until active work exits", "got %v", err)
	}
	close(finish)
	r.ErrorIs(<-done, context.Canceled)
	topology, err := gate.BeginTopologyChange(t.Context(), "user", "bucket")
	r.NoError(err)
	r.NoError(topology.Release(t.Context()))
}
