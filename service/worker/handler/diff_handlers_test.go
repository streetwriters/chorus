package handler

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/tasks"
)

type diffQueueStub struct {
	stats    map[string]*tasks.QueueStats
	enqueued int
}

func (q *diffQueueStub) UnprocessedCount(context.Context, bool, ...string) (int, error) {
	return 0, nil
}
func (*diffQueueStub) IsPaused(context.Context, string) (bool, error) { return false, nil }
func (*diffQueueStub) Resume(context.Context, string) error           { return nil }
func (*diffQueueStub) Pause(context.Context, string) error            { return nil }
func (q *diffQueueStub) Delete(_ context.Context, name string, _ bool) error {
	if _, ok := q.stats[name]; !ok {
		return dom.ErrNotFound
	}
	delete(q.stats, name)
	return nil
}
func (q *diffQueueStub) Stats(_ context.Context, name string) (*tasks.QueueStats, error) {
	stats, ok := q.stats[name]
	if !ok {
		return nil, dom.ErrNotFound
	}
	return stats, nil
}
func (q *diffQueueStub) EnqueueTask(context.Context, any) error {
	q.enqueued++
	return nil
}

func newDiffSvcForTest(t *testing.T, queue *diffQueueStub) *DiffSvc {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewDiffSvc(client, nil, queue)
}

func TestStartDiffRerunsCompletedCheckAfterQueueExpires(t *testing.T) {
	ctx := t.Context()
	queue := &diffQueueStub{stats: make(map[string]*tasks.QueueStats)}
	svc := newDiffSvcForTest(t, queue)
	id := entity.NewDiffID(
		entity.NewDiffLocation("main", "notesnook-bucket"),
		entity.NewDiffLocation("follower", "notesnook-bucket"),
	)
	settings := entity.NewDiffSettings("user1", false, true, false)

	require.NoError(t, svc.StartDiff(ctx, id, settings))
	require.Equal(t, 1, queue.enqueued)

	// Asynq has removed the empty completed queue, but its Redis diff report
	// remains available for inspection until the next diff run.
	require.NoError(t, svc.StartDiff(ctx, id, settings))
	require.Equal(t, 2, queue.enqueued)
	_, err := svc.GetDiffStatus(ctx, id)
	require.NoError(t, err)
}

func TestStartDiffDoesNotReplaceActiveCheck(t *testing.T) {
	ctx := t.Context()
	queue := &diffQueueStub{stats: make(map[string]*tasks.QueueStats)}
	svc := newDiffSvcForTest(t, queue)
	id := entity.NewDiffID(
		entity.NewDiffLocation("main", "notesnook-bucket"),
		entity.NewDiffLocation("follower", "notesnook-bucket"),
	)
	settings := entity.NewDiffSettings("user1", false, true, false)
	require.NoError(t, svc.StartDiff(ctx, id, settings))
	queue.stats[tasks.DiffQueue(id)] = &tasks.QueueStats{Unprocessed: 1}

	err := svc.StartDiff(ctx, id, settings)
	require.ErrorContains(t, err, "still in progress")
	require.Equal(t, 1, queue.enqueued)
}
