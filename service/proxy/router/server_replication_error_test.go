package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/tasks"
)

type replicationFailureRouter struct{ task tasks.ReplicationTask }

func (r replicationFailureRouter) Route(*http.Request) (*http.Response, []tasks.ReplicationTask, string, bool, error) {
	return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, []tasks.ReplicationTask{r.task}, "b", false, nil
}

type failingReplicator struct{}

func (failingReplicator) Replicate(context.Context, string, tasks.ReplicationTask) error {
	return dom.ErrInternal
}

var _ replication.Service = failingReplicator{}

func TestServeReturnsRetryableErrorWhenObjectEventCannotBePersisted(t *testing.T) {
	task := &tasks.ObjectSyncPayload{Deleted: true}
	handler := serve(replicationFailureRouter{task: task}, failingReplicator{})
	request := httptest.NewRequest(http.MethodDelete, "/bucket/object", nil)
	requestCtx := ctx.SetBucket(request.Context(), "bucket")
	requestCtx = ctx.SetRoutingPolicy(requestCtx, "b")
	request = request.WithContext(requestCtx)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, "1", response.Header().Get("Retry-After"))
	require.Contains(t, response.Body.String(), "ServiceUnavailable")
}
