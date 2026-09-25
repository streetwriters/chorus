package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/ratelimit"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/testutil"
)

type partialDeleteS3Client struct{ s3client.Client }

func (partialDeleteS3Client) Do(*http.Request) (*http.Response, bool, error) {
	body := `<DeleteResult><Deleted><Key>foo</Key></Deleted><Error><Key>bar</Key><Code>AccessDenied</Code></Error><Deleted><Key>baz</Key></Deleted></DeleteResult>`
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, false, nil
}

type partialDeleteClients struct {
	objstore.Clients
	client s3client.Client
}

func (c partialDeleteClients) AsS3(context.Context, string, string) (s3client.Client, error) {
	return c.client, nil
}

func TestDeleteObjectsPersistsOnlyProviderSuccessfulDeleteIntent(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	versions := meta.NewVersionService(redis)
	queue := &deleteRaceQueue{}
	replID := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket"})
	router := NewS3Router(partialDeleteClients{client: partialDeleteS3Client{}}, versions, nil, ratelimit.New(redis, nil))
	handler := serve(router, replication.NewS3(queue, versions, nil), store.NewObjectLocker(redis, 0))
	request := httptest.NewRequest(http.MethodPost, "http://chorus/bucket?delete", strings.NewReader(`<Delete><Object><Key>foo</Key></Object><Object><Key>bar</Key></Object><Object><Key>baz</Key></Object></Delete>`))
	ctx := xctx.SetUser(request.Context(), "user")
	ctx = xctx.SetBucket(ctx, "bucket")
	ctx = xctx.SetMethod(ctx, s3.DeleteObjects)
	ctx = xctx.SetRoutingPolicy(ctx, "a")
	ctx = xctx.SetReplications(ctx, []entity.UniversalReplicationID{replID})
	request = request.WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	r.Equal(http.StatusOK, response.Code, response.Body.String())

	queue.mu.Lock()
	r.Len(queue.tasks, 2)
	got := make(map[string]bool)
	for _, item := range queue.tasks {
		task := item.(*tasks.ObjectSyncPayload)
		got[task.Object.Name] = task.Deleted
	}
	queue.mu.Unlock()
	r.Equal(map[string]bool{"foo": true, "baz": true}, got)
	for _, key := range []string{"foo", "baz"} {
		version, err := versions.GetObj(t.Context(), replID, dom.Object{Bucket: "bucket", Name: key})
		r.NoError(err)
		r.Equal(1, version.From)
	}
	barVersion, err := versions.GetObj(t.Context(), replID, dom.Object{Bucket: "bucket", Name: "bar"})
	r.NoError(err)
	r.True(barVersion.IsEmpty(), "provider-failed key must not advance durable deletion state")
}
