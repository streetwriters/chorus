package s3client

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

type canceledRoundTripper struct{}

func (canceledRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, context.Canceled
}

func TestCanceledForwardRequestReturnsCancellation(t *testing.T) {
	provider := &client{
		c:           &http.Client{Transport: canceledRoundTripper{}},
		conf:        s3.StorageAddress{Address: "http://storage.example"},
		cred:        s3.CredentialsV4{AccessKeyID: "access", SecretAccessKey: "secret"},
		metricsSvc:  metrics.NewS3Service(false),
		storageName: "main",
		userName:    "user",
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://proxy.example/bucket/object", nil)
	require.NoError(t, err)

	_, _, err = provider.Do(request)
	require.ErrorIs(t, err, context.Canceled)
}
