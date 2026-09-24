package objstore

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/stretchr/testify/require"
)

func TestNewRegistryKeepsOfflineS3StorageAvailableToRouting(t *testing.T) {
	ctx := t.Context()
	backendB := httptest.NewServer(http.NotFoundHandler())
	defer backendB.Close()

	storage := func(address string) Storage {
		return Storage{
			CommonConfig: CommonConfig{Type: dom.S3},
			S3: &s3.Storage{
				StorageAddress: s3.StorageAddress{Address: address, Provider: s3.ProviderMinIO},
				Credentials: map[string]s3.CredentialsV4{
					"user": {AccessKeyID: "access", SecretAccessKey: "secret"},
				},
			},
		}
	}
	conf := &Config{
		Main: "a",
		Storages: map[string]Storage{
			"a": storage("http://127.0.0.1:1"),
			"b": storage(backendB.URL),
		},
	}
	creds, err := NewCredsSvc(ctx, conf, nil)
	require.NoError(t, err)

	// Proxy and Worker independently create a provider registry at process
	// startup. A switched Redis route points at B; an unreachable A must not
	// make either registry, and therefore either process, fail to start.
	for _, process := range []string{"proxy", "worker"} {
		t.Run(process, func(t *testing.T) {
			registry, err := NewRegistry(ctx, creds, metrics.NewS3Service(false))
			require.NoError(t, err)
			onlineClient, err := registry.AsS3(ctx, "b", "user")
			require.NoError(t, err)
			require.NotNil(t, onlineClient)
			onlineHealth, ok := onlineClient.(s3client.HealthReporter)
			require.True(t, ok)
			require.Eventually(t, onlineHealth.IsOnline, time.Second, 10*time.Millisecond)
			offlineClient, err := registry.AsS3(ctx, "a", "user")
			require.NoError(t, err)
			offlineHealth, ok := offlineClient.(s3client.HealthReporter)
			require.True(t, ok)
			require.Eventually(t, offlineHealth.HealthKnown, time.Second, 10*time.Millisecond)
			require.False(t, offlineHealth.IsOnline())
		})
	}
}
