package handler

import (
	"testing"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/stretchr/testify/require"
)

func TestSourceOnlineDetectsUnavailableProvider(t *testing.T) {
	ctx := t.Context()
	storage := func(address string) objstore.Storage {
		return objstore.Storage{
			CommonConfig: objstore.CommonConfig{Type: dom.S3},
			S3: &s3.Storage{
				StorageAddress: s3.StorageAddress{Address: address, Provider: s3.ProviderMinIO},
				Credentials: map[string]s3.CredentialsV4{
					"user": {AccessKeyID: "access", SecretAccessKey: "secret"},
				},
			},
		}
	}
	conf := &objstore.Config{
		Main: "a",
		Storages: map[string]objstore.Storage{
			"a": storage("http://127.0.0.1:1"),
			"b": storage("http://127.0.0.1:2"),
		},
	}
	creds, err := objstore.NewCredsSvc(ctx, conf, nil)
	require.NoError(t, err)
	clients, err := objstore.NewRegistry(ctx, creds, metrics.NewS3Service(false))
	require.NoError(t, err)
	worker := &switchSvc{clients: clients}
	id := entity.UniversalFromBucketReplication(entity.BucketReplicationPolicy{
		User: "user", FromStorage: "a", FromBucket: "bucket", ToStorage: "b", ToBucket: "bucket",
	})
	online, err := worker.sourceOnline(ctx, id)
	require.NoError(t, err)
	require.False(t, online)
}
