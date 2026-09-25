/*
 * Copyright © 2024 Clyso GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/testutil"
)

func Test_svc_StoreUploadID(t *testing.T) {
	r := require.New(t)
	c := testutil.SetupRedis(t)

	storage := NewUploadSvc(c)
	ctx := context.Background()

	users := []string{"u1", "u2"}
	buckets := []string{"b1", "b2"}
	uploads := []string{"id1", "id2"}

	for _, user := range users {
		exists, err := storage.UploadsExistForUser(ctx, user)
		r.NoError(err)
		r.False(exists)
		for _, bucket := range buckets {
			exists, err := storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket))
			r.NoError(err)
			r.False(exists)
			for _, upload := range uploads {
				exists, err := storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket), entity.NewUserUploadObject(upload, upload))
				r.NoError(err)
				r.False(exists)
			}
		}
	}

	for _, user := range users {
		for _, bucket := range buckets {
			for _, upload := range uploads {
				err := storage.StoreUpload(ctx, entity.NewUserUploadObjectID(user, bucket), entity.NewUserUploadObject(upload, upload), time.Minute)
				r.NoError(err)
			}
		}
	}

	for _, user := range users {
		exists, err := storage.UploadsExistForUser(ctx, user)
		r.NoError(err)
		r.True(exists)
		for _, bucket := range buckets {
			exists, err := storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket))
			r.NoError(err)
			r.True(exists)
			for _, upload := range uploads {
				exists, err := storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket), entity.NewUserUploadObject(upload, upload))
				r.NoError(err)
				r.True(exists)
			}
		}
	}
	exists, err := storage.UploadExists(ctx, entity.NewUserUploadObjectID(users[0], buckets[0]), entity.NewUserUploadObject(uploads[0], uploads[0]))
	r.NoError(err)
	r.True(exists)
	err = storage.DeleteUpload(ctx, entity.NewUserUploadObjectID(users[0], buckets[0]), entity.NewUserUploadObject(uploads[0], uploads[0]))
	r.NoError(err)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(users[0], buckets[0]), entity.NewUserUploadObject(uploads[0], uploads[0]))
	r.NoError(err)
	r.False(exists)

	err = storage.DeleteUpload(ctx, entity.NewUserUploadObjectID("missing", "keys"), entity.NewUserUploadObject("valid", "args"))
	r.NoError(err)

}

func Test_StoreUploadID(t *testing.T) {
	c := testutil.SetupRedis(t)
	r := require.New(t)
	ctx := t.Context()
	// storage := New(c)
	storage := NewUploadSvc(c)

	user := "u1"
	bucket1, bucket2 := "b1", "b2"
	obj := "o1"
	upload := "id1"

	// not exists
	exists, err := storage.UploadsExistForUser(ctx, user)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket1))
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)
	r.False(exists)

	// store to user bucket1
	err = storage.StoreUpload(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload), time.Minute)
	r.NoError(err)
	// exists for user and bucket1
	exists, err = storage.UploadsExistForUser(ctx, user)
	r.NoError(err)
	r.True(exists)
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket1))
	r.NoError(err)
	r.True(exists)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)
	r.True(exists)

	// not exists for bucket2
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket2))
	r.NoError(err)
	r.False(exists)
	// not exists for other user
	exists, err = storage.UploadsExistForUser(ctx, "u2")
	r.NoError(err)
	r.False(exists)

	// delete upload ID
	err = storage.DeleteUpload(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)

	// not exists
	exists, err = storage.UploadsExistForUser(ctx, user)
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadsExistForUserBucket(ctx, entity.NewUserUploadObjectID(user, bucket1))
	r.NoError(err)
	r.False(exists)
	exists, err = storage.UploadExists(ctx, entity.NewUserUploadObjectID(user, bucket1), entity.NewUserUploadObject(obj, upload))
	r.NoError(err)
	r.False(exists)
}

func TestGetUploadReturnsOriginAndTreatsMissingAsAbsent(t *testing.T) {
	r := require.New(t)
	svc := NewUploadSvc(testutil.SetupRedis(t))
	ctx := t.Context()
	id := entity.NewUserUploadObjectID("u1", "b1")

	missing, err := svc.GetUpload(ctx, id, "object", "missing")
	r.NoError(err)
	r.Nil(missing)

	want := entity.NewUserUploadObject("object", "upload-1", "storage-a")
	r.NoError(svc.StoreUpload(ctx, id, want, time.Hour))
	got, err := svc.GetUpload(ctx, id, "object", "upload-1")
	r.NoError(err)
	r.Equal(want.Object, got.Object)
	r.Equal(want.UploadID, got.UploadID)
	r.Equal(want.Storage, got.Storage)
	r.False(got.ExpiresAt.IsZero())
}

func TestUpdateUploadPreservesMarkerTTLAndIdentity(t *testing.T) {
	r := require.New(t)
	redis := testutil.SetupRedis(t)
	svc := NewUploadSvc(redis)
	ctx := t.Context()
	id := entity.NewUserUploadObjectID("u1", "b1")
	started := entity.NewUserUploadObject("object", "upload-1", "storage-a")
	started.StartedAt = time.Now().UTC()
	r.NoError(svc.StoreUpload(ctx, id, started, time.Hour))
	startedStored, err := svc.GetUpload(ctx, id, started.Object, started.UploadID)
	r.NoError(err)
	key, err := store.NewUserUploadStore(redis).MakeKey(id)
	r.NoError(err)
	before := redis.TTL(ctx, key).Val()

	completed := *startedStored
	completed.CompletedETag = "etag"
	completed.CompletedSize = 42
	completed.CompletedLastModified = time.Now().UTC()
	r.NoError(svc.UpdateUpload(ctx, id, *startedStored, completed))
	got, err := svc.GetUpload(ctx, id, "object", "upload-1")
	r.NoError(err)
	r.Equal(&completed, got)
	after := redis.TTL(ctx, key).Val()
	r.Greater(after, time.Duration(0))
	r.LessOrEqual(after, before, "updating the receipt must not extend marker lifetime")

	wrongIdentity := completed
	wrongIdentity.Storage = "storage-b"
	r.Error(svc.UpdateUpload(ctx, id, completed, wrongIdentity))
	r.NoError(svc.DeleteUpload(ctx, id, completed))
	other := entity.NewUserUploadObject("other-object", "other-upload", "storage-a")
	r.NoError(svc.StoreUpload(ctx, id, other, time.Hour))
	r.Error(svc.UpdateUpload(ctx, id, completed, completed), "an expired or aborted marker cannot be recreated")
}

func TestUploadMarkersKeepIndependentEffectiveTTLWithinBucket(t *testing.T) {
	r := require.New(t)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	svc := NewUploadSvc(client)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return now }
	ctx := t.Context()
	id := entity.NewUserUploadObjectID("u1", "b1")
	sevenDays := 7 * 24 * time.Hour

	uploadA := entity.NewUserUploadObject("object-a", "upload-a", "storage-a")
	r.NoError(svc.StoreUpload(ctx, id, uploadA, sevenDays))
	storedA, err := svc.GetUpload(ctx, id, uploadA.Object, uploadA.UploadID)
	r.NoError(err)
	expiresA := storedA.ExpiresAt

	now = now.Add(6 * 24 * time.Hour)
	uploadB := entity.NewUserUploadObject("object-b", "upload-b", "storage-a")
	r.NoError(svc.StoreUpload(ctx, id, uploadB, sevenDays))
	storedA, err = svc.GetUpload(ctx, id, uploadA.Object, uploadA.UploadID)
	r.NoError(err)
	r.Equal(expiresA, storedA.ExpiresAt, "adding B must not extend A's original receipt expiry")

	now = expiresA.Add(time.Minute)
	gotA, err := svc.GetUpload(ctx, id, uploadA.Object, uploadA.UploadID)
	r.NoError(err)
	r.Nil(gotA, "A is expired at its own seven-day deadline")
	gotB, err := svc.GetUpload(ctx, id, uploadB.Object, uploadB.UploadID)
	r.NoError(err)
	r.NotNil(gotB, "B remains available through its own receipt window")

	now = gotB.ExpiresAt.Add(time.Minute)
	exists, err := svc.UploadsExistForUserBucket(ctx, id)
	r.NoError(err)
	r.False(exists, "expired receipts are pruned even while the bucket remains active")
}
