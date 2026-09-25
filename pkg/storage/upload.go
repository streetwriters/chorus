// Copyright 2025 Clyso GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/validate"
)

type UploadSvc struct {
	store *store.UserUploadStore
	now   func() time.Time
}

func NewUploadSvc(client redis.Cmdable) *UploadSvc {
	return &UploadSvc{
		store: store.NewUserUploadStore(client),
		now:   time.Now,
	}
}

func (r *UploadSvc) StoreUpload(ctx context.Context, id entity.UserUploadObjectID,
	object entity.UserUploadObject, ttl time.Duration) error {
	if err := validate.UserUploadObjectID(id); err != nil {
		return fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(object); err != nil {
		return fmt.Errorf("unable to validate user upload object: %w", err)
	}
	active, oldTTL, err := r.activeUploads(ctx, id)
	if err != nil {
		return err
	}
	if ttl > 0 {
		object.ExpiresAt = r.now().Add(ttl).UTC()
	}
	if ttl > 0 {
		maxTTL := ttl
		for _, value := range active {
			if value.ExpiresAt.IsZero() {
				if oldTTL > maxTTL {
					maxTTL = oldTTL
				}
				continue
			}
			remaining := value.ExpiresAt.Sub(r.now())
			if remaining > maxTTL {
				maxTTL = remaining
			}
		}
		if _, err := r.store.AddWithTTL(ctx, id, object, maxTTL); err != nil {
			return fmt.Errorf("unable to add user upload object with expiration: %w", err)
		}
		return nil
	}
	if _, err := r.store.Add(ctx, id, object); err != nil {
		return fmt.Errorf("unable to add user upload object: %w", err)
	}
	return nil
}

func (r *UploadSvc) GetUpload(ctx context.Context, id entity.UserUploadObjectID, object, uploadID string) (*entity.UserUploadObject, error) {
	if err := validate.UserUploadObjectID(id); err != nil {
		return nil, fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	values, _, err := r.activeUploads(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("unable to list user uploads: %w", err)
	}
	for _, value := range values {
		if value.Object == object && value.UploadID == uploadID {
			return &value, nil
		}
	}
	return nil, nil
}

// UpdateUpload replaces a marker without resetting its existing TTL.
func (r *UploadSvc) UpdateUpload(ctx context.Context, id entity.UserUploadObjectID, old, updated entity.UserUploadObject) error {
	if err := validate.UserUploadObjectID(id); err != nil {
		return fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(old); err != nil {
		return fmt.Errorf("unable to validate old user upload object: %w", err)
	}
	if err := validate.UserUploadObject(updated); err != nil {
		return fmt.Errorf("unable to validate updated user upload object: %w", err)
	}
	if old.Object != updated.Object || old.UploadID != updated.UploadID || old.Storage != updated.Storage {
		return fmt.Errorf("upload marker identity cannot be changed")
	}
	updated.ExpiresAt = old.ExpiresAt
	replaced, err := r.store.Replace(ctx, id, old, updated)
	if err != nil {
		return fmt.Errorf("unable to update user upload object: %w", err)
	}
	if !replaced {
		return fmt.Errorf("upload marker expired before completion receipt was stored")
	}
	return nil
}

func (r *UploadSvc) UploadExists(ctx context.Context, id entity.UserUploadObjectID,
	object entity.UserUploadObject) (bool, error) {
	if err := validate.UserUploadObjectID(id); err != nil {
		return false, fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(object); err != nil {
		return false, fmt.Errorf("unable to validate user upload object: %w", err)
	}
	values, _, err := r.activeUploads(ctx, id)
	if err != nil {
		return false, fmt.Errorf("unable to check if upload exists: %w", err)
	}
	for _, value := range values {
		if value.Object == object.Object && value.UploadID == object.UploadID && value.Storage == object.Storage {
			return true, nil
		}
	}
	return false, nil
}

func (r *UploadSvc) UploadsExistForUser(ctx context.Context, user string) (bool, error) {
	if user == "" {
		return false, fmt.Errorf("%w: user is required to set uploadID", dom.ErrInvalidArg)
	}
	contains, err := r.store.HasIDs(ctx, user)
	if err != nil {
		return false, fmt.Errorf("unable to check if user uploads exist: %w", err)
	}
	return contains, nil
}

func (r *UploadSvc) UploadsExistForUserBucket(ctx context.Context, id entity.UserUploadObjectID) (bool, error) {
	if err := validate.UserUploadObjectID(id); err != nil {
		return false, fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	values, _, err := r.activeUploads(ctx, id)
	if err != nil {
		return false, fmt.Errorf("unable to check if user bucket uploads exist: %w", err)
	}
	return len(values) != 0, nil
}

// activeUploads removes expired per-upload receipts before returning the live
// set. The Redis collection TTL is set to the latest member expiration; the
// per-entry timestamp prevents later uploads from extending older markers.
func (r *UploadSvc) activeUploads(ctx context.Context, id entity.UserUploadObjectID) ([]entity.UserUploadObject, time.Duration, error) {
	values, err := r.store.Get(ctx, id)
	if errors.Is(err, dom.ErrNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	oldTTL, err := r.store.TTL(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	now := r.now()
	active := make([]entity.UserUploadObject, 0, len(values))
	for _, value := range values {
		if !value.ExpiresAt.IsZero() && !now.Before(value.ExpiresAt) {
			if _, err := r.store.Remove(ctx, id, value); err != nil {
				return nil, 0, err
			}
			continue
		}
		active = append(active, value)
	}
	if len(active) == 0 {
		return nil, oldTTL, nil
	}
	return active, oldTTL, nil
}

func (r *UploadSvc) DeleteUpload(ctx context.Context, id entity.UserUploadObjectID, object entity.UserUploadObject) error {
	if err := validate.UserUploadObjectID(id); err != nil {
		return fmt.Errorf("unable to validate user upload object id: %w", err)
	}
	if err := validate.UserUploadObject(object); err != nil {
		return fmt.Errorf("unable to validate user upload object: %w", err)
	}
	stored, err := r.GetUpload(ctx, id, object.Object, object.UploadID)
	if err != nil {
		return fmt.Errorf("unable to find upload marker to remove: %w", err)
	}
	if stored == nil || (object.Storage != "" && object.Storage != stored.Storage) {
		return nil
	}
	if _, err := r.store.Remove(ctx, id, *stored); err != nil {
		return fmt.Errorf("unable to remove upload: %w", err)
	}
	return nil
}
