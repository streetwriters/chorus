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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/clyso/chorus/pkg/entity"
)

func TokensToUserUploadObjectIDConverter(tokens []string) (entity.UserUploadObjectID, error) {
	return entity.UserUploadObjectID{
		User:   tokens[0],
		Bucket: tokens[1],
	}, nil
}

func UserUploadObjectIDToTokensConverter(id entity.UserUploadObjectID) ([]string, error) {
	return []string{id.User, id.Bucket}, nil
}

func UserUploadObjectToStringConverter(value entity.UserUploadObject) (string, error) {
	bytes, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("unable to serialize user upload object: %w", err)
	}
	return string(bytes), nil
}

func StringToUserUploadObjectConverter(value string) (entity.UserUploadObject, error) {
	var result entity.UserUploadObject
	if err := json.Unmarshal([]byte(value), &result); err != nil {
		var noVal entity.UserUploadObject
		return noVal, fmt.Errorf("unable to deserialize user upload object: %w", err)
	}
	return result, nil
}

type UserUploadStore struct {
	RedisIDKeySet[entity.UserUploadObjectID, entity.UserUploadObject]
}

// Replace retains the upload set's existing TTL while atomically replacing a marker.
func (r *UserUploadStore) Replace(ctx context.Context, id entity.UserUploadObjectID, old, updated entity.UserUploadObject) (bool, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return false, err
	}
	oldValue, err := UserUploadObjectToStringConverter(old)
	if err != nil {
		return false, err
	}
	updatedValue, err := UserUploadObjectToStringConverter(updated)
	if err != nil {
		return false, err
	}
	const replaceScript = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[2])
redis.call('SREM', KEYS[1], ARGV[1])
return 1`
	result, err := r.client.Eval(ctx, replaceScript, []string{key}, oldValue, updatedValue).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (r *UserUploadStore) TTL(ctx context.Context, id entity.UserUploadObjectID) (time.Duration, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, err
	}
	return r.client.TTL(ctx, key).Result()
}

func (r *UserUploadStore) AddWithTTL(ctx context.Context, id entity.UserUploadObjectID, value entity.UserUploadObject, ttl time.Duration) (uint64, error) {
	key, err := r.MakeKey(id)
	if err != nil {
		return 0, err
	}
	serialized, err := UserUploadObjectToStringConverter(value)
	if err != nil {
		return 0, err
	}
	millis := ttl / time.Millisecond
	if ttl%time.Millisecond != 0 {
		millis++
	}
	const addScript = `
local added = redis.call('SADD', KEYS[1], ARGV[1])
local current = redis.call('PTTL', KEYS[1])
local requested = tonumber(ARGV[2])
if current < requested then
  redis.call('PEXPIRE', KEYS[1], requested)
end
return added`
	added, err := r.client.Eval(ctx, addScript, []string{key}, serialized, int64(millis)).Uint64()
	return added, err
}

func NewUserUploadStore(client redis.Cmdable) *UserUploadStore {
	return &UserUploadStore{
		*NewRedisIDKeySet(client, "r:upload",
			UserUploadObjectIDToTokensConverter, TokensToUserUploadObjectIDConverter,
			UserUploadObjectToStringConverter, StringToUserUploadObjectConverter),
	}
}
