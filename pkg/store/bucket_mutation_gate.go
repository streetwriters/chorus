// Copyright 2026 Clyso GmbH
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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrBucketHasActiveMutations = errors.New("BucketHasActiveMutations")
	ErrTopologyChangeInProgress = errors.New("TopologyChangeInProgress")
)

const defaultBucketGateLeaseTTL = 30 * time.Second

type BucketMutationGate struct {
	client redis.Scripter
	ttl    time.Duration
}

type BucketGateLease struct {
	client  redis.Scripter
	token   string
	mode    string
	topos   []string
	indexes []string
	leases  []string
	ttl     time.Duration
}

func NewBucketMutationGate(client redis.Scripter, ttl time.Duration) *BucketMutationGate {
	if ttl <= 0 {
		ttl = defaultBucketGateLeaseTTL
	}
	return &BucketMutationGate{client: client, ttl: ttl}
}

func (g *BucketMutationGate) BeginMutation(ctx context.Context, user, bucket string) (*BucketGateLease, error) {
	if user == "" || bucket == "" {
		return nil, fmt.Errorf("bucket mutation gate requires user and bucket")
	}
	token, err := newGateToken()
	if err != nil {
		return nil, err
	}
	uScope := userGateScope(user)
	bScope := bucketGateScope(user, bucket)
	lease := &BucketGateLease{
		client:  g.client,
		ttl:     g.ttl,
		token:   token,
		mode:    "mutation",
		indexes: []string{userScopeKey(uScope, "mutations"), userScopeKey(bScope, "mutations")},
		leases:  []string{leaseKey(uScope, token), leaseKey(bScope, token)},
	}
	keys := []string{
		userScopeKey(uScope, "topology"),
		userScopeKey(bScope, "topology"),
		lease.indexes[0],
		lease.indexes[1],
		lease.leases[0],
		lease.leases[1],
	}
	result, err := beginMutationScript.Run(ctx, g.client, keys, token, ttlMillis(g.ttl)).Int()
	if err != nil {
		return nil, err
	}
	if result == 0 {
		return nil, ErrTopologyChangeInProgress
	}
	return lease, nil
}

// BeginTopologyChange reserves either one bucket or the user's fallback route.
// Mutations register against both scopes, so user-wide routing changes also
// wait for all active object mutations belonging to that user.
func (g *BucketMutationGate) BeginTopologyChange(ctx context.Context, user, bucket string) (*BucketGateLease, error) {
	if user == "" {
		return nil, fmt.Errorf("topology gate requires user")
	}
	token, err := newGateToken()
	if err != nil {
		return nil, err
	}
	userScope := userGateScope(user)
	if bucket != "" {
		bucketScope := bucketGateScope(user, bucket)
		bucketTopology := userScopeKey(bucketScope, "topology")
		bucketIndex := userScopeKey(bucketScope, "mutations")
		userTopology := userScopeKey(userScope, "topology")
		userIndex := userScopeKey(userScope, "mutations")
		leaseKey := leaseKey(userScope, token+":topology:"+scopeHash(bucket))
		result, err := beginBucketTopologyScript.Run(ctx, g.client, []string{bucketTopology, bucketIndex, userTopology, userIndex, leaseKey}, token, ttlMillis(g.ttl)).Int()
		if err != nil {
			return nil, err
		}
		if result == -1 {
			return nil, ErrTopologyChangeInProgress
		}
		if result > 0 {
			return nil, ErrBucketHasActiveMutations
		}
		return &BucketGateLease{
			client: g.client, ttl: g.ttl, token: token, mode: "topology",
			topos: []string{bucketTopology, leaseKey}, indexes: []string{userIndex}, leases: []string{leaseKey},
		}, nil
	}
	topo := userScopeKey(userScope, "topology")
	index := userScopeKey(userScope, "mutations")
	result, err := beginTopologyScript.Run(ctx, g.client, []string{topo, index}, token, ttlMillis(g.ttl)).Int()
	if err != nil {
		return nil, err
	}
	if result == -1 {
		return nil, ErrTopologyChangeInProgress
	}
	if result > 0 {
		return nil, ErrBucketHasActiveMutations
	}
	return &BucketGateLease{client: g.client, ttl: g.ttl, token: token, mode: "topology", topos: []string{topo}}, nil
}

func (l *BucketGateLease) Run(ctx context.Context, work func(context.Context) error) error {
	if l == nil {
		return work(ctx)
	}
	defer func() { _ = l.Release(context.Background()) }()
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- work(workCtx) }()
	refresh := l.ttl / 3
	if refresh < 10*time.Millisecond {
		refresh = 10 * time.Millisecond
	}
	ticker := time.NewTicker(refresh)
	defer ticker.Stop()
	ctxDone := ctx.Done()
	cancelled := false
	var renewErr error
	for {
		select {
		case err := <-done:
			if cancelled && ctx.Err() != nil {
				return ctx.Err()
			}
			if renewErr != nil {
				return renewErr
			}
			return err
		case <-ctxDone:
			cancelled = true
			ctxDone = nil
			cancel()
		case <-ticker.C:
			renewCtx := ctx
			if cancelled {
				renewCtx = context.Background()
			}
			err, owned := l.renew(renewCtx)
			if err != nil {
				renewErr = err
				cancel()
			} else if !owned {
				renewErr = errors.New("bucket gate lease was lost")
				cancel()
			}
		}
	}
}

func (l *BucketGateLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if l.mode == "topology" {
		keys := []string{l.topos[0]}
		if len(l.indexes) != 0 && len(l.leases) != 0 {
			keys = append(keys, l.indexes[0], l.leases[0])
		}
		return releaseTopologyScript.Run(ctx, l.client, keys, l.token).Err()
	}
	return releaseMutationScript.Run(ctx, l.client, []string{l.indexes[0], l.indexes[1], l.leases[0], l.leases[1]}, l.token).Err()
}

func (l *BucketGateLease) renew(ctx context.Context) (error, bool) {
	var keys []string
	var script *redis.Script
	if l.mode == "topology" {
		keys = l.topos
		script = renewTopologyScript
	} else {
		keys = l.leases
		script = renewMutationScript
	}
	owned, err := script.Run(ctx, l.client, keys, l.token, ttlMillis(l.ttl)).Int()
	return err, owned == 1
}

func newGateToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

func ttlMillis(ttl time.Duration) int64 {
	millis := ttl / time.Millisecond
	if ttl%time.Millisecond != 0 {
		millis++
	}
	return int64(millis)
}

func scopeHash(value string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return fmt.Sprintf("%016x", h.Sum64())
}

func userGateScope(user string) string { return "{" + scopeHash(user) + "}:user" }
func bucketGateScope(user, bucket string) string {
	return "{" + scopeHash(user) + "}:bucket:" + scopeHash(bucket)
}
func userScopeKey(scope, kind string) string { return "g:mutation:" + scope + ":" + kind }
func leaseKey(scope, token string) string    { return "g:mutation:" + scope + ":lease:" + token }

var beginMutationScript = redis.NewScript(`
local function prune(index)
  local members = redis.call('SMEMBERS', index)
  for _, lease in ipairs(members) do
    if redis.call('EXISTS', lease) == 0 then redis.call('SREM', index, lease) end
  end
end
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
prune(KEYS[3]); prune(KEYS[4])
redis.call('SET', KEYS[5], ARGV[1], 'PX', ARGV[2])
redis.call('SET', KEYS[6], ARGV[1], 'PX', ARGV[2])
redis.call('SADD', KEYS[3], KEYS[5]); redis.call('SADD', KEYS[4], KEYS[6])
return 1
`)

var beginTopologyScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return -1 end
local members = redis.call('SMEMBERS', KEYS[2])
local live = 0
for _, lease in ipairs(members) do
  if redis.call('EXISTS', lease) == 0 then redis.call('SREM', KEYS[2], lease)
  else live = live + 1 end
end
if live > 0 then return live end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 0
`)

var beginBucketTopologyScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then return -1 end
local members = redis.call('SMEMBERS', KEYS[2])
local live = 0
for _, lease in ipairs(members) do
  if redis.call('EXISTS', lease) == 0 then redis.call('SREM', KEYS[2], lease)
  else live = live + 1 end
end
local userMembers = redis.call('SMEMBERS', KEYS[4])
for _, lease in ipairs(userMembers) do
  if redis.call('EXISTS', lease) == 0 then redis.call('SREM', KEYS[4], lease) end
end
if live > 0 then return live end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SET', KEYS[5], ARGV[1], 'PX', ARGV[2])
redis.call('SADD', KEYS[4], KEYS[5])
return 0
`)

var renewMutationScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2]); redis.call('PEXPIRE', KEYS[2], ARGV[2])
return 1
`)

var renewTopologyScript = redis.NewScript(`
for _, key in ipairs(KEYS) do
  if redis.call('GET', key) ~= ARGV[1] then return 0 end
end
for _, key in ipairs(KEYS) do redis.call('PEXPIRE', key, ARGV[2]) end
return 1
`)

var releaseMutationScript = redis.NewScript(`
if redis.call('GET', KEYS[3]) == ARGV[1] then redis.call('DEL', KEYS[3]) end
if redis.call('GET', KEYS[4]) == ARGV[1] then redis.call('DEL', KEYS[4]) end
redis.call('SREM', KEYS[1], KEYS[3]); redis.call('SREM', KEYS[2], KEYS[4])
return 1
`)

var releaseTopologyScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end
if #KEYS >= 3 then
  if redis.call('GET', KEYS[3]) == ARGV[1] then redis.call('DEL', KEYS[3]) end
  redis.call('SREM', KEYS[2], KEYS[3])
end
return 1
`)
