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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/meta"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/service/worker/copy"
)

func (s *svc) HandleObjectSync(ctx context.Context, t *asynq.Task) (err error) {
	var p tasks.ObjectSyncPayload
	if err = json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("ObjectSyncPayload Unmarshal failed: %w: %w", err, asynq.SkipRetry)
	}
	ctx = log.WithBucket(ctx, p.Object.Bucket)
	ctx = log.WithObjName(ctx, p.Object.Name)
	ctx = log.WithUser(ctx, p.ID.User())
	logger := zerolog.Ctx(ctx)
	fromBucket, toBucket := p.ID.FromToBuckets(p.Object.Bucket)

	// acquire rate limits for source and destination storage before proceeding
	if err := s.rateLimit(ctx, p.ID.FromStorage(), s3.HeadObject, s3.GetObject, s3.GetObjectAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.FromStorage()).Msg("rate limit error")
		return err
	}
	if err := s.rateLimit(ctx, p.ID.ToStorage(), s3.HeadObject, s3.PutObject, s3.PutObjectAcl); err != nil {
		logger.Debug().Err(err).Str(log.Storage, p.ID.ToStorage()).Msg("rate limit error")
		return err
	}

	objectLockID := entity.NewVersionedObjectLockID(p.ID.ToStorage(), toBucket, p.Object.Name, p.Object.Version)
	lock, err := s.objectLocker.Lock(ctx, objectLockID, store.WithRetry(true))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	versions, err := s.versionSvc.GetObj(ctx, p.ID, p.Object)
	if err != nil {
		return err
	}
	if p.Deleted {
		// Explicit delete intent remains authoritative when no version exists,
		// but a delayed source delete must identify and still match its version.
		if !shouldApplyDeleteVersion(p.FromVersion, versions) {
			logger.Info().Int64("task_from_ver", p.FromVersion).Int("from_ver", versions.From).Int("to_ver", versions.To).Msg("object delete: skip stale source delete")
			return nil
		}
		if err := s.objectDelete(ctx, p); err != nil {
			return err
		}
		if err := s.enqueueSwitchRepairFollowers(ctx, p, toBucket); err != nil {
			return err
		}
		if p.FromVersion > 0 {
			destination := meta.Destination{Storage: p.ID.ToStorage(), Bucket: toBucket}
			return s.versionSvc.UpdateIfGreater(ctx, p.ID, p.Object, destination, int(p.FromVersion))
		}
		return nil
	}
	if versions.IsEmpty() {
		// Absence of version metadata is not proof of a delete. Older or
		// partially repaired events must never erase a destination object.
		logger.Warn().Msg("object sync: skip event without version or delete intent")
		return nil
	}

	fromVer, toVer := versions.From, versions.To
	if p.FromVersion > 0 && int(p.FromVersion) != fromVer {
		logger.Info().Int64("task_from_ver", p.FromVersion).Int("from_ver", fromVer).Msg("object sync: skip stale source event")
		return nil
	}
	if fromVer <= toVer {
		logger.Info().Int("from_ver", fromVer).Int("to_ver", toVer).Msg("object sync: identical from/to obj version: skip copy")
		return nil
	}

	err = lock.Do(ctx, time.Second*2, func() error {
		return s.copySvc.CopyObject(ctx, p.ID.User(), copy.File{
			Storage: p.ID.FromStorage(),
			Bucket:  fromBucket,
			Name:    p.Object.Name,
		}, copy.File{
			Storage: p.ID.ToStorage(),
			Bucket:  toBucket,
			Name:    p.Object.Name,
		})
	})
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			logger.Warn().Msg("object sync: skip object sync: object missing in source")
			return nil
		}
		return err
	}
	if err := s.enqueueSwitchRepairFollowers(ctx, p, toBucket); err != nil {
		return err
	}
	logger.Info().Msg("object sync: done")

	if fromVer != 0 {
		destination := meta.Destination{Storage: p.ID.ToStorage(), Bucket: toBucket}
		return s.versionSvc.UpdateIfGreater(ctx, p.ID, p.Object, destination, fromVer)
	}

	return nil
}

// enqueueSwitchRepairFollowers forwards a delayed repair applied to a promoted
// target to that target's active bucket replication policies. Proxy mutations
// already fan out to all active policies; a Worker copy from the old source
// bypasses Proxy, so without this step a follower added during backlog would
// miss objects repaired after its initial listing completed.
func (s *svc) enqueueSwitchRepairFollowers(ctx context.Context, p tasks.ObjectSyncPayload, targetBucket string) error {
	if s.replicationPolicySvc == nil {
		return nil
	}
	policies, err := s.replicationPolicySvc.ListBucketReplicationsInfo(ctx, p.ID.User())
	if err != nil {
		if errors.Is(err, dom.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("list repaired target followers: %w", err)
	}
	var switchInfo *entity.ReplicationSwitchInfo
	for policy, status := range policies {
		policyID := entity.UniversalFromBucketReplication(policy)
		if policyID.AsString() == p.ID.AsString() && status.Switch != nil && status.Switch.ReplicationIDStr == p.ID.AsString() {
			switchInfo = status.Switch
			break
		}
	}
	if switchInfo == nil || (switchInfo.LastStatus != entity.StatusPromotedWithBacklog && switchInfo.LastStatus != entity.StatusDone) {
		return nil
	}
	for policy, status := range policies {
		if policy.FromStorage != p.ID.ToStorage() || policy.FromBucket != targetBucket || policy.ToStorage == p.ID.FromStorage() {
			continue
		}
		if status.ReplicationStatus == nil || status.IsArchived {
			continue
		}
		replicationID := entity.UniversalFromBucketReplication(policy)
		version, err := s.versionSvc.IncrementObj(ctx, replicationID, p.Object, meta.Destination{Storage: policy.FromStorage, Bucket: targetBucket})
		if err != nil {
			return fmt.Errorf("advance repaired target follower version: %w", err)
		}
		followerTask := p
		followerTask.FromVersion = int64(version)
		followerTask.SetReplicationID(replicationID)
		if err := s.queueSvc.EnqueueTask(ctx, &followerTask); err != nil {
			return fmt.Errorf("enqueue repaired target follower event: %w", err)
		}
	}
	return nil
}

func shouldApplyDeleteVersion(taskVersion int64, versions meta.Version) bool {
	if taskVersion == 0 {
		// Legacy delete tasks carry no ordering identity. Only apply when no
		// newer state is known; otherwise preserve the object conservatively.
		return versions.IsEmpty()
	}
	return taskVersion == int64(versions.From) && versions.From > versions.To
}

func (s *svc) objectDelete(ctx context.Context, p tasks.ObjectSyncPayload) (err error) {
	_, toClient, err := s.getClients(ctx, p.ID.User(), p.ID.FromStorage(), p.ID.ToStorage())
	if err != nil {
		return err
	}
	_, toBucket := p.ID.FromToBuckets(p.Object.Bucket)
	err = toClient.S3().RemoveObject(ctx, toBucket, p.Object.Name, mclient.RemoveObjectOptions{VersionID: p.Object.Version})
	return
}
