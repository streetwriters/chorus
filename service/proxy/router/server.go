/*
 * Copyright © 2023 Clyso GmbH
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

package router

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/log"
	"github.com/clyso/chorus/pkg/objstore"
	"github.com/clyso/chorus/pkg/replication"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/tasks"
	"github.com/clyso/chorus/pkg/util"
)

type Config struct {
	Storages          map[dom.StorageType]StorageProxy
	LogMiddleware     func(next http.Handler) http.Handler
	TraceMiddleware   func(next http.Handler) http.Handler
	MetricsMiddleware func(next http.Handler) http.Handler
	PolicyMiddleware  func(next http.Handler) http.Handler
}

type StorageProxy struct {
	Router             Router
	Replicator         replication.Service
	ObjectLocker       *store.ObjectLocker
	AuthMiddleware     func(next http.Handler) http.Handler
	ReqParseMiddleware func(next http.Handler) http.Handler
}

func (c *Config) Validate() error {
	if len(c.Storages) == 0 {
		return fmt.Errorf("%w: at least one storage proxy must be configured", dom.ErrInternal)
	}
	for storType, storProxy := range c.Storages {
		if storProxy.Router == nil {
			return fmt.Errorf("%w: router is not configured for storage type %s", dom.ErrInternal, storType)
		}
		if storProxy.Replicator == nil {
			return fmt.Errorf("%w: replicator service is not configured for storage type %s", dom.ErrInternal, storType)
		}
		if storProxy.ReqParseMiddleware == nil {
			return fmt.Errorf("%w: request parse middleware is not configured for storage type %s", dom.ErrInternal, storType)
		}
	}
	if c.LogMiddleware == nil {
		return fmt.Errorf("%w: log middleware is not configured", dom.ErrInternal)
	}
	if c.PolicyMiddleware == nil {
		return fmt.Errorf("%w: policy middleware is not configured", dom.ErrInternal)
	}
	return nil
}

func New(config Config) (http.Handler, error) {
	// build proxy handlers. Middleware wraps request handler, so it is applied in reverse order
	if err := config.Validate(); err != nil {
		return nil, err
	}
	handlers := make(map[dom.StorageType]http.Handler, len(config.Storages))
	for storType, storProxy := range config.Storages {
		// 8. main request handler. Forward request to storage backend and emit replication tasks
		handler := serve(storProxy.Router, storProxy.Replicator, storProxy.ObjectLocker)
		if config.TraceMiddleware != nil {
			// 7. tracing
			handler = config.TraceMiddleware(handler)
		}
		if config.MetricsMiddleware != nil {
			// 6. metrics collection
			handler = config.MetricsMiddleware(handler)
		}
		// 5. set routing & replication policies to context
		handler = config.PolicyMiddleware(handler)
		// 4. parse storage-specific request method/user/bucket/object and set to context
		handler = storProxy.ReqParseMiddleware(handler)
		if storProxy.AuthMiddleware != nil {
			// 3. storage-specific auth check
			handler = storProxy.AuthMiddleware(handler)
		}
		handlers[storType] = handler
	}
	var result http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 2. detect storage type from request
		storType := objstore.StorageTypeFromRequest(r)
		ctx := log.WithStorType(r.Context(), storType)
		handler, ok := handlers[storType]
		if !ok {
			util.WriteError(ctx, w, fmt.Errorf("%w: proxy handler is not configured for storage type %s", dom.ErrInternal, storType))
			return
		}
		handler.ServeHTTP(w, r.WithContext(ctx))
	})

	// 1. init request scoped logger
	result = config.LogMiddleware(result)
	return result, nil

}

func serve(router Router, replSvc replication.Service, lockers ...*store.ObjectLocker) http.Handler {
	var objectLocker *store.ObjectLocker
	if len(lockers) != 0 {
		objectLocker = lockers[0]
	}
	// Use a custom handler function instead of ServeMux to avoid automatic redirects
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("").Start(r.Context(), "Route")
		defer span.End()
		r = r.WithContext(ctx)
		logger := zerolog.Ctx(r.Context())
		logger.Debug().Msg("proxy: new request received")

		var result routeResult
		run := func() error {
			result = routeAndReplicate(ctx, router, replSvc, r)
			return result.err
		}
		if lockIDs := deleteObjectLockIDs(ctx); objectLocker != nil && len(lockIDs) != 0 {
			if err := withObjectLocks(ctx, objectLocker, lockIDs, 0, run); err != nil {
				if result.err == nil {
					result.err = err
				}
			}
		} else if err := run(); err != nil {
			result.err = err
		}
		if result.err != nil {
			if result.retryAfter || xctx.GetMethod(ctx) == s3.DeleteObject {
				w.Header().Set("Retry-After", "1")
			}
			util.WriteError(ctx, w, result.err)
			return
		}
		resp := result.resp
		defer func() {
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
		}()
		// Forward response to original client
		for k, v := range resp.Header {
			w.Header().Set(k, v[0])
		}
		w.WriteHeader(resp.StatusCode)
		if _, err := io.Copy(w, resp.Body); err != nil {
			logger.Err(err).Msg("unable to copy response body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	})
}

type routeResult struct {
	resp       *http.Response
	err        error
	retryAfter bool
}

func routeAndReplicate(ctx context.Context, router Router, replSvc replication.Service, req *http.Request) routeResult {
	resp, taskList, storage, isApiErr, err := router.Route(req)
	if err != nil {
		return routeResult{err: err}
	}
	ctx = log.WithStorage(ctx, storage)
	if isApiErr {
		zerolog.Ctx(ctx).Info().Msg("s3 api error returned")
		return routeResult{resp: resp}
	}
	replCtx, cancel := log.StartNew(ctx)
	defer cancel()
	for _, task := range taskList {
		if replErr := replSvc.Replicate(replCtx, storage, task); replErr != nil {
			zerolog.Ctx(ctx).Err(replErr).Msg("unable to handle replication")
			if isObjectSyncTask(task) {
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				return routeResult{retryAfter: true, err: mclient.ErrorResponse{
					Code:       "ServiceUnavailable",
					Message:    "The write succeeded on the active provider but its replication event could not be stored. Retry the request.",
					StatusCode: http.StatusServiceUnavailable,
				}}
			}
		}
	}
	return routeResult{resp: resp}
}

func deleteObjectLockIDs(ctx context.Context) []entity.ObjectLockID {
	if xctx.GetMethod(ctx) != s3.DeleteObject || xctx.GetObject(ctx) == "" {
		return nil
	}
	replications := xctx.GetReplications(ctx)
	switchInfo := xctx.GetInProgressZeroDowntime(ctx)
	if switchInfo == nil {
		switchInfo = xctx.GetCompletedZeroDowntime(ctx)
	}
	if len(replications) == 0 && switchInfo == nil {
		return nil
	}
	bucket, object, version := xctx.GetBucket(ctx), xctx.GetObject(ctx), xctx.GetObjectVer(ctx)
	ids := make(map[entity.ObjectLockID]struct{})
	add := func(storage, targetBucket string) {
		if storage != "" && targetBucket != "" {
			ids[entity.NewVersionedObjectLockID(storage, targetBucket, object, version)] = struct{}{}
		}
	}
	add(xctx.GetRoutingPolicy(ctx), bucket)
	for _, replicationID := range replications {
		_, toBucket := replicationID.FromToBuckets(bucket)
		add(replicationID.ToStorage(), toBucket)
	}
	if switchInfo != nil {
		id := switchInfo.ReplicationID()
		_, toBucket := id.FromToBuckets(bucket)
		add(id.ToStorage(), toBucket)
		// A B->A recovery policy may be installed after this request's policy
		// context was read. Lock that potential destination as well so an
		// already-started recovery copy cannot race this delete handoff.
		reverseID := id.Swap()
		_, reverseToBucket := reverseID.FromToBuckets(bucket)
		add(reverseID.ToStorage(), reverseToBucket)
	}
	result := make([]entity.ObjectLockID, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Storage == result[j].Storage {
			return result[i].Bucket < result[j].Bucket
		}
		return result[i].Storage < result[j].Storage
	})
	return result
}

func withObjectLocks(ctx context.Context, locker *store.ObjectLocker, ids []entity.ObjectLockID, index int, work func() error) error {
	if index == len(ids) {
		return work()
	}
	lock, err := locker.Lock(ctx, ids[index], store.WithRetry(true))
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	return lock.Do(ctx, 2*time.Second, func() error {
		return withObjectLocks(ctx, locker, ids, index+1, work)
	})
}

func isObjectSyncTask(task tasks.ReplicationTask) bool {
	switch task.(type) {
	case *tasks.ObjectSyncPayload:
		return true
	default:
		return false
	}
}
