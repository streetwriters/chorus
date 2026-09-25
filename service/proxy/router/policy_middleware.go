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

package router

import (
	"context"
	"fmt"
	"net/http"

	mclient "github.com/minio/minio-go/v7"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/policy"
	"github.com/clyso/chorus/pkg/s3"
	"github.com/clyso/chorus/pkg/store"
	"github.com/clyso/chorus/pkg/util"
)

func PolicyMiddleware(policySvc policy.Service, gates ...*store.BucketMutationGate) func(next http.Handler) http.Handler {
	var gate *store.BucketMutationGate
	if len(gates) != 0 {
		gate = gates[0]
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			user := xctx.GetUser(ctx)
			bucket := xctx.GetBucket(ctx)

			run := func(ctx context.Context) error {
				policyCtx, err := initPolicyContext(ctx, policySvc, user, bucket)
				if err != nil {
					util.WriteError(ctx, w, err)
					return nil
				}
				next.ServeHTTP(w, r.WithContext(policyCtx))
				return nil
			}
			if gate == nil || !isObjectStateMutation(xctx.GetMethod(ctx)) || user == "" || bucket == "" {
				_ = run(ctx)
				return
			}
			lease, err := gate.BeginMutation(ctx, user, bucket)
			if err != nil {
				w.Header().Set("Retry-After", "1")
				util.WriteError(ctx, w, mclient.ErrorResponse{
					Code:       "ServiceUnavailable",
					Message:    "Object mutations are temporarily unavailable. Retry the request.",
					StatusCode: http.StatusServiceUnavailable,
				})
				return
			}
			if err := lease.Run(ctx, run); err != nil {
				w.Header().Set("Retry-After", "1")
				util.WriteError(ctx, w, mclient.ErrorResponse{
					Code:       "ServiceUnavailable",
					Message:    "Object mutations are temporarily unavailable. Retry the request.",
					StatusCode: http.StatusServiceUnavailable,
				})
			}
		})
	}
}

func isObjectStateMutation(method s3.Method) bool {
	switch method {
	case s3.PutObject, s3.CopyObject, s3.DeleteObject, s3.DeleteObjects,
		s3.CompleteMultipartUpload, s3.PutObjectAcl, s3.PutObjectTagging,
		s3.DeleteObjectTagging, s3.PutObjectRetention, s3.PutObjectLegalHold:
		return true
	default:
		return false
	}
}

func initPolicyContext(ctx context.Context, policySvc policy.Service, user, bucket string) (context.Context, error) {
	if user == "" {
		return nil, fmt.Errorf("%w: user is not defined in proxy context", dom.ErrInternal)
	}
	if bucket == "" {
		return policySvc.BuildProxyNoBucketContext(ctx, user)
	}
	return policySvc.BuildProxyContext(ctx, user, bucket)
}
