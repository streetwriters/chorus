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

package router

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"time"

	mclient "github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	xctx "github.com/clyso/chorus/pkg/ctx"
	"github.com/clyso/chorus/pkg/dom"
	"github.com/clyso/chorus/pkg/entity"
	"github.com/clyso/chorus/pkg/s3client"
	"github.com/clyso/chorus/pkg/tasks"
)

const multipartUploadTrackingTTL = 7 * 24 * time.Hour

func (r *s3Router) createMultipartUpload(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)

	resp, storage, isApiErr, err = r.commonWrite(req)
	if err != nil || isApiErr {
		return
	}

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil && len(xctx.GetReplications(ctx)) == 0 {
		// No replication needs this upload's completion event.
		return
	}

	respBody := initiateMultipartUploadResult{}
	err = s3client.ExtractRespBody(resp, &respBody)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("unable to unmarshal initiateMultipartUploadResult response body")
		return
	}
	id := entity.NewUserUploadObjectID(user, bucket)
	val := entity.NewUserUploadObject(object, respBody.UploadID, storage)
	val.StartedAt = time.Now().UTC()
	ttl := multipartUploadTrackingTTL
	if inProgressSwitch != nil {
		ttl = inProgressSwitch.MultipartTTL
	}
	err = r.uploadSvc.StoreUpload(ctx, id, val, ttl)

	return
}

func (r *s3Router) completeMultipartUpload(req *http.Request) (resp *http.Response, taskList []tasks.ReplicationTask, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	uploadID := req.URL.Query().Get("uploadId")
	storage, _, err = r.routeMultipart(req)
	if err != nil {
		return
	}
	trackedUpload, err := r.uploadSvc.GetUpload(ctx, entity.NewUserUploadObjectID(user, bucket), object, uploadID)
	if err != nil {
		return nil, nil, "", false, err
	}

	client, err := r.clients.AsS3(ctx, storage, user)
	if err != nil {
		return nil, nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	var reconciledObject *mclient.ObjectInfo
	// CompleteMultipartUpload is not idempotent at S3: after a successful
	// completion, a retry with the same upload ID returns NoSuchUpload. Keep the
	// upload marker until its object event is queued and use HEAD to reconcile
	// that retry with the committed object.
	if isApiErr && err != nil && trackedUpload != nil && mclient.ToErrorResponse(err).Code == "NoSuchUpload" {
		if info, statErr := client.S3().StatObject(ctx, bucket, object, mclient.StatObjectOptions{}); statErr == nil &&
			(trackedUpload.StartedAt.IsZero() || !info.LastModified.Add(time.Second).Before(trackedUpload.StartedAt)) {
			reconciledObject = &info
			etag := info.ETag
			if !strings.HasPrefix(etag, `"`) {
				etag = `"` + etag + `"`
			}
			result, marshalErr := xml.Marshal(struct {
				XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
				Bucket  string   `xml:"Bucket"`
				Key     string   `xml:"Key"`
				ETag    string   `xml:"ETag"`
			}{Bucket: bucket, Key: object, ETag: etag})
			if marshalErr != nil {
				err = marshalErr
				return
			}
			resp = &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body:       io.NopCloser(bytes.NewReader(result)),
			}
			isApiErr = false
			err = nil
		} else {
			zerolog.Ctx(ctx).Warn().Err(statErr).Msg("unable to reconcile completed multipart upload retry")
		}
	}
	if err != nil || isApiErr {
		return
	}

	var res completeMultipartUploadResult
	xmlErr := s3client.ExtractRespBody(resp, &res)
	if xmlErr != nil {
		zerolog.Ctx(ctx).Err(xmlErr).Msg("unable to unmarshal response body")
	} else {
		zerolog.Ctx(ctx).Debug().Str("etag", res.ETag).Msg("multipart uploaded object with etag")
	}

	var objSize int64
	if reconciledObject != nil {
		objSize = reconciledObject.Size
	} else {
		objInfo, statErr := client.S3().StatObject(ctx, bucket, object, mclient.StatObjectOptions{})
		if statErr != nil {
			zerolog.Ctx(ctx).Err(statErr).Msg("unable to get uploaded object size")
		} else {
			objSize = objInfo.Size
		}
	}
	obj := dom.Object{
		Bucket:  bucket,
		Name:    object,
		Version: "", // versionID not supported for obj PUT (including multipart)
	}
	taskList = []tasks.ReplicationTask{
		&tasks.ObjectSyncPayload{
			Object:  obj,
			ObjSize: objSize,
			UploadID: func() string {
				if trackedUpload != nil {
					return uploadID
				}
				return ""
			}(),
			UploadStorage: func() string {
				if trackedUpload != nil {
					return trackedUpload.Storage
				}
				return ""
			}(),
		},
		&tasks.ObjSyncACLPayload{
			Object: obj,
		},
		&tasks.ObjSyncTagsPayload{
			Object: obj,
		},
	}
	return

}

func (r *s3Router) replicationStored(ctx context.Context, task tasks.ReplicationTask) error {
	objectTask, ok := task.(*tasks.ObjectSyncPayload)
	if !ok || objectTask.UploadID == "" {
		return nil
	}
	trackedUpload, err := r.uploadSvc.GetUpload(ctx,
		entity.NewUserUploadObjectID(xctx.GetUser(ctx), objectTask.Object.Bucket),
		objectTask.Object.Name, objectTask.UploadID)
	if err != nil || trackedUpload == nil {
		return err
	}
	return r.uploadSvc.DeleteUpload(ctx,
		entity.NewUserUploadObjectID(xctx.GetUser(ctx), objectTask.Object.Bucket), *trackedUpload)
}

func (r *s3Router) abortMultipartUpload(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user, bucket, object := xctx.GetUser(ctx), xctx.GetBucket(ctx), xctx.GetObject(ctx)
	storage, _, err = r.routeMultipart(req)
	if err != nil {
		return
	}

	client, err := r.clients.AsS3(ctx, storage, user)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	if err != nil || isApiErr {
		return
	}
	if trackedUpload, lookupErr := r.uploadSvc.GetUpload(ctx,
		entity.NewUserUploadObjectID(user, bucket), object, req.URL.Query().Get("uploadId")); lookupErr == nil && trackedUpload != nil {
		_ = r.uploadSvc.DeleteUpload(ctx, entity.NewUserUploadObjectID(user, bucket), *trackedUpload)
	}
	return
}

func (r *s3Router) listMultipartUploads(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	storage, err = r.routeListMultipart(req)
	if err != nil {
		return
	}
	client, err := r.clients.AsS3(ctx, storage, user)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

func (r *s3Router) uploadPart(req *http.Request) (resp *http.Response, storage string, isApiErr bool, err error) {
	ctx := req.Context()
	user := xctx.GetUser(ctx)
	storage, _, err = r.routeMultipart(req)
	if err != nil {
		return
	}

	client, err := r.clients.AsS3(ctx, storage, user)
	if err != nil {
		return nil, "", false, err
	}
	resp, isApiErr, err = client.Do(req)
	return
}

func (r *s3Router) routeMultipart(req *http.Request) (storage string, switchInProgress bool, err error) {
	ctx := req.Context()
	storage = xctx.GetRoutingPolicy(ctx)
	id := entity.NewUserUploadObjectID(xctx.GetUser(ctx), xctx.GetBucket(ctx))
	val := entity.NewUserUploadObject(xctx.GetObject(ctx), req.URL.Query().Get("uploadId"))
	trackedUpload, err := r.uploadSvc.GetUpload(ctx, id, val.Object, val.UploadID)
	if err != nil {
		return storage, false, err
	}

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		if trackedUpload != nil && trackedUpload.Storage != "" {
			return trackedUpload.Storage, false, nil
		}
		// no upload metadata exists and no switch is in progress
		return storage, false, nil
	}
	if trackedUpload != nil {
		// Upload metadata records the provider that accepted initiation. This
		// keeps pre-switch uploads on the old provider during migration.
		if trackedUpload.Storage != "" {
			return trackedUpload.Storage, true, nil
		}
		// Backward compatibility for markers written before storage was recorded.
		return storage, true, nil
	}
	// multipart upload was started before switch.
	// route to old storage
	oldReplicationID := inProgressSwitch.ReplicationID()
	return oldReplicationID.FromStorage(), true, nil
}

func (r *s3Router) routeListMultipart(req *http.Request) (storage string, err error) {
	ctx := req.Context()
	storage = xctx.GetRoutingPolicy(ctx)

	inProgressSwitch := xctx.GetInProgressZeroDowntime(ctx)
	if inProgressSwitch == nil {
		// no switch in progress
		return storage, nil
	}
	// todo: maybe better always return old?
	id := entity.NewUserUploadObjectID(xctx.GetUser(ctx), xctx.GetBucket(ctx))
	exists, err := r.uploadSvc.UploadsExistForUserBucket(ctx, id)

	if err != nil {
		return "", err
	}
	if exists {
		// multipart upload id exists in redis.
		// route to new storage
		return storage, nil
	}
	// multipart upload was started before switch.
	// route to old storage
	oldReplicationID := inProgressSwitch.ReplicationID()
	return oldReplicationID.FromStorage(), nil
}
