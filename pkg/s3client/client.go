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

package s3client

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	aws_credentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/clyso/chorus/pkg/metrics"
	"github.com/clyso/chorus/pkg/s3"
)

type Client interface {
	S3() *S3
	AWS() *AWS
	SNS() *sns.Client
	Do(req *http.Request) (*http.Response, bool, error)
}

// HealthReporter is optionally implemented by S3 clients that expose the
// provider connectivity state without expanding the main Client contract.
type HealthReporter interface {
	IsOnline() bool
	HealthKnown() bool
}

const (
	healthUnknown int32 = iota
	healthOffline
	healthOnline
)

type client struct {
	metricsSvc  metrics.Service
	c           *http.Client
	mc          *S3
	aws         *AWS
	sns         *sns.Client
	cred        s3.CredentialsV4
	storageName string
	userName    string
	conf        s3.StorageAddress
	health      atomic.Int32
}

func NewClient(ctx context.Context, metricsSvc metrics.Service, storageConf s3.StorageAddress, cred s3.CredentialsV4, storageName, userName string) (Client, error) {
	c := &client{
		c: &http.Client{
			Timeout: storageConf.HttpTimeout,
		},
		conf:        storageConf,
		storageName: storageName,
		userName:    userName,
		cred:        cred,
		metricsSvc:  metricsSvc,
	}
	host := strings.TrimPrefix(storageConf.Address, "http://")
	host = strings.TrimPrefix(host, "https://")

	mc, err := mclient.New(host, &mclient.Options{
		Creds:  credentials.NewStaticV4(c.cred.AccessKeyID, c.cred.SecretAccessKey, ""),
		Secure: storageConf.IsSecure,
	})
	if err != nil {
		return nil, err
	}
	c.mc = newMinioClient(storageName, userName, mc, metricsSvc)
	awsClient, err := newAWSClient(storageConf, cred, storageName, userName, metricsSvc)
	if err != nil {
		return nil, err
	}
	c.aws = awsClient
	snsEndpoint := storageConf.Address
	if !strings.HasPrefix(snsEndpoint, "http") {
		if storageConf.IsSecure {
			snsEndpoint = "https://" + snsEndpoint
		} else {
			snsEndpoint = "http://" + snsEndpoint
		}
	}

	c.sns = sns.NewFromConfig(aws.Config{
		Region:      "default",
		Credentials: aws_credentials.NewStaticCredentialsProvider(cred.AccessKeyID, cred.SecretAccessKey, ""),
		// EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		// 	return aws.Endpoint{URL: snsEndpoint}, nil
		// }),
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: snsEndpoint}, nil
		}),
	})
	// Provider availability is a runtime condition. Keep the client registered
	// so routing and management can start while a provider is offline.
	go func() {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		state := healthOffline
		if isOnline(probeCtx, c) == nil {
			state = healthOnline
		}
		c.health.CompareAndSwap(healthUnknown, state)
	}()

	return c, nil
}

// IsOnline reports the result of the most recent startup connectivity probe.
func (c *client) IsOnline() bool { return c.health.Load() == healthOnline }

func (c *client) HealthKnown() bool { return c.health.Load() != healthUnknown }

func isOnline(ctx context.Context, c *client) error {
	_, err := c.mc.GetBucketLocation(ctx, "probe-health-test")
	if err == nil {
		return nil
	} else if !mclient.IsNetworkOrHostDown(err, false) {
		switch mclient.ToErrorResponse(err).Code {
		case "NoSuchBucket", "AccessDenied", "":
			return nil
		}
	}
	return err
}

func (c *client) SNS() *sns.Client {
	return c.sns
}

func (c *client) AWS() *AWS {
	return c.aws
}

func (c *client) S3() *S3 {
	return c.mc
}
