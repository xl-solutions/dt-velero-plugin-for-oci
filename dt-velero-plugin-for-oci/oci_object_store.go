/*
Copyright 2026 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	veleroplugin "github.com/vmware-tanzu/velero/pkg/plugin/framework"
)

const (
	ociNamespaceKey      = "ociNamespace"
	ociMultipartPartSize = int64(10 * 1024 * 1024)
	ociMultipartWorkers  = 4
)

type ociObjectStorageClient interface {
	PutObject(context.Context, objectstorage.PutObjectRequest) (objectstorage.PutObjectResponse, error)
	GetObject(context.Context, objectstorage.GetObjectRequest) (objectstorage.GetObjectResponse, error)
	HeadObject(context.Context, objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error)
	ListObjects(context.Context, objectstorage.ListObjectsRequest) (objectstorage.ListObjectsResponse, error)
	DeleteObject(context.Context, objectstorage.DeleteObjectRequest) (objectstorage.DeleteObjectResponse, error)
	AbortMultipartUpload(context.Context, objectstorage.AbortMultipartUploadRequest) (objectstorage.AbortMultipartUploadResponse, error)
	CreatePreauthenticatedRequest(context.Context, objectstorage.CreatePreauthenticatedRequestRequest) (objectstorage.CreatePreauthenticatedRequestResponse, error)
}

type ociStreamUploader interface {
	UploadStream(context.Context, transfer.UploadStreamRequest) (transfer.UploadResponse, error)
}

// OCIObjectStore implements Velero's object store contract using the native
// OCI Object Storage API. It deliberately does not inspect AWS credentials,
// OCI config files, or AWS web identity variables.
type OCIObjectStore struct {
	log       logrus.FieldLogger
	client    ociObjectStorageClient
	uploader  ociStreamUploader
	sdkClient *objectstorage.ObjectStorageClient
	endpoint  string
	namespace string
}

func newOCIObjectStore(logger logrus.FieldLogger) (interface{}, error) {
	return &OCIObjectStore{log: logger}, nil
}

func (o *OCIObjectStore) Init(config map[string]string) error {
	if err := veleroplugin.ValidateObjectStoreConfigKeys(config, regionKey, ociNamespaceKey); err != nil {
		return err
	}

	region := strings.TrimSpace(config[regionKey])
	namespace := strings.TrimSpace(config[ociNamespaceKey])
	bucket := strings.TrimSpace(config[bucketKey])
	if region == "" {
		return errors.Errorf("%s is required for the OCI object store", regionKey)
	}
	if namespace == "" {
		return errors.Errorf("%s is required for the OCI object store", ociNamespaceKey)
	}
	if bucket == "" {
		return errors.Errorf("%s is required for the OCI object store", bucketKey)
	}

	// This is intentionally the only authentication path for this provider.
	// The OCI SDK reads the projected OKE service-account token and exchanges it
	// for a resource principal token. There is no static-key or AWS fallback.
	provider, err := auth.OkeWorkloadIdentityConfigurationProvider()
	if err != nil {
		return errors.Wrap(err, "could not initialize OCI OKE Workload Identity provider")
	}

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return errors.Wrap(err, "could not create OCI Object Storage client")
	}
	client.SetRegion(region)

	o.client = &client
	o.sdkClient = &client
	o.uploader = transfer.NewUploadManager()
	o.endpoint = client.Host
	o.namespace = namespace
	o.log.WithFields(logrus.Fields{
		"region":    region,
		"namespace": namespace,
		"bucket":    bucket,
	}).Info("OCI object store initialized with OKE Workload Identity")
	return nil
}

func (o *OCIObjectStore) PutObject(bucket, key string, body io.Reader) error {
	if o.client == nil || o.uploader == nil || o.sdkClient == nil {
		return errors.New("OCI object store is not initialized")
	}

	partSize := ociMultipartPartSize
	workers := ociMultipartWorkers
	allowMultipart := true
	allowParallel := true
	// The OCI transfer manager performs the native multipart upload. Its stream
	// uploader returns the multipart upload ID on failure, so abort it here to
	// avoid leaving incomplete uploads behind.
	response, err := o.uploader.UploadStream(context.Background(), transfer.UploadStreamRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:         stringPtr(o.namespace),
			BucketName:            stringPtr(bucket),
			ObjectName:            stringPtr(key),
			PartSize:              &partSize,
			AllowMultipartUploads: &allowMultipart,
			AllowParrallelUploads: &allowParallel,
			NumberOfGoroutines:    &workers,
			ObjectStorageClient:   o.sdkClient,
		},
		StreamReader: body,
	})
	if err != nil {
		if response.MultipartUploadResponse != nil && response.MultipartUploadResponse.UploadID != nil {
			abortErr := o.client.AbortMultipartUpload(context.Background(), objectstorage.AbortMultipartUploadRequest{
				NamespaceName: stringPtr(o.namespace),
				BucketName:    stringPtr(bucket),
				ObjectName:    stringPtr(key),
				UploadId:      response.MultipartUploadResponse.UploadID,
			})
			if abortErr != nil {
				return errors.Wrapf(err, "error putting OCI object %s; aborting multipart upload also failed: %v", key, abortErr)
			}
		}
		return errors.Wrapf(err, "error putting OCI object %s", key)
	}
	return nil
}

func (o *OCIObjectStore) ObjectExists(bucket, key string) (bool, error) {
	if o.client == nil {
		return false, errors.New("OCI object store is not initialized")
	}

	response, err := o.client.HeadObject(context.Background(), objectstorage.HeadObjectRequest{
		NamespaceName: stringPtr(o.namespace),
		BucketName:    stringPtr(bucket),
		ObjectName:    stringPtr(key),
	})
	if err == nil {
		return true, nil
	}
	if response.RawResponse != nil && response.RawResponse.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, errors.Wrapf(err, "error checking OCI object %s", key)
}

func (o *OCIObjectStore) GetObject(bucket, key string) (io.ReadCloser, error) {
	if o.client == nil {
		return nil, errors.New("OCI object store is not initialized")
	}

	response, err := o.client.GetObject(context.Background(), objectstorage.GetObjectRequest{
		NamespaceName: stringPtr(o.namespace),
		BucketName:    stringPtr(bucket),
		ObjectName:    stringPtr(key),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error getting OCI object %s", key)
	}
	if response.Content == nil {
		return nil, errors.Errorf("OCI returned an empty response body for object %s", key)
	}
	return response.Content, nil
}

func (o *OCIObjectStore) ListCommonPrefixes(bucket, prefix, delimiter string) ([]string, error) {
	_, prefixes, err := o.list(bucket, prefix, delimiter)
	if err != nil {
		return nil, err
	}
	sort.Strings(prefixes)
	return prefixes, nil
}

func (o *OCIObjectStore) ListObjects(bucket, prefix string) ([]string, error) {
	objects, _, err := o.list(bucket, prefix, "")
	if err != nil {
		return nil, err
	}
	// Keep the same reverse ordering used by the AWS implementation so Velero
	// removes object keys before pseudo-folder keys.
	sort.Sort(sort.Reverse(sort.StringSlice(objects)))
	return objects, nil
}

func (o *OCIObjectStore) list(bucket, prefix, delimiter string) ([]string, []string, error) {
	if o.client == nil {
		return nil, nil, errors.New("OCI object store is not initialized")
	}

	var objects []string
	var prefixes []string
	var start *string
	for {
		delimiterValue := (*string)(nil)
		if delimiter != "" {
			delimiterValue = stringPtr(delimiter)
		}
		request := objectstorage.ListObjectsRequest{
			NamespaceName: stringPtr(o.namespace),
			BucketName:    stringPtr(bucket),
			Prefix:        stringPtr(prefix),
			Delimiter:     delimiterValue,
			Start:         start,
		}
		response, err := o.client.ListObjects(context.Background(), request)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "error listing OCI objects under %s", prefix)
		}
		for _, object := range response.Objects {
			if object.Name != nil {
				objects = append(objects, *object.Name)
			}
		}
		prefixes = append(prefixes, response.Prefixes...)
		if response.NextStartWith == nil || *response.NextStartWith == "" {
			break
		}
		if start != nil && *start == *response.NextStartWith {
			return nil, nil, errors.New("OCI list pagination did not advance")
		}
		next := *response.NextStartWith
		start = &next
	}
	return objects, prefixes, nil
}

func (o *OCIObjectStore) DeleteObject(bucket, key string) error {
	if o.client == nil {
		return errors.New("OCI object store is not initialized")
	}
	_, err := o.client.DeleteObject(context.Background(), objectstorage.DeleteObjectRequest{
		NamespaceName: stringPtr(o.namespace),
		BucketName:    stringPtr(bucket),
		ObjectName:    stringPtr(key),
	})
	return errors.Wrapf(err, "error deleting OCI object %s", key)
}

func (o *OCIObjectStore) CreateSignedURL(bucket, key string, ttl time.Duration) (string, error) {
	if o.client == nil {
		return "", errors.New("OCI object store is not initialized")
	}
	if ttl <= 0 {
		return "", errors.New("signed URL TTL must be greater than zero")
	}

	expires := time.Now().Add(ttl)
	nameHash := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", bucket, key, expires.UnixNano())))
	parName := "velero-" + hex.EncodeToString(nameHash[:8])
	response, err := o.client.CreatePreauthenticatedRequest(context.Background(), objectstorage.CreatePreauthenticatedRequestRequest{
		NamespaceName: stringPtr(o.namespace),
		BucketName:    stringPtr(bucket),
		CreatePreauthenticatedRequestDetails: objectstorage.CreatePreauthenticatedRequestDetails{
			AccessType:          objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeObjectread,
			BucketListingAction: objectstorage.PreauthenticatedRequestBucketListingActionDeny,
			Name:                stringPtr(parName),
			ObjectName:          stringPtr(key),
			TimeExpires:         &common.SDKTime{Time: expires},
		},
	})
	if err != nil {
		return "", errors.Wrap(err, "error creating OCI pre-authenticated request")
	}
	if response.AccessUri == nil || *response.AccessUri == "" {
		return "", errors.New("OCI pre-authenticated request did not return an access URI")
	}
	if strings.HasPrefix(*response.AccessUri, "http://") || strings.HasPrefix(*response.AccessUri, "https://") {
		return *response.AccessUri, nil
	}
	return strings.TrimRight(o.endpoint, "/") + "/" + strings.TrimLeft(*response.AccessUri, "/"), nil
}

func stringPtr(value string) *string {
	return &value
}
