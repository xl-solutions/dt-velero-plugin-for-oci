package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type ociClientStub struct {
	head      func(context.Context, objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error)
	get       func(context.Context, objectstorage.GetObjectRequest) (objectstorage.GetObjectResponse, error)
	list      func(context.Context, objectstorage.ListObjectsRequest) (objectstorage.ListObjectsResponse, error)
	delete    func(context.Context, objectstorage.DeleteObjectRequest) (objectstorage.DeleteObjectResponse, error)
	abort     func(context.Context, objectstorage.AbortMultipartUploadRequest) (objectstorage.AbortMultipartUploadResponse, error)
	createPAR func(context.Context, objectstorage.CreatePreauthenticatedRequestRequest) (objectstorage.CreatePreauthenticatedRequestResponse, error)
}

func (s *ociClientStub) PutObject(context.Context, objectstorage.PutObjectRequest) (objectstorage.PutObjectResponse, error) {
	return objectstorage.PutObjectResponse{}, nil
}

func (s *ociClientStub) HeadObject(ctx context.Context, request objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error) {
	return s.head(ctx, request)
}

func (s *ociClientStub) GetObject(ctx context.Context, request objectstorage.GetObjectRequest) (objectstorage.GetObjectResponse, error) {
	return s.get(ctx, request)
}

func (s *ociClientStub) ListObjects(ctx context.Context, request objectstorage.ListObjectsRequest) (objectstorage.ListObjectsResponse, error) {
	return s.list(ctx, request)
}

func (s *ociClientStub) DeleteObject(ctx context.Context, request objectstorage.DeleteObjectRequest) (objectstorage.DeleteObjectResponse, error) {
	return s.delete(ctx, request)
}

func (s *ociClientStub) AbortMultipartUpload(ctx context.Context, request objectstorage.AbortMultipartUploadRequest) (objectstorage.AbortMultipartUploadResponse, error) {
	if s.abort == nil {
		return objectstorage.AbortMultipartUploadResponse{}, nil
	}
	return s.abort(ctx, request)
}

func (s *ociClientStub) CreatePreauthenticatedRequest(ctx context.Context, request objectstorage.CreatePreauthenticatedRequestRequest) (objectstorage.CreatePreauthenticatedRequestResponse, error) {
	return s.createPAR(ctx, request)
}

type ociUploaderStub struct {
	request  transfer.UploadStreamRequest
	response transfer.UploadResponse
	err      error
}

func (s *ociUploaderStub) UploadStream(_ context.Context, request transfer.UploadStreamRequest) (transfer.UploadResponse, error) {
	s.request = request
	return s.response, s.err
}

func testOCIStore(client ociObjectStorageClient) *OCIObjectStore {
	return &OCIObjectStore{
		log:       logrus.New(),
		client:    client,
		namespace: "namespace",
		endpoint:  "https://objectstorage.sa-saopaulo-1.oraclecloud.com",
	}
}

func TestNewOCIObjectStore(t *testing.T) {
	plugin, err := newOCIObjectStore(logrus.New())
	require.NoError(t, err)
	require.IsType(t, &OCIObjectStore{}, plugin)
}

func TestOCIObjectStoreInitRequiresRegionAndNamespace(t *testing.T) {
	for name, config := range map[string]map[string]string{
		"region": {
			bucketKey:       "bucket",
			ociNamespaceKey: "namespace",
		},
		"namespace": {
			regionKey: "sa-saopaulo-1",
			bucketKey: "bucket",
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := &OCIObjectStore{log: logrus.New()}
			err := store.Init(config)
			require.Error(t, err)
		})
	}
}

func TestOCIObjectStoreRejectsStaticCredentialConfig(t *testing.T) {
	store := &OCIObjectStore{log: logrus.New()}
	err := store.Init(map[string]string{
		regionKey:       "sa-saopaulo-1",
		ociNamespaceKey: "namespace",
		bucketKey:       "bucket",
		"accessKey":     "should-not-be-used",
	})
	require.Error(t, err)
}

func TestOCIObjectStorePutObjectUsesNativeMultipartSettings(t *testing.T) {
	uploader := &ociUploaderStub{}
	store := testOCIStore(&ociClientStub{})
	store.uploader = uploader
	store.sdkClient = &objectstorage.ObjectStorageClient{}

	require.NoError(t, store.PutObject("bucket", "backup/data", strings.NewReader("payload")))
	require.Equal(t, int64(10*1024*1024), *uploader.request.PartSize)
	require.Equal(t, 4, *uploader.request.NumberOfGoroutines)
	require.True(t, *uploader.request.AllowMultipartUploads)
	require.True(t, *uploader.request.AllowParrallelUploads)
	require.Equal(t, "namespace", *uploader.request.NamespaceName)
}

func TestOCIObjectStorePutObjectPropagatesMultipartFailure(t *testing.T) {
	aborted := false
	store := testOCIStore(&ociClientStub{})
	store.client = &ociClientStub{
		abort: func(_ context.Context, request objectstorage.AbortMultipartUploadRequest) (objectstorage.AbortMultipartUploadResponse, error) {
			aborted = request.UploadId != nil && *request.UploadId == "upload-id"
			return objectstorage.AbortMultipartUploadResponse{}, nil
		},
	}
	store.sdkClient = &objectstorage.ObjectStorageClient{}

	// The transfer manager returns the upload ID when a multipart operation
	// fails, allowing the provider to clean it up.
	store.uploader = &ociUploaderStub{
		err: errors.New("part 2 failed"),
		response: transfer.UploadResponse{
			MultipartUploadResponse: &transfer.MultipartUploadResponse{UploadID: stringPtr("upload-id")},
		},
	}
	err := store.PutObject("bucket", "backup/data", strings.NewReader("payload"))
	require.ErrorContains(t, err, "part 2 failed")
	require.True(t, aborted)
}

func TestOCIObjectStoreObjectExistsAndMissing(t *testing.T) {
	store := testOCIStore(&ociClientStub{
		head: func(_ context.Context, _ objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error) {
			return objectstorage.HeadObjectResponse{}, nil
		},
	})
	exists, err := store.ObjectExists("bucket", "present")
	require.NoError(t, err)
	require.True(t, exists)

	store.client = &ociClientStub{
		head: func(_ context.Context, _ objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error) {
			return objectstorage.HeadObjectResponse{RawResponse: &http.Response{StatusCode: http.StatusNotFound}}, errors.New("not found")
		},
	}
	exists, err = store.ObjectExists("bucket", "missing")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestOCIObjectStoreGetAndDelete(t *testing.T) {
	deleted := false
	store := testOCIStore(&ociClientStub{
		get: func(_ context.Context, _ objectstorage.GetObjectRequest) (objectstorage.GetObjectResponse, error) {
			return objectstorage.GetObjectResponse{Content: io.NopCloser(strings.NewReader("backup"))}, nil
		},
		delete: func(_ context.Context, request objectstorage.DeleteObjectRequest) (objectstorage.DeleteObjectResponse, error) {
			deleted = request.ObjectName != nil && *request.ObjectName == "backup/data"
			return objectstorage.DeleteObjectResponse{}, nil
		},
	})
	body, err := store.GetObject("bucket", "backup/data")
	require.NoError(t, err)
	data, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "backup", string(data))
	require.NoError(t, store.DeleteObject("bucket", "backup/data"))
	require.True(t, deleted)
}

func TestOCIObjectStoreListObjectsAndPrefixes(t *testing.T) {
	page := 0
	store := testOCIStore(&ociClientStub{
		list: func(_ context.Context, request objectstorage.ListObjectsRequest) (objectstorage.ListObjectsResponse, error) {
			page++
			if page == 1 {
				next := "backup/b"
				return objectstorage.ListObjectsResponse{ListObjects: objectstorage.ListObjects{
					Objects:       []objectstorage.ObjectSummary{{Name: stringPtr("backup/a")}},
					Prefixes:      []string{"backup/dir/"},
					NextStartWith: &next,
				}}, nil
			}
			require.NotNil(t, request.Start)
			return objectstorage.ListObjectsResponse{ListObjects: objectstorage.ListObjects{
				Objects:  []objectstorage.ObjectSummary{{Name: stringPtr("backup/b")}},
				Prefixes: []string{"backup/other/"},
			}}, nil
		},
	})
	objects, err := store.ListObjects("bucket", "backup/")
	require.NoError(t, err)
	require.Equal(t, []string{"backup/b", "backup/a"}, objects)
	page = 0
	prefixes, err := store.ListCommonPrefixes("bucket", "backup/", "/")
	require.NoError(t, err)
	require.Equal(t, []string{"backup/dir/", "backup/other/"}, prefixes)
}

func TestOCIObjectStoreCreateSignedURLUsesTTL(t *testing.T) {
	var request objectstorage.CreatePreauthenticatedRequestRequest
	store := testOCIStore(&ociClientStub{
		createPAR: func(_ context.Context, r objectstorage.CreatePreauthenticatedRequestRequest) (objectstorage.CreatePreauthenticatedRequestResponse, error) {
			request = r
			return objectstorage.CreatePreauthenticatedRequestResponse{
				PreauthenticatedRequest: objectstorage.PreauthenticatedRequest{AccessUri: stringPtr("/p/token/n/namespace/b/bucket/o/backup")},
			}, nil
		},
	})

	started := time.Now()
	url, err := store.CreateSignedURL("bucket", "backup", 15*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "https://objectstorage.sa-saopaulo-1.oraclecloud.com/p/token/n/namespace/b/bucket/o/backup", url)
	require.NotNil(t, request.TimeExpires)
	require.True(t, request.TimeExpires.Time.After(started.Add(14*time.Minute)))
	require.Equal(t, objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeObjectread, request.AccessType)
}

func TestOCIObjectStoreCreateSignedURLRejectsNonPositiveTTL(t *testing.T) {
	store := testOCIStore(&ociClientStub{})
	_, err := store.CreateSignedURL("bucket", "backup", 0)
	require.Error(t, err)
}
