package router

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/clyso/chorus/pkg/dom"
)

func TestDeleteObjectsPartialProviderResultReplicatesOnlySuccessfulKeys(t *testing.T) {
	request := deleteObjectsRequest{
		Objects: []objectID{{Key: "foo"}, {Key: "bar"}, {Key: "baz"}},
	}
	result := multiDeleteResult{
		Deleted: []objectID{{Key: "foo"}, {Key: "baz"}},
		Error:   []errorResult{{Key: "bar", Code: "AccessDenied"}},
	}
	objects := successfulDeleteObjects("bucket", request, result)
	require.Equal(t, []string{"foo", "baz"}, objectNames(objects))

	quiet := deleteObjectsRequest{Quiet: true, Objects: []objectID{{Key: "foo"}, {Key: "bar"}, {Key: "baz"}}}
	quietObjects := successfulDeleteObjects("bucket", quiet, multiDeleteResult{Error: []errorResult{{Key: "bar", Code: "AccessDenied"}}})
	require.Equal(t, []string{"foo", "baz"}, objectNames(quietObjects))
}

func TestDeleteObjectsPartialVersionedErrorDoesNotSuppressOtherVersion(t *testing.T) {
	request := deleteObjectsRequest{Quiet: true, Objects: []objectID{
		{Key: "foo", VersionID: "v1"}, {Key: "foo", VersionID: "v2"},
	}}
	result := multiDeleteResult{Error: []errorResult{{Key: "foo", VersionID: "v1", Code: "AccessDenied"}}}
	objects := successfulDeleteObjects("bucket", request, result)
	require.Equal(t, []string{"v2"}, objectVersions(objects))
}

func objectNames(objects []dom.Object) []string {
	result := make([]string, len(objects))
	for i, object := range objects {
		result[i] = object.Name
	}
	return result
}

func objectVersions(objects []dom.Object) []string {
	result := make([]string, len(objects))
	for i, object := range objects {
		result[i] = object.Version
	}
	return result
}
