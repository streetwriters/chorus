package util

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteErrorMapsOfflineProviderToRetryableServiceUnavailable(t *testing.T) {
	for _, err := range []error{
		&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		context.DeadlineExceeded,
	} {
		response := httptest.NewRecorder()
		WriteError(context.Background(), response, err)
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Equal(t, "1", response.Header().Get("Retry-After"))
		require.Contains(t, response.Body.String(), "ServiceUnavailable")
	}
}
