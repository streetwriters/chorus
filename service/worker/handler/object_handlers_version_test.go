package handler

import (
	"testing"

	"github.com/clyso/chorus/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestDelayedDeleteCannotRemoveNewerTargetObject(t *testing.T) {
	require.False(t, shouldApplyDeleteVersion(meta.Version{From: 2, To: 2}), "a delete from A must not remove a newer/equal B version")
	require.False(t, shouldApplyDeleteVersion(meta.Version{From: 1, To: 3}), "a stale A delete must not remove newer B data")
	require.True(t, shouldApplyDeleteVersion(meta.Version{From: 3, To: 2}), "a delete newer than the target still applies")
	require.True(t, shouldApplyDeleteVersion(meta.Version{}), "explicit delete intent remains actionable without version metadata")
}
