package handler

import (
	"testing"

	"github.com/clyso/chorus/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestDelayedDeleteCannotRemoveNewerTargetObject(t *testing.T) {
	require.False(t, shouldApplyDeleteVersion(2, meta.Version{From: 2, To: 2}), "a delete from A must not remove a newer/equal B version")
	require.False(t, shouldApplyDeleteVersion(2, meta.Version{From: 3, To: 2}), "a stale v2 delete must not remove v3 source state")
	require.False(t, shouldApplyDeleteVersion(3, meta.Version{From: 3, To: 3}), "an already-applied delete is idempotent")
	require.True(t, shouldApplyDeleteVersion(3, meta.Version{From: 3, To: 2}), "a matching delete newer than the target applies")
	require.False(t, shouldApplyDeleteVersion(0, meta.Version{From: 3, To: 1}), "legacy versionless deletes are conservative with known state")
	require.True(t, shouldApplyDeleteVersion(0, meta.Version{}), "legacy explicit delete can apply only when no state is known")
}
