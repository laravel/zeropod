package image_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	nodev1 "github.com/laravel/zeropod/api/node/v1"
	"github.com/laravel/zeropod/manager/image"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPull(t *testing.T) {
	addr, clientTLS, data := startImageService(t)
	dst := t.TempDir()

	stats, err := image.NewClient(clientTLS).Pull(context.Background(), addr, testImageID, dst)
	require.NoError(t, err)
	assert.Greater(t, stats.TransferredBytes, testImageSize)
	assert.NotZero(t, stats.Duration)

	got, err := os.ReadFile(filepath.Join(dst, nodev1.SnapshotSuffix, "pages-1.img"))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}
