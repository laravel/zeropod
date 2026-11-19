package image_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	nodev1 "github.com/laravel/zeropod/api/node/v1"
	"github.com/laravel/zeropod/manager/image"
	"github.com/laravel/zeropod/manager/node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	testImageID   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testImageSize = 16 << 20
)

// startImageService writes a test image and serves it. It returns the address
// of the image service and the client TLS config to connect to it.
func startImageService(t *testing.T) (string, *tls.Config, []byte) {
	nodev1.SetImageBasePath(t.TempDir())
	snapshot := nodev1.SnapshotPath(testImageID)
	require.NoError(t, os.MkdirAll(snapshot, os.ModePerm))
	// incompressible data so the image spans many chunks
	data := make([]byte, testImageSize)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(snapshot, "pages-1.img"), data, 0o644))

	// both sides use the same mTLS setup as the managers
	caCert, err := node.GenCert(nil, nil)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caCert.Certificate[0])
	require.NoError(t, err)
	caKey := caCert.PrivateKey.(ed25519.PrivateKey)
	serverCert, err := node.GenCert(ca, caKey, net.ParseIP("127.0.0.1"))
	require.NoError(t, err)
	clientCert, err := node.GenCert(ca, caKey, net.ParseIP("127.0.0.2"))
	require.NoError(t, err)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	is, err := image.NewImageService(slog.Default(), node.ServerTLSConfig(caPool, serverCert), "test", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go is.Start(ctx)

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(is.Port()))
	return addr, node.ServerTLSConfig(caPool, clientCert), data
}

func TestPullImageSlowConsumer(t *testing.T) {
	addr, clientTLS, _ := startImageService(t)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := nodev1.NewImageServiceClient(conn).PullImage(context.Background(), &nodev1.PullImageRequest{ImageId: testImageID})
	require.NoError(t, err)
	total := 0
	for i := 0; ; i++ {
		img, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		total += len(img.ImageData)
		// stall for longer than the 1s after which ttrpc used to fail the
		// stream, simulating a slow extraction
		if i == 0 {
			time.Sleep(2 * time.Second)
		}
	}
	assert.Greater(t, total, testImageSize)
}
