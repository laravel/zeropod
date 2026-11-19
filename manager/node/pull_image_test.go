package node

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	nodev1 "github.com/laravel/zeropod/api/node/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestPullImageSlowConsumer(t *testing.T) {
	const (
		imageID   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		imageSize = 16 << 20
	)
	nodev1.SetImageBasePath(t.TempDir())
	snapshot := nodev1.SnapshotPath(imageID)
	require.NoError(t, os.MkdirAll(snapshot, os.ModePerm))
	// incompressible data so the image spans many chunks
	data := make([]byte, imageSize)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(snapshot, "pages-1.img"), data, 0o644))

	// both sides use the same mTLS setup as the managers
	ca, serverCert, clientCert := prepareTLS(t)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLSConfig(caPool, serverCert))))
	nodev1.RegisterImageServiceServer(srv, &nodeService{log: slog.Default()})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(l) }()
	conn, err := grpc.NewClient(l.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(serverTLSConfig(caPool, clientCert))))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})

	stream, err := nodev1.NewImageServiceClient(conn).PullImage(context.Background(), &nodev1.PullImageRequest{ImageId: imageID})
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
	assert.Greater(t, total, imageSize)
}
