package image

import (
	"archive/tar"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	nodev1 "github.com/laravel/zeropod/api/node/v1"
	"github.com/mholt/archives"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Client pulls images from the image services of other nodes.
type Client struct {
	tlsConfig *tls.Config
}

func NewClient(tlsConfig *tls.Config) *Client {
	return &Client{tlsConfig: tlsConfig}
}

type PullStats struct {
	Duration         time.Duration
	TransferredBytes int
}

// Pull pulls the image with imageID from the image service at addr and
// extracts it into dst.
func (c *Client) Pull(ctx context.Context, addr, imageID, dst string) (PullStats, error) {
	stats := PullStats{}
	beforePull := time.Now()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(c.tlsConfig)))
	if err != nil {
		return stats, fmt.Errorf("creating image service client: %w", err)
	}
	//nolint:errcheck
	defer conn.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cl, err := nodev1.NewImageServiceClient(conn).PullImage(ctx, &nodev1.PullImageRequest{ImageId: imageID})
	if err != nil {
		return stats, err
	}

	var extractErr error
	extractDone := make(chan struct{})
	r, w := io.Pipe()
	defer func() {
		_ = w.Close()
		<-extractDone
	}()
	go func() {
		defer close(extractDone)
		extractErr = extract(ctx, dst, r)
		// unblocks pending writes if extract stopped reading early
		r.CloseWithError(extractErr)
	}()
	for {
		img, err := cl.Recv()
		if err != nil {
			if err == io.EOF {
				_ = w.Close()
				break
			}
			return stats, err
		}
		n, err := w.Write(img.ImageData)
		if err != nil {
			return stats, fmt.Errorf("sending image data: %w", err)
		}
		stats.TransferredBytes += n
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}
	}
	<-extractDone
	if extractErr != nil {
		return stats, extractErr
	}
	stats.Duration = time.Since(beforePull)
	return stats, nil
}

func extract(ctx context.Context, dst string, reader io.ReadCloser) error {
	format := archives.CompressedArchive{
		Compression: archives.Zstd{},
		Extraction:  archives.Tar{},
	}

	if err := os.MkdirAll(dst, os.ModePerm); err != nil {
		return err
	}
	return format.Extract(ctx, reader, func(ctx context.Context, f archives.FileInfo) error {
		name := filepath.Join(dst, filepath.Clean(f.NameInArchive))
		header, ok := f.Sys().(*tar.Header)
		if f.IsDir() {
			if err := os.MkdirAll(name, f.Mode()); err != nil {
				return err
			}
			if err := os.Chmod(name, f.Mode()); err != nil {
				return err
			}
			if !ok {
				return nil
			}
			return os.Chown(name, int(header.Uid), int(header.Gid))
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}
		//nolint:errcheck
		defer rc.Close()

		file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}
		//nolint:errcheck
		defer file.Close()

		if _, err := io.Copy(file, rc); err != nil {
			return err
		}

		if err := os.Chmod(name, f.Mode()); err != nil {
			return err
		}

		if !ok {
			return nil
		}
		return os.Chown(name, int(header.Uid), int(header.Gid))
	})
}
