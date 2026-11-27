// Package image provides the gRPC service to pull checkpoint images from other
// nodes.
package image

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"regexp"

	"github.com/klauspost/compress/zstd"
	nodev1 "github.com/laravel/zeropod/api/node/v1"
	"github.com/mholt/archives"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var imageIDRegexp = regexp.MustCompile("^[A-Fa-f0-9]{64}$")

// ImageService serves checkpoint images over gRPC.
type ImageService struct {
	nodev1.UnimplementedImageServiceServer
	log      *slog.Logger
	grpc     *grpc.Server
	listener net.Listener
}

func NewImageService(log *slog.Logger, tlsConfig *tls.Config, nodeName, addr string) (*ImageService, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("creating net listener: %w", err)
	}

	is := &ImageService{
		log:      log.With("node", nodeName),
		grpc:     grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig))),
		listener: listener,
	}
	nodev1.RegisterImageServiceServer(is.grpc, is)

	is.log.Info("new image server")
	return is, nil
}

// Port returns the port the image service is listening on.
func (is *ImageService) Port() int {
	return is.listener.Addr().(*net.TCPAddr).Port
}

// Start serves the image service until ctx is done.
func (is *ImageService) Start(ctx context.Context) {
	defer is.grpc.Stop()
	go func() {
		if err := is.grpc.Serve(is.listener); err != nil {
			is.log.Error("serving grpc listener", "error", err)
		}
	}()

	is.log.Info("starting image server", "socket", is.listener.Addr())

	<-ctx.Done()
	is.log.Info("stopping image server")
}

// PullImage allows the caller to pull a compressed image from the server
func (is *ImageService) PullImage(req *nodev1.PullImageRequest, imageStream grpc.ServerStreamingServer[nodev1.Image]) error {
	ctx := imageStream.Context()
	log := is.log.With("image_id", req.ImageId)
	log.Info("got pull image request")
	if !imageIDRegexp.MatchString(req.ImageId) {
		log.Error("requested image_id is invalid")
		return fmt.Errorf("invalid image_id requested: %s", req.ImageId)
	}

	files, err := filesToArchive(ctx, log, req)
	if err != nil {
		return fmt.Errorf("unable to archive checkpoint: %w", err)
	}

	format := archives.CompressedArchive{
		Compression: archives.Zstd{
			EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedFastest)},
		},
		Archival: archives.Tar{},
	}
	// split our image into 64KiB chunks
	const chunkSize = 64 * 1024

	errChan := make(chan error, 1)
	r, w := io.Pipe()
	// unblocks the archiver if we return early
	//nolint:errcheck
	defer r.Close()
	archive := func() {
		defer close(errChan)
		//nolint:errcheck
		defer w.Close()
		if err := format.Archive(ctx, w, files); err != nil {
			log.Error("archiving checkpoint", "error", err)
			errChan <- err
		}
	}
	go archive()
	for {
		chunk := make([]byte, chunkSize)
		n, err := r.Read(chunk)
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		if n == 0 {
			continue
		}
		if err := imageStream.Send(&nodev1.Image{ImageData: chunk[:n]}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	for err := range errChan {
		return err
	}
	return nil
}

func filesToArchive(ctx context.Context, log *slog.Logger, req *nodev1.PullImageRequest) ([]archives.FileInfo, error) {
	allFiles, err := archives.FilesFromDisk(ctx, nil, map[string]string{nodev1.SnapshotPath(req.ImageId): ""})
	if err != nil {
		return nil, fmt.Errorf("unable to archive checkpoint: %w", err)
	}
	toArchive := []archives.FileInfo{}
	for _, f := range allFiles {
		if f.Mode().Type() == os.ModeSocket {
			log.Debug("skipping unix socket", "name", f.NameInArchive)
			continue
		}
		toArchive = append(toArchive, f)
	}
	return toArchive, nil
}
