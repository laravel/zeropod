package task

import (
	"context"
	"strings"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/ttrpc"
	zshim "github.com/laravel/zeropod/shim"
)

// the containerd shim picks up interceptors of plugin instances.
var _ = (shim.TTRPCServerUnaryOptioner)(&wrapper{})

// UnaryServerInterceptor adds the container ID to the logger of every task
// request.
func (w *wrapper) UnaryServerInterceptor() ttrpc.UnaryServerInterceptor {
	return func(ctx context.Context, unmarshal ttrpc.Unmarshaler, info *ttrpc.UnaryServerInfo, method ttrpc.Method) (any, error) {
		if !strings.HasPrefix(info.FullMethod, "/"+taskServiceV2+"/") &&
			!strings.HasPrefix(info.FullMethod, "/"+taskServiceV3+"/") {
			return method(ctx, unmarshal)
		}
		// all task requests have the container ID as field 1 and StatsRequest
		// only has the ID so we can decode any type into it as long as this
		// does not change.
		var req taskAPI.StatsRequest
		if err := unmarshal(&req); err == nil && req.ID != "" {
			ctx = zshim.WithContainerLogger(ctx, req.ID)
		}
		return method(ctx, unmarshal)
	}
}
