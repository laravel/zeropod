package task

import (
	"context"
	"testing"

	taskAPI "github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/log"
	"github.com/containerd/ttrpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestLogInterceptor(t *testing.T) {
	for name, tc := range map[string]struct {
		fullMethod string
		req        proto.Message
		expected   any
	}{
		"create": {
			fullMethod: "/" + taskServiceV2 + "/Create",
			req:        &taskAPI.CreateTaskRequest{ID: "create", Bundle: "/run/bundle"},
			expected:   "create",
		},
		"update": {
			fullMethod: "/" + taskServiceV3 + "/Update",
			req:        &taskAPI.UpdateTaskRequest{ID: "update", Resources: &anypb.Any{TypeUrl: "foo", Value: []byte{0xff, 0xfe}}},
			expected:   "update",
		},
		"shutdown": {
			fullMethod: "/" + taskServiceV2 + "/Shutdown",
			req:        &taskAPI.ShutdownRequest{ID: "shutdown", Now: true},
			expected:   "shutdown",
		},
		"no id": {
			fullMethod: "/" + taskServiceV2 + "/State",
			req:        &taskAPI.StateRequest{},
		},
		"other service": {
			fullMethod: "/containerd.other.v1.Other/Get",
			req:        &taskAPI.StateRequest{ID: "other"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, err := proto.Marshal(tc.req)
			require.NoError(t, err)
			unmarshal := func(v any) error { return proto.Unmarshal(p, v.(proto.Message)) }

			resp, err := (&wrapper{}).UnaryServerInterceptor()(
				context.Background(), unmarshal, &ttrpc.UnaryServerInfo{FullMethod: tc.fullMethod},
				func(ctx context.Context, unmarshal func(any) error) (any, error) {
					assert.Equal(t, tc.expected, log.G(ctx).Data["container"])
					req := proto.Clone(tc.req)
					proto.Reset(req)
					return req, unmarshal(req)
				},
			)
			require.NoError(t, err)
			assert.True(t, proto.Equal(tc.req, resp.(proto.Message)))
		})
	}
}
