package node

import (
	"context"
	"log/slog"
	"testing"
	"time"

	nodev1 "github.com/laravel/zeropod/api/node/v1"
	v1 "github.com/laravel/zeropod/api/runtime/v1"
	shimv1 "github.com/laravel/zeropod/api/shim/v1"
	"github.com/laravel/zeropod/manager"
	"github.com/laravel/zeropod/manager/capacity"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSetContainerStatus(t *testing.T) {
	migration := &v1.Migration{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Status: v1.MigrationStatus{
			Containers: []v1.MigrationContainerStatus{
				{
					Name:     "container1",
					PausedAt: metav1.NowMicro(),
				},
			},
		},
	}

	setOrUpdateContainerStatus(migration, "container1", func(cms *v1.MigrationContainerStatus) {
		cms.Condition.Phase = v1.MigrationPhaseCompleted
		cms.RestoredAt = metav1.NowMicro()
	})
	assert.Equal(t, v1.MigrationPhaseCompleted, migration.Status.Containers[0].Condition.Phase)
	assert.NotEmpty(t, migration.Status.Containers[0].PausedAt)
	assert.NotEmpty(t, migration.Status.Containers[0].RestoredAt)

	setOrUpdateContainerStatus(migration, "container2", func(cms *v1.MigrationContainerStatus) {
		cms.Condition.Phase = v1.MigrationPhaseFailed
		cms.RestoredAt = metav1.NowMicro()
	})
	assert.Len(t, migration.Status.Containers, 2)
	assert.Equal(t, v1.MigrationPhaseFailed, migration.Status.Containers[1].Condition.Phase)
	assert.NotEmpty(t, migration.Status.Containers[1].RestoredAt)
}

func TestRestoreCapacity(t *testing.T) {
	for name, tc := range map[string]struct {
		node                *corev1.Node
		pod                 *corev1.Pod
		migration           *v1.Migration
		cpuThreshold        float64
		memoryThreshold     float64
		containerName       string
		allowed             bool
		expectedErr         bool
		podScaledDown       bool
		podDeleting         bool
		denyDeletionRestore bool
		evictions           float64
	}{
		"empty node": {
			node: newNode("empty", nil),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
			containerName:   "app1",
			allowed:         true,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
		"empty requests": {
			node: newNode("enough", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			pod:             newPod("empty-req", corev1.ResourceList{}),
			allowed:         true,
			expectedErr:     false,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
		"node with enough capacity": {
			node: newNode("enough", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			containerName:   "app1",
			allowed:         true,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
		"node at memory capacity": {
			node: newNode("full-mem", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			migration:       newMigration("10.0.0.1"),
			containerName:   "app1",
			allowed:         false,
			evictions:       1,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
		"node at memory capacity because of low threshold": {
			node: newNode("full-mem", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			}),
			migration:       newMigration("10.0.0.1"),
			containerName:   "app1",
			allowed:         false,
			evictions:       1,
			cpuThreshold:    1,
			memoryThreshold: 0.1,
		},
		"node at cpu capacity because of low threshold": {
			node: newNode("full-mem", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("10Mi"),
			}),
			migration:       newMigration("10.0.0.1"),
			containerName:   "app1",
			allowed:         false,
			evictions:       1,
			cpuThreshold:    0.1,
			memoryThreshold: 1.0,
		},
		"pod with resources in annotation": {
			node: newNode("full-mem", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("16Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			migration:       newMigration("10.0.0.1"),
			containerName:   "app1",
			allowed:         false,
			evictions:       1,
			podScaledDown:   true,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
		"pod already being evicted with enough capacity": {
			node: newNode("enough", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
			migration:           newMigration("10.0.0.1"),
			containerName:       "app1",
			allowed:             false,
			podDeleting:         true,
			denyDeletionRestore: true,
			cpuThreshold:        1.0,
			memoryThreshold:     1.0,
		},
		"pod being deleted at capacity": {
			node: newNode("full-mem", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			}),
			migration:       newMigration("10.0.0.1"),
			containerName:   "app1",
			allowed:         false,
			podDeleting:     true,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
		"pod being deleted with enough capacity": {
			node: newNode("enough", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("20Gi"),
			}),
			pod: newPod("app1", corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			}),
			migration:       newMigration("10.0.0.1"),
			containerName:   "app1",
			allowed:         true,
			podDeleting:     true,
			cpuThreshold:    1.0,
			memoryThreshold: 1.0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			assert.NoError(t, corev1.AddToScheme(scheme))
			assert.NoError(t, v1.AddToScheme(scheme))
			if tc.podScaledDown {
				assert.NoError(t, setScaledDown(tc.pod))
			}
			if tc.podDeleting {
				tc.pod.DeletionTimestamp = new(metav1.Now())
				tc.pod.Finalizers = []string{"test"}
			}
			objs := []client.Object{tc.node, tc.pod}
			if tc.migration != nil {
				objs = append(objs, tc.migration)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			reg := prometheus.NewRegistry()
			cap := capacity.NewNodeTracker(
				reg,
				"name",
				capacity.CPUThreshold(tc.cpuThreshold),
				capacity.MemoryThreshold(tc.memoryThreshold),
			)
			for name, q := range tc.node.Status.Capacity {
				cap.SetCapacity(name, q)
			}
			slog.SetLogLoggerLevel(slog.LevelDebug)
			ns := &nodeService{
				kube:                kube,
				nodeName:            tc.node.Name,
				log:                 slog.Default(),
				cap:                 cap,
				denyDeletionRestore: tc.denyDeletionRestore,
			}
			resp, err := ns.RestoreCapacity(t.Context(), &nodev1.RestoreCapacityRequest{
				PodInfo: &nodev1.PodInfo{
					Name:          tc.pod.Name,
					Namespace:     tc.pod.Namespace,
					ContainerName: tc.containerName,
				},
			})
			if tc.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.NotNil(t, resp)
			assert.Equal(t, tc.allowed, resp.Allowed)
			assert.Equal(t, tc.evictions, evictionCount(t, reg))
			if tc.allowed {
				assert.Empty(t, resp.RedirectAddr)
			}
		})
	}
}

func TestRestoreCapacityAlreadyEvicted(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, corev1.AddToScheme(scheme))
	assert.NoError(t, v1.AddToScheme(scheme))
	node := newNode("node", corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	})
	pod := newPod("app1", corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("2Gi"),
	})
	pod.UID = "pod-uid"
	newKube := func() client.Client {
		return fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(node.DeepCopy(), pod.DeepCopy(), newMigration("10.0.0.1")).Build()
	}
	reg := prometheus.NewRegistry()
	cap := capacity.NewNodeTracker(reg, "name", capacity.CPUThreshold(1.0), capacity.MemoryThreshold(1.0))
	for name, q := range node.Status.Capacity {
		cap.SetCapacity(name, q)
	}
	ns := &nodeService{
		kube:     newKube(),
		nodeName: node.Name,
		log:      slog.Default(),
		cap:      cap,
		timeouts: Timeouts{EvictionTimeout: time.Minute},
	}
	req := &nodev1.RestoreCapacityRequest{
		PodInfo: &nodev1.PodInfo{
			Name:          pod.Name,
			Namespace:     pod.Namespace,
			ContainerName: "app1",
		},
	}
	resp, err := ns.RestoreCapacity(t.Context(), req)
	assert.NoError(t, err)
	assert.False(t, resp.Allowed)

	// simulate a stale cache that does not reflect the pod deletion yet and
	// enough capacity for the pod to be restored.
	ns.kube = newKube()
	cap.SetCapacity(corev1.ResourceMemory, resource.MustParse("20Gi"))
	resp, err = ns.RestoreCapacity(t.Context(), req)
	assert.NoError(t, err)
	assert.False(t, resp.Allowed)

	assert.Equal(t, 1.0, evictionCount(t, reg))
}

func evictionCount(t *testing.T, reg *prometheus.Registry) float64 {
	mfs, err := reg.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == "zeropod_"+capacity.MetricNodeEvictions {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

func TestRestoreCapacityMigrationFinal(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, corev1.AddToScheme(scheme))
	assert.NoError(t, v1.AddToScheme(scheme))
	node := newNode("node", corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	})
	pod := newPod("app1", corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("2Gi"),
	})
	pod.Annotations = map[string]string{shimv1.MigrateAnnotationKey: "app1"}
	pod.DeletionTimestamp = new(metav1.Now())
	pod.Finalizers = []string{"test"}
	migration := newMigration("")
	migration.Status.Containers = []v1.MigrationContainerStatus{{
		Name:      "app1",
		Condition: v1.MigrationCondition{Phase: v1.MigrationPhaseUnclaimed},
	}}
	cap := capacity.NewNodeTracker(prometheus.NewRegistry(), "name", capacity.CPUThreshold(1.0), capacity.MemoryThreshold(1.0))
	for name, q := range node.Status.Capacity {
		cap.SetCapacity(name, q)
	}
	ns := &nodeService{
		kube:     fake.NewClientBuilder().WithScheme(scheme).WithObjects(node, pod, migration).Build(),
		nodeName: node.Name,
		log:      slog.Default(),
		cap:      cap,
		timeouts: Timeouts{EvictionTimeout: time.Minute},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	resp, err := ns.RestoreCapacity(ctx, &nodev1.RestoreCapacityRequest{
		PodInfo: &nodev1.PodInfo{
			Name:          pod.Name,
			Namespace:     pod.Namespace,
			ContainerName: "app1",
		},
	})
	assert.NoError(t, err)
	assert.NoError(t, ctx.Err(), "should stop waiting once the migration is final")
	assert.False(t, resp.Allowed)
	assert.Empty(t, resp.RedirectAddr)
}

func TestRestore(t *testing.T) {
	const (
		migrationName   = "foo"
		podTemplateHash = "hashi"
		containerName   = "app1"
		containerID     = "container-id"
	)

	for name, tc := range map[string]struct {
		migration       *v1.Migration
		containerName   string
		abortMigration  bool
		expectedErr     string
		expectedFailed  bool
		expectedImageID string
	}{
		"no matching migration": {
			migration:     newRestoreMigration(migrationName, "other-hash", containerName, containerID, nil),
			containerName: containerName,
			expectedErr:   "timeout waiting for matching migration",
		},
		"migration already failed": {
			migration: withFailedContainer(
				newRestoreMigration(migrationName, podTemplateHash, containerName, containerID, nil), containerName,
			),
			containerName: containerName,
			expectedErr:   "matching migration is set to failed",
		},
		"migration servers never set": {
			migration:      newRestoreMigration(migrationName, podTemplateHash, containerName, containerID, nil),
			containerName:  containerName,
			expectedErr:    "migration servers are not set on migration",
			expectedFailed: true,
		},
		"migration aborted while waiting for servers": {
			migration:      newRestoreMigration(migrationName, podTemplateHash, containerName, containerID, nil),
			containerName:  containerName,
			abortMigration: true,
			expectedErr:    "matching migration is set to failed",
			expectedFailed: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			assert.NoError(t, corev1.AddToScheme(scheme))
			assert.NoError(t, v1.AddToScheme(scheme))

			pod := newPod(tc.containerName, nil)
			pod.Labels = map[string]string{appsv1.DefaultDeploymentUniqueLabelKey: podTemplateHash}

			kube := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&v1.Migration{}).
				WithObjects(pod, tc.migration).Build()

			slog.SetLogLoggerLevel(slog.LevelDebug)
			ns := &nodeService{
				kube:     kube,
				nodeName: "node1",
				host:     "10.0.0.1",
				log:      slog.Default(),
				timeouts: Timeouts{MigrationServers: 200 * time.Millisecond},
			}

			req := &nodev1.RestoreRequest{
				PodInfo: &nodev1.PodInfo{
					Name:          pod.Name,
					Namespace:     pod.Namespace,
					ContainerName: tc.containerName,
					Ip:            "10.0.0.2",
				},
				MigrationInfo: &nodev1.MigrationInfo{ImageId: "image-id"},
			}

			if tc.abortMigration {
				abortErr := make(chan error, 1)
				go func() {
					abortErr <- pollUntilContextCancel(t.Context(), func(ctx context.Context) (bool, error) {
						migration := &v1.Migration{}
						if err := kube.Get(ctx, client.ObjectKeyFromObject(tc.migration), migration); err != nil {
							return false, err
						}
						if !migration.Spec.RestoreReady {
							return false, nil
						}
						_, err := ns.AbortEvac(ctx, &nodev1.EvacRequest{PodInfo: req.PodInfo})
						return true, err
					})
				}()
				t.Cleanup(func() { assert.NoError(t, <-abortErr) })
			}

			resp, err := ns.Restore(t.Context(), req)
			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				assert.Equal(t, tc.expectedImageID, resp.MigrationInfo.ImageId)
			}

			if tc.expectedFailed {
				migration := &v1.Migration{}
				assert.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(tc.migration), migration))
				assert.True(t, migrationFailed(migration, tc.containerName))
			}
		})
	}
}

func TestAbortEvac(t *testing.T) {
	for name, tc := range map[string]struct {
		migration     *v1.Migration
		containerName string
		expectedErr   bool
	}{
		"sets container to failed": {
			migration:     newRestoreMigration("foo", "hashi", "app1", "container-id", nil),
			containerName: "app1",
		},
		"unknown container is added as failed": {
			migration:     newRestoreMigration("foo", "hashi", "app1", "container-id", nil),
			containerName: "app2",
		},
		"migration not found": {
			containerName: "app1",
			expectedErr:   true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			assert.NoError(t, corev1.AddToScheme(scheme))
			assert.NoError(t, v1.AddToScheme(scheme))

			objs := []client.Object{}
			if tc.migration != nil {
				objs = append(objs, tc.migration)
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&v1.Migration{}).
				WithObjects(objs...).Build()

			slog.SetLogLoggerLevel(slog.LevelDebug)
			ns := &nodeService{kube: kube, nodeName: "node1", log: slog.Default()}

			resp, err := ns.AbortEvac(t.Context(), &nodev1.EvacRequest{
				PodInfo: &nodev1.PodInfo{
					Name:          "foo",
					Namespace:     "default",
					ContainerName: tc.containerName,
				},
			})
			assert.NotNil(t, resp)
			if tc.expectedErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			migration := &v1.Migration{}
			assert.NoError(t, kube.Get(t.Context(), client.ObjectKeyFromObject(tc.migration), migration))
			assert.True(t, migrationFailed(migration, tc.containerName))
		})
	}
}

func newPod(containerName string, requests corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: containerName,
				Resources: corev1.ResourceRequirements{
					Requests: requests,
				},
			}},
		},
	}
}

func setScaledDown(pod *corev1.Pod) error {
	if err := manager.SetAnnotations(pod); err != nil {
		return err
	}
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Resources.Requests[corev1.ResourceCPU] = manager.ScaledDownCPU
		pod.Spec.Containers[i].Resources.Requests[corev1.ResourceMemory] = manager.ScaledDownMemory
	}
	return nil
}

func newNode(name string, cap corev1.ResourceList) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Capacity: cap,
		},
	}
}

func newMigration(targetPodIP string) *v1.Migration {
	return &v1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "default"},
		Spec: v1.MigrationSpec{
			TargetPodIP: targetPodIP,
		},
	}
}

func newRestoreMigration(name, podTemplateHash, containerName, containerID string, imageServer *v1.MigrationServer) *v1.Migration {
	return &v1.Migration{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: v1.MigrationSpec{
			SourceNode:      "node2",
			SourcePod:       name,
			PodTemplateHash: podTemplateHash,
			Containers: []v1.MigrationContainer{{
				Name:        containerName,
				ID:          containerID,
				ImageServer: imageServer,
			}},
		},
	}
}

func withFailedContainer(migration *v1.Migration, containerName string) *v1.Migration {
	setOrUpdateContainerStatus(migration, containerName, func(status *v1.MigrationContainerStatus) {
		status.Condition.Phase = v1.MigrationPhaseFailed
	})
	return migration
}
