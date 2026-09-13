package serviceprovider

import (
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"strings"
	"testing"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
)

func TestTenantAccessKey(t *testing.T) {
	base := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "db"}}

	t.Run("no cluster keeps the request identity (classic compatibility)", func(t *testing.T) {
		got := tenantAccessKey(mcreconcile.Request{Request: base})
		if got != base {
			t.Fatalf("expected %v, got %v", base, got)
		}
	})

	t.Run("cluster name qualifies the namespace, name is untouched", func(t *testing.T) {
		got := tenantAccessKey(mcreconcile.Request{Request: base, ClusterName: "22zec2xbbxeai38f"})
		if got.Name != "db" {
			t.Fatalf("name must be preserved, got %q", got.Name)
		}
		if got.Namespace != "22zec2xbbxeai38f_ns" {
			t.Fatalf("expected qualified namespace, got %q", got.Namespace)
		}
	})

	t.Run("matches the ControlPlane controller's kcp-mode identity", func(t *testing.T) {
		// The ControlPlane controller port derives the platform-side MCP
		// namespace from ("<cluster>_<namespace>", name). The access key must
		// feed the same inputs into StableMCPNamespace so a service object
		// finds the MCP cluster of its same-named ControlPlane.
		got := tenantAccessKey(mcreconcile.Request{Request: base, ClusterName: "cl1"})
		if got.Namespace != "cl1_ns" || got.Name != "db" {
			t.Fatalf("identity drifted from the ControlPlane scheme: %v", got)
		}
	})

	t.Run("logical cluster path remains part of the identity", func(t *testing.T) {
		got := tenantAccessKey(mcreconcile.Request{Request: base, ClusterName: "root:org:account"})
		if got.Namespace != "root:org:account_ns" || got.Name != "db" {
			t.Fatalf("unexpected qualified identity: %v", got)
		}
	})

	t.Run("same object name in different clusters does not collide", func(t *testing.T) {
		a := tenantAccessKey(mcreconcile.Request{Request: base, ClusterName: "cluster-a"})
		b := tenantAccessKey(mcreconcile.Request{Request: base, ClusterName: "cluster-b"})
		if a == b {
			t.Fatalf("collision across clusters: %v", a)
		}
	})
}

func multiclusterTestBuilder(t *testing.T) *APIReconcilerBuilder[*fakeApiImpl, *fakeProviderConfigImpl] {
	t.Helper()
	platformCluster := clusters.NewTestClusterFromClient("platform", fake.NewClientBuilder().Build())
	return NewAPIReconcilerBuilder[*fakeApiImpl, *fakeProviderConfigImpl]().
		EmptyObjectProvider(func() *fakeApiImpl { return &fakeApiImpl{} }).
		EmptyConfigProvider(func() *fakeProviderConfigImpl { return &fakeProviderConfigImpl{} }).
		PlatformCluster(platformCluster).
		AdvancedClusterAccessReconciler(FakeAdvancedClusterAccessProvider{}).
		Reconciler(&MockServiceProviderReconciler{})
}

func TestMulticlusterModeGuards(t *testing.T) {
	t.Run("secret/configmap watching is rejected at build time", func(t *testing.T) {
		defer expectPanicContaining(t, "not supported in multicluster mode")()
		multiclusterTestBuilder(t).SecretNamespace("ns").MustBuildMulticluster()
	})

	t.Run("legacy cluster access reconciler is rejected at build time", func(t *testing.T) {
		defer expectPanicContaining(t, "legacy ClusterAccessReconciler")()
		multiclusterTestBuilder(t).
			ClusterAccessReconciler(FakeClusterAccessProvider{}).
			MustBuildMulticluster()
	})

	t.Run("classic setup on a multicluster-built reconciler fails fast", func(t *testing.T) {
		r := multiclusterTestBuilder(t).MustBuildMulticluster()
		if err := r.SetupWithManager(nil, "p"); err == nil {
			t.Fatal("expected an error for classic setup without an onboarding cluster")
		}
	})

	t.Run("multicluster setup on a classic-built reconciler fails fast", func(t *testing.T) {
		onboarding := clusters.NewTestClusterFromClient("onboarding", fake.NewClientBuilder().Build())
		r := multiclusterTestBuilder(t).OnboardingCluster(onboarding).MustBuild()
		if err := r.SetupWithMulticlusterManager(nil, "p"); err == nil {
			t.Fatal("expected an error for multicluster setup with an onboarding cluster set")
		}
	})
}

func expectPanicContaining(t *testing.T, contains string) func() {
	t.Helper()
	return func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q", contains)
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, contains) {
			t.Fatalf("unexpected panic: %v", r)
		}
	}
}

// Both manager paths use this event contract. Status writes must not create
// a reconcile loop, while changes that require provider work must enqueue.
func TestDefaultForPredicates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*corev1.Pod)
		want   bool
	}{
		{"status only", func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning }, false},
		{"generation", func(p *corev1.Pod) { p.Generation++ }, true},
		{"labels", func(p *corev1.Pod) { p.Labels = map[string]string{"test": "changed"} }, true},
		{"annotations", func(p *corev1.Pod) { p.Annotations = map[string]string{"test": "changed"} }, true},
		{"deletion", func(p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }, true},
		{"ignore", func(p *corev1.Pod) {
			p.Generation++
			p.Annotations = map[string]string{apiconst.OperationAnnotation: apiconst.OperationAnnotationValueIgnore}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "service", Generation: 1}}
			after := before.DeepCopy()
			tc.change(after)
			got := true
			for _, filter := range defaultForPredicates() {
				got = got && filter.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after})
			}
			if got != tc.want {
				t.Fatalf("enqueue = %v, want %v", got, tc.want)
			}
		})
	}
}
