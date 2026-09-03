package serviceprovider

import (
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

	t.Run("name-safe cluster names are used verbatim (collision-free)", func(t *testing.T) {
		got := tenantAccessKey(mcreconcile.Request{Request: base, ClusterName: "22zec2xbbxeai38f"})
		if got.Name != "22zec2xbbxeai38f-db" {
			t.Fatalf("expected verbatim prefix, got %q", got.Name)
		}
		if got.Namespace != "ns" {
			t.Fatalf("namespace must be preserved, got %q", got.Namespace)
		}
	})

	t.Run("unsafe cluster names fall back to the standard short hash", func(t *testing.T) {
		req := mcreconcile.Request{Request: base, ClusterName: "root:orgs:acme:team1"}
		first := tenantAccessKey(req)
		second := tenantAccessKey(req)
		if first != second {
			t.Fatalf("not deterministic: %v vs %v", first, second)
		}
		if strings.ContainsAny(first.Name, ":") {
			t.Fatalf("unsafe characters leaked into the name: %q", first.Name)
		}
		if len(first.Name) != 8+1+len("db") {
			t.Fatalf("unexpected name shape: %q", first.Name)
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
