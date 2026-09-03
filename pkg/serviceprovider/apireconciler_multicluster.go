package serviceprovider

import (
	"context"
	"errors"
	"regexp"

	controllerutil2 "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/opencontrolplane-runtime/pkg/serviceprovider/clusteraccess"
	ctrl "sigs.k8s.io/controller-runtime"

	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
)

// Multicluster deployment mode
//
// This implements a first increment of the ADR "kcp-aware Service Provider
// Runtime" (2026-08-20): the runtime, not the provider, owns the kcp watch
// path, while the provider seam (CreateOrUpdate/Delete) stays unchanged.
// APIExport provisioning, workspace token minting and running both modes in
// one process are later increments of that ADR and slot into this seam.
//
// In this mode the APIReconciler does not watch a single onboarding cluster.
// Instead it is driven by a multicluster-runtime manager whose provider
// supplies one logical cluster per tenant, for example the kcp `apiexport`
// provider of github.com/kcp-dev/multicluster-provider, which engages every
// kcp workspace that bound the service provider's APIExport. Objects are
// reconciled in place in the tenant's own cluster and status is written back
// there; no object synchronization is involved.
//
// Differences from the classic mode, by design:
//
//   - No onboarding cluster is configured; MustBuildMulticluster is used
//     instead of MustBuild.
//   - Cluster access objects (ClusterRequests / AccessRequests) on the
//     platform cluster are named with a stable per-tenant prefix derived from
//     the logical cluster name, so identically named API objects in different
//     tenant clusters do not collide.
//   - Platform-side ProviderConfig / Secret / ConfigMap watches are not wired
//     in this mode yet. A missing ProviderConfig is retried on a fixed
//     interval; secret/configmap watching is rejected at build time.
//   - Known limitation: when a tenant cluster disappears together with live
//     API objects, in-flight requests trigger a best-effort cleanup of the
//     platform-side access objects, but a full garbage collector for
//     disengaged clusters is a follow-up.
//
// Usage (the provider construction is up to the service provider binary; the
// library itself does not depend on kcp):
//
//	provider, _ := apiexport.New(cfg, endpointSliceName, apiexport.Options{Scheme: scheme})
//	mgr, _ := mcmanager.New(cfg, provider, manager.Options{Scheme: scheme})
//	rec := serviceprovider.NewAPIReconcilerBuilder[*myv1alpha1.MyAPI, *myv1alpha1.MyConfig]().
//		PlatformCluster(platformCluster).
//		AdvancedClusterAccessReconciler(accessProvider).
//		Reconciler(&myReconciler{}).
//		EmptyObjectProvider(func() *myv1alpha1.MyAPI { return &myv1alpha1.MyAPI{} }).
//		EmptyConfigProvider(func() *myv1alpha1.MyConfig { return &myv1alpha1.MyConfig{} }).
//		MustBuildMulticluster()
//	_ = rec.SetupWithMulticlusterManager(mgr, "my-provider")
//	_ = mgr.Start(ctx)

// MustBuildMulticluster validates every field required for the multicluster
// mode and returns the APIReconciler. The onboarding cluster must not be set:
// tenant clusters are resolved per request from the multicluster manager.
func (b *APIReconcilerBuilder[T, C]) MustBuildMulticluster() *APIReconciler[T, C] {
	b.validateCommon()
	if b.apiReconciler.onboardingCluster != nil {
		panic("onboarding cluster must not be set in multicluster mode")
	}
	if b.apiReconciler.secretNamespace != "" || b.apiReconciler.configMapNamespace != "" {
		panic("secret/configmap watching is not supported in multicluster mode yet; rely on the ProviderConfig PollInterval instead")
	}
	if clusteraccess.IsLegacyAdapter(b.apiReconciler.clusterAccessProvider) {
		panic("the legacy ClusterAccessReconciler resolves cluster identity from the request name and is not supported in multicluster mode; use AdvancedClusterAccessReconciler")
	}
	return &b.apiReconciler
}

// SetupWithMulticlusterManager sets up the controller with a
// multicluster-runtime manager. The manager's provider defines the fleet of
// tenant clusters (e.g. kcp workspaces via an APIExport virtual workspace).
func (r *APIReconciler[T, C]) SetupWithMulticlusterManager(mgr mcmanager.Manager, providerName string) error {
	if providerName == "" {
		return errors.New("provider name is required for manager setup")
	}
	if r.onboardingCluster != nil {
		return errors.New("reconciler was built for the classic mode; use MustBuildMulticluster")
	}
	r.providerName = providerName
	return mcbuilder.ControllerManagedBy(mgr).
		Named(providerName).
		For(r.emptyObj()).
		Complete(mcreconcile.Func(func(ctx context.Context, req mcreconcile.Request) (ctrl.Result, error) {
			return r.reconcileMulticluster(ctx, mgr, req)
		}))
}

func (r *APIReconciler[T, C]) reconcileMulticluster(ctx context.Context, mgr mcmanager.Manager, req mcreconcile.Request) (ctrl.Result, error) {
	cl, err := mgr.GetCluster(ctx, req.ClusterName)
	if err != nil {
		if errors.Is(err, multicluster.ErrClusterNotFound) {
			// The tenant cluster is gone. Best-effort cleanup of the
			// platform-side cluster access objects for this request; full
			// garbage collection of access objects for disengaged clusters
			// is a documented follow-up.
			res, derr := r.clusterAccessProvider.ReconcileDelete(ctx, tenantAccessKey(req))
			if derr != nil {
				return ctrl.Result{}, derr
			}
			return res, nil
		}
		return ctrl.Result{}, err
	}
	t := tenant{
		cli:                    cl.GetClient(),
		accessKey:              tenantAccessKey(req),
		requeueOnMissingConfig: true,
	}
	return r.reconcileTenant(ctx, t, req.NamespacedName)
}

// safeNameComponent matches cluster names that can be used verbatim inside a
// Kubernetes object name. kcp logical cluster names always match.
var safeNameComponent = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$`)

// tenantAccessKey derives the request identity used for naming cluster access
// objects on the platform cluster. The logical cluster name is folded into
// the request name as a stable prefix, keeping the result deterministic and
// collision-free across tenant clusters. A name-safe cluster name (the kcp
// case) is used verbatim; anything else falls back to the codebase's standard
// short hash. Downstream naming helpers already shorten long names safely.
func tenantAccessKey(req mcreconcile.Request) ctrl.Request {
	out := ctrl.Request{NamespacedName: req.NamespacedName}
	cluster := req.ClusterName
	if cluster == "" {
		return out
	}
	prefix := string(cluster)
	if !safeNameComponent.MatchString(prefix) {
		prefix = controllerutil2.NameHashSHAKE128Base32(prefix)
	}
	out.Name = prefix + "-" + req.Name
	return out
}
