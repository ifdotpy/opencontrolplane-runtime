package serviceprovider

import (
	"context"
	"errors"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	controllerutil2 "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/opencontrolplane-runtime/pkg/serviceprovider/clusteraccess"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	apiconst "github.com/openmcp-project/openmcp-operator/api/constants"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// APIReconciler implements a generic reconcile loop to separate platform
// and service provider developer space.
type APIReconciler[T API, C Config] struct {
	// platformCluster represents the platform cluster of the v2 architecture
	platformCluster *clusters.Cluster
	// onboardingCluster represents the onboarding cluster of the v2 architecture
	onboardingCluster *clusters.Cluster
	// clusterAccessProvider reconciles access to MCP and workload clusters
	clusterAccessProvider clusteraccess.AdvancedProvider
	// reconciler reconciles the end-user facing onboarding API of a service provider
	reconciler Reconciler[T, C]
	// withWorkloadCluster defines whether a service provider requires access to a workload cluster
	withWorkloadCluster bool
	// secretNamespace is the namespace to watch secrets in on the platform cluster. Used only if the ServiceProviderReconciler also implements SecretWatcher.
	secretNamespace string
	// configMapNamespace is the namespace to watch ConfigMaps in on the platform cluster. Used only if the ServiceProviderReconciler also implements ConfigMapWatcher.
	configMapNamespace string
	// emptyObj creates an empty object of the api type
	emptyObj func() T
	// additionalDataGenerators is an optional list of functions which are called during reconciliation.
	// Their outputs are collected and forwarded as additionalData to only the advanced cluster access reconciler.
	additionalDataGenerators []func(ctx context.Context, obj T, config C) (any, error)
	// providerName
	providerName string
	// emptyConfig creates an empty object of the config type
	emptyConfig func() C
}

// APIReconcilerBuilder enables building valid APIReconcilers.
type APIReconcilerBuilder[T API, C Config] struct {
	apiReconciler APIReconciler[T, C]
}

// NewAPIReconcilerBuilder creates a builder.
func NewAPIReconcilerBuilder[T API, C Config]() *APIReconcilerBuilder[T, C] {
	return &APIReconcilerBuilder[T, C]{
		apiReconciler: APIReconciler[T, C]{},
	}
}

// MustBuild validates every required field has been set and returns the APIReconciler.
func (b *APIReconcilerBuilder[T, C]) MustBuild() *APIReconciler[T, C] {
	// validate required fields
	if b.apiReconciler.clusterAccessProvider == nil {
		panic("cluster access provider is required")
	}
	if b.apiReconciler.emptyObj == nil {
		panic("empty object provider is required")
	}
	if b.apiReconciler.emptyConfig == nil {
		panic("empty config provider is required")
	}
	if b.apiReconciler.onboardingCluster == nil {
		panic("onboarding cluster is required")
	}
	if b.apiReconciler.platformCluster == nil {
		panic("platform cluster is required")
	}
	if b.apiReconciler.reconciler == nil {
		panic("reconciler is required")
	}
	return &b.apiReconciler
}

// EmptyObjectProvider sets the empty object function required for concrete type processing.
func (b *APIReconcilerBuilder[T, C]) EmptyObjectProvider(emptyObj func() T) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.emptyObj = emptyObj
	return b
}

// EmptyConfigProvider sets the empty object function required for concrete type processing.
func (b *APIReconcilerBuilder[T, C]) EmptyConfigProvider(emptyConfig func() C) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.emptyConfig = emptyConfig
	return b
}

// PlatformCluster sets the platform cluster.
func (b *APIReconcilerBuilder[T, C]) PlatformCluster(c *clusters.Cluster) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.platformCluster = c
	return b
}

// OnboardingCluster set the onboarding cluster.
func (b *APIReconcilerBuilder[T, C]) OnboardingCluster(c *clusters.Cluster) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.onboardingCluster = c
	return b
}

// ClusterAccessReconciler sets the letgacy cluster access reconciler.
// additionalData provided through additionalDataGenerators is dropped since the legacy Provider does not support it.
func (b *APIReconcilerBuilder[T, C]) ClusterAccessReconciler(provider clusteraccess.Provider) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.clusterAccessProvider = clusteraccess.NewSimpleProviderAdapter(provider)
	return b
}

// AdvancedClusterAccessReconciler sets the advanced cluster access reconciler.
func (b *APIReconcilerBuilder[T, C]) AdvancedClusterAccessReconciler(provider clusteraccess.AdvancedProvider) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.clusterAccessProvider = provider
	return b
}

// Reconciler sets the reconciler for a concrete API type.
func (b *APIReconcilerBuilder[T, C]) Reconciler(reconciler Reconciler[T, C]) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.reconciler = reconciler
	return b
}

// WorkloadCluster results in the service provider requesting a workload cluster
func (b *APIReconcilerBuilder[T, C]) WorkloadCluster(wlCluster bool) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.withWorkloadCluster = wlCluster
	return b
}

// SecretNamespace enables secret watching in the given namespace on the platform cluster.
// Only used if the ServiceProviderReconciler also implements SecretWatcher.
func (b *APIReconcilerBuilder[T, C]) SecretNamespace(ns string) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.secretNamespace = ns
	return b
}

// ConfigMapNamespace enables ConfigMap watching in the given namespace on the platform cluster.
// Only used if the ServiceProviderReconciler also implements ConfigMapWatcher.
func (b *APIReconcilerBuilder[T, C]) ConfigMapNamespace(ns string) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.configMapNamespace = ns
	return b
}

// AdditionalDataGenerators registers functions that are called during reconciliation.
// Their output is collected and passed as additionalData to the advanced cluster access reconciler methods.
func (b *APIReconcilerBuilder[T, C]) AdditionalDataGenerators(generators ...func(ctx context.Context, obj T, config C) (any, error)) *APIReconcilerBuilder[T, C] {
	b.apiReconciler.additionalDataGenerators = append(b.apiReconciler.additionalDataGenerators, generators...)
	return b
}

// tenant carries the per-request onboarding-side identity: the client to reach
// the cluster (or kcp workspace) holding the reconciled object, and the request
// key under which cluster access objects are named on the platform cluster.
// In the classic single-onboarding-cluster mode both are static; in the
// multicluster mode they are resolved per request.
type tenant struct {
	cli client.Client
	// accessKey is the request identity used for naming ClusterRequests /
	// AccessRequests on the platform cluster. In multicluster mode it carries
	// a per-workspace prefix so identically named objects in different
	// workspaces do not collide.
	accessKey ctrl.Request
}

// Reconcile orchestrates platform and (domain specific) Reconciler logic to reconcile API objects
// in the classic single-onboarding-cluster mode.
func (r *APIReconciler[T, C]) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	t := tenant{cli: r.onboardingCluster.Client(), accessKey: req}
	return r.reconcileTenant(ctx, t, req.NamespacedName)
}

// reconcileTenant is the mode-independent reconcile core.
func (r *APIReconciler[T, C]) reconcileTenant(ctx context.Context, t tenant, key client.ObjectKey) (_ ctrl.Result, reconcileErr error) {
	l := logf.FromContext(ctx)
	// common reconciler logic including get obj, providerconfig, mcp/workload access
	obj := r.emptyObj()
	if err := t.cli.Get(ctx, key, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	skip, err := r.handleOperationAnnotation(ctx, t, obj)
	if err != nil {
		return ctrl.Result{}, err
	}
	if skip {
		l.Info("Skipping resource due to ignore operation annotation")
		return ctrl.Result{}, nil
	}
	oldObj := obj.DeepCopyObject().(T)
	// always try to update the obj status
	defer func() {
		if err := r.updateStatus(ctx, t, obj, oldObj); err != nil {
			l.Error(err, "status update failed")
			reconcileErr = errors.Join(reconcileErr, err)
		}
	}()
	providerConfig := r.emptyConfig()
	providerConfig.SetName(r.providerName)
	if err := r.platformCluster.Client().Get(ctx, client.ObjectKeyFromObject(providerConfig), providerConfig); err != nil {
		if apierrors.IsNotFound(err) {
			StatusProgressing(obj, reasonReconcileError, "No ProviderConfig found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !providerConfig.GetDeletionTimestamp().IsZero() {
		StatusProgressing(obj, reasonReconcileError, "ProviderConfig is marked for deletion")
		return ctrl.Result{}, nil
	}
	providerConfigCopy := providerConfig.DeepCopyObject().(C)
	// generate additional data
	additionalData, err := r.generateAdditionalData(ctx, obj, providerConfigCopy)
	if err != nil {
		StatusProgressing(obj, reasonReconcileError, "failed to generate additional data")
		return ctrl.Result{}, err
	}
	// core crud
	deleted := !obj.GetDeletionTimestamp().IsZero()
	var res ctrl.Result
	if deleted {
		res, err = r.delete(ctx, t, obj, providerConfigCopy, additionalData)
	} else {
		res, err = r.createOrUpdate(ctx, t, obj, providerConfigCopy, additionalData)
	}
	// return based on result/err
	if err != nil {
		l.Error(err, "reconcile failed")
		return ctrl.Result{}, err
	}
	if res.RequeueAfter > 0 {
		return res, nil
	}
	// fallback to poll interval to prevent 'managed service' drift
	return ctrl.Result{
		RequeueAfter: providerConfigCopy.PollInterval(),
	}, nil
}

func (r *APIReconciler[T, C]) handleOperationAnnotation(ctx context.Context, t tenant, obj T) (bool, error) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return false, nil
	}
	op, ok := annotations[apiconst.OperationAnnotation]
	if !ok {
		return false, nil
	}
	l := logf.FromContext(ctx)
	switch op {
	case apiconst.OperationAnnotationValueIgnore:
		return true, nil
	case apiconst.OperationAnnotationValueReconcile:
		l.Info("Reconciliation requested via annotation. Removing annotation and proceeding with reconciliation")
		if err := controllerutil2.EnsureAnnotation(ctx, t.cli, obj, apiconst.OperationAnnotation, "", true, controllerutil2.DELETE); err != nil {
			l.Error(err, "Failed to remove operation annotation")
			return false, err
		}
	}
	return false, nil
}

func (r *APIReconciler[T, PC]) updateStatus(ctx context.Context, t tenant, newObj T, oldObj T) error {
	if equality.Semantic.DeepEqual(oldObj.GetStatus(), newObj.GetStatus()) {
		return nil
	}
	err := t.cli.Status().Patch(ctx, newObj, client.MergeFrom(oldObj))
	// can't update status if object doesn't exist
	return client.IgnoreNotFound(err)
}

// delete eventually invokes the domain delete logic of a service provider and is the place to implement
// common logic that should be abstracted away from a service provider developer like handling cluster access.
func (r *APIReconciler[T, C]) delete(ctx context.Context, t tenant, obj T, config C, additionalData []any) (ctrl.Result, error) {
	req := t.accessKey
	accessRequestsInDeletion, err := r.areAccessRequestsInDeletion(ctx, req, additionalData)
	if err != nil {
		StatusProgressing(obj, reasonReconcileError, "failed to check access requests in deletion")
		return reconcile.Result{}, err
	}
	if !accessRequestsInDeletion {
		clusterContext, res, err := r.clusters(ctx, req, additionalData)
		if err != nil {
			StatusTerminatingWithReason(obj, reasonReconcileError, "cluster cleanup error")
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			StatusTerminatingWithReason(obj, "Reconciling", "cluster cleanup")
			return res, nil
		}
		res, err = r.reconciler.Delete(ctx, obj, config, clusterContext)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}
	}
	// remove cluster access
	var res ctrl.Result
	res, err = r.clusterAccessProvider.ReconcileDelete(ctx, req, additionalData...)
	if err != nil {
		StatusTerminatingWithReason(obj, reasonReconcileError, "failed cluster access reconcile delete")
		return ctrl.Result{}, err
	}
	// make sure to not drop the object before cleanup has been done
	if res.RequeueAfter > 0 {
		return res, nil
	}
	// remove finalizer
	controllerutil.RemoveFinalizer(obj, obj.Finalizer())
	if err := t.cli.Update(ctx, obj); err != nil {
		StatusTerminatingWithReason(obj, reasonReconcileError, "failed to remove finalizer")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// createOrUpdate eventually invokes the domain createOrUpdate logic of a service provider and is the place to implement
// common logic that should be abstracted away from a service provider developer like handling cluster access.
func (r *APIReconciler[T, C]) createOrUpdate(ctx context.Context, t tenant, obj T, config C, additionalData []any) (ctrl.Result, error) {
	if _, err := controllerutil.CreateOrUpdate(ctx, t.cli, obj, func() error {
		controllerutil.AddFinalizer(obj, obj.Finalizer())
		return nil
	}); err != nil {
		StatusProgressing(obj, reasonReconcileError, "failed to add finalizer")
		return ctrl.Result{}, err
	}
	req := t.accessKey
	clusterContext, res, err := r.clusters(ctx, req, additionalData)
	if err != nil {
		StatusProgressing(obj, reasonReconcileError, "cluster setup error")
		return ctrl.Result{}, err
	}
	if res.RequeueAfter > 0 {
		return res, nil
	}
	return r.reconciler.CreateOrUpdate(ctx, obj, config, clusterContext)
}

// areAccessRequestsInDeletion determines if the access requests for a reconcile request are in deletion.
// It returns true if any access requests (mcp, workload) is deleted or has a deletion timestamp.
// It is used to prevent renewing cluster access when deleting an ServiceProviderAPI object.
func (r *APIReconciler[T, C]) areAccessRequestsInDeletion(ctx context.Context, req ctrl.Request, additionalData []any) (bool, error) {
	accessRequest, err := r.clusterAccessProvider.AccessRequest(ctx, req, clusteraccess.MCPClusterID, additionalData...)
	if apierrors.IsNotFound(err) || (accessRequest != nil && accessRequest.DeletionTimestamp != nil) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if r.withWorkloadCluster {
		accessRequest, err = r.clusterAccessProvider.AccessRequest(ctx, req, clusteraccess.WorkloadClusterID, additionalData...)
		if apierrors.IsNotFound(err) || (accessRequest != nil && accessRequest.DeletionTimestamp != nil) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// clusters returns any request scoped cluster that a servicer provider developer might want to access in order
// to delivery its service.
func (r *APIReconciler[T, C]) clusters(ctx context.Context, req ctrl.Request, additionalData []any) (clusteraccess.ClusterContext, ctrl.Result, error) {
	clusterContext := clusteraccess.ClusterContext{}
	res, err := r.clusterAccessProvider.Reconcile(ctx, req, additionalData...)
	if err != nil {
		return clusterContext, ctrl.Result{}, err
	}
	if res.RequeueAfter > 0 {
		return clusterContext, res, nil
	}
	mcpCluster, err := r.clusterAccessProvider.Access(ctx, req, clusteraccess.MCPClusterID, additionalData...)
	if err != nil {
		return clusterContext, ctrl.Result{}, err
	}
	if mcpCluster == nil {
		return clusterContext, res, errors.New("mcp access missing")
	}
	clusterContext.MCPCluster = mcpCluster
	ar, err := r.clusterAccessProvider.AccessRequest(ctx, req, clusteraccess.MCPClusterID, additionalData...)
	if err != nil {
		return clusterContext, ctrl.Result{}, err
	}
	clusterContext.MCPAccessSecretKey = retrieveSecretKey(ar)
	if r.withWorkloadCluster {
		workloadCluster, err := r.clusterAccessProvider.Access(ctx, req, clusteraccess.WorkloadClusterID, additionalData...)
		if err != nil {
			return clusterContext, ctrl.Result{}, err
		}
		if workloadCluster == nil {
			return clusterContext, res, errors.New("workload cluster access missing")
		}
		clusterContext.WorkloadCluster = workloadCluster
		ar, err := r.clusterAccessProvider.AccessRequest(ctx, req, clusteraccess.WorkloadClusterID, additionalData...)
		if err != nil {
			return clusterContext, ctrl.Result{}, err
		}
		clusterContext.WorkloadAccessSecretKey = retrieveSecretKey(ar)
	}
	return clusterContext, res, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *APIReconciler[T, C]) SetupWithManager(mgr ctrl.Manager, providerName string) error {
	if providerName == "" {
		return errors.New("provider name is required for manager setup")
	}
	r.providerName = providerName
	controller := ctrl.NewControllerManagedBy(mgr).
		For(r.emptyObj()).
		// add provider config watch
		WatchesRawSource(source.Kind(
			r.platformCluster.Cluster().GetCache(),
			r.emptyConfig(),
			handler.TypedEnqueueRequestsFromMapFunc(r.mapProviderConfigToRequests()),
			controllerutil2.ToTypedPredicate[C](controllerutil2.ExactNamePredicate(providerName, "")),
		))

	// Optional: watch secrets on the platform cluster if the reconciler implements SecretWatcher
	if sw, ok := r.reconciler.(SecretWatcher[C]); ok && r.secretNamespace != "" {
		controller = controller.WatchesRawSource(
			source.Kind(
				r.platformCluster.Cluster().GetCache(),
				&corev1.Secret{},
				handler.TypedEnqueueRequestsFromMapFunc(r.mapSecretToRequests(sw)),
				controllerutil2.ToTypedPredicate[*corev1.Secret](
					predicate.NewPredicateFuncs(func(obj client.Object) bool {
						return obj.GetNamespace() == r.secretNamespace
					}),
				),
			),
		)
	}

	// Optional: watch ConfigMaps on the platform cluster if the reconciler implements ConfigMapWatcher
	if cw, ok := r.reconciler.(ConfigMapWatcher[C]); ok && r.configMapNamespace != "" {
		controller = controller.WatchesRawSource(
			source.Kind(
				r.platformCluster.Cluster().GetCache(),
				&corev1.ConfigMap{},
				handler.TypedEnqueueRequestsFromMapFunc(r.mapConfigMapToRequests(cw)),
				controllerutil2.ToTypedPredicate[*corev1.ConfigMap](
					predicate.NewPredicateFuncs(func(obj client.Object) bool {
						return obj.GetNamespace() == r.configMapNamespace
					}),
				),
			),
		)
	}

	return controller.Named(providerName).Complete(r)
}

// mapSecretToRequests returns a typed map function that checks whether a changed secret
// is referenced by the service provider and, if so, enqueues all ServiceProviderAPI objects.
func (r *APIReconciler[T, C]) mapSecretToRequests(sw SecretWatcher[C]) func(ctx context.Context, secret *corev1.Secret) []reconcile.Request {
	return func(ctx context.Context, secret *corev1.Secret) []reconcile.Request {
		providerConfig := r.emptyConfig()
		providerConfig.SetName(r.providerName)
		if err := r.platformCluster.Client().Get(ctx, client.ObjectKeyFromObject(providerConfig), providerConfig); err != nil {
			return nil
		}
		if !sw.IsReferencedSecret(ctx, secret, providerConfig) {
			return nil
		}
		return r.enqueueAllObjects(ctx)
	}
}

// mapConfigMapToRequests returns a typed map function that checks whether a changed ConfigMap
// is referenced by the service provider and, if so, enqueues all ServiceProviderAPI objects.
func (r *APIReconciler[T, C]) mapConfigMapToRequests(cw ConfigMapWatcher[C]) func(ctx context.Context, configMap *corev1.ConfigMap) []reconcile.Request {
	return func(ctx context.Context, configMap *corev1.ConfigMap) []reconcile.Request {
		providerConfig := r.emptyConfig()
		providerConfig.SetName(r.providerName)
		if err := r.platformCluster.Client().Get(ctx, client.ObjectKeyFromObject(providerConfig), providerConfig); err != nil {
			return nil
		}
		if !cw.IsReferencedConfigMap(ctx, configMap, providerConfig) {
			return nil
		}
		return r.enqueueAllObjects(ctx)
	}
}

func (r *APIReconciler[T, C]) mapProviderConfigToRequests() func(ctx context.Context, _ C) []reconcile.Request {
	return func(ctx context.Context, _ C) []reconcile.Request {
		return r.enqueueAllObjects(ctx)
	}
}

// enqueueAllObjects lists all ServiceProviderAPI objects and returns a reconcile request for each.
func (r *APIReconciler[T, C]) enqueueAllObjects(ctx context.Context) []reconcile.Request {
	var list unstructured.UnstructuredList
	gvk, err := apiutil.GVKForObject(r.emptyObj(), r.onboardingCluster.Scheme())
	if err != nil {
		logf.FromContext(ctx).Error(err, "failed to retrieve gvk")
		return nil
	}
	list.SetGroupVersionKind(gvk)
	if err := r.onboardingCluster.Client().List(ctx, &list); err != nil {
		logf.FromContext(ctx).Error(err, "failed to list objects")
		return nil
	}
	reqs := make([]reconcile.Request, len(list.Items))
	for i := range list.Items {
		reqs[i] = reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&list.Items[i]),
		}
	}
	return reqs
}

func retrieveSecretKey(ar *clustersv1alpha1.AccessRequest) client.ObjectKey {
	if ar == nil || ar.Status.SecretRef == nil {
		return client.ObjectKey{}
	}
	return client.ObjectKey{
		Namespace: ar.Namespace,
		Name:      ar.Status.SecretRef.Name,
	}
}

// generateAdditionalData calls all registered generators and returns their outputs as a slice.
func (r *APIReconciler[T, C]) generateAdditionalData(ctx context.Context, obj T, config C) ([]any, error) {
	if len(r.additionalDataGenerators) == 0 {
		return nil, nil
	}
	data := make([]any, 0, len(r.additionalDataGenerators))
	for _, gen := range r.additionalDataGenerators {
		d, err := gen(ctx, obj, config)
		if err != nil {
			return nil, err
		}
		data = append(data, d)
	}
	return data, nil
}
