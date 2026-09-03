package clusteraccess

import (
	"context"
	"fmt"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// MCPClusterID is the id used to identify the MCP cluster in the AdvancedProvider.
	MCPClusterID = "mcp"
	// WorkloadClusterID is the id used to identify the workload cluster in the AdvancedProvider.
	WorkloadClusterID = "workload"
)

// Provider is a light weight version of the ClusterAccessReconciler
type Provider interface {
	// MCPCluster creates a Cluster for the MCP AccessRequest.
	// This function will only be successful if the MCP AccessRequest is granted and Reconcile returned without an error
	// and a reconcile.Result with no RequeueAfter value.
	MCPCluster(ctx context.Context, request reconcile.Request) (*clusters.Cluster, error)
	// MCPAccessRequest returns the AccessRequest for the MCP cluster.
	MCPAccessRequest(ctx context.Context, request reconcile.Request) (*clustersv1alpha1.AccessRequest, error)
	// WorkloadCluster creates a Cluster for the Workload AccessRequest.
	// This function will only be successful if the Workload AccessRequest is granted and Reconcile returned without an error
	// and a reconcile.Result with no RequeueAfter value.
	WorkloadCluster(ctx context.Context, request reconcile.Request) (*clusters.Cluster, error)
	// WorkloadAccessRequest returns the AccessRequest for the Workload cluster.
	WorkloadAccessRequest(ctx context.Context, request reconcile.Request) (*clustersv1alpha1.AccessRequest, error)
	// Reconcile creates the ClusterRequests and AccessRequests for the MCP and Workload clusters based on the reconciled object.
	// This function should be called during all reconciliations of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error)
	// ReconcileDelete deletes the AccessRequests and ClusterRequests for the MCP and Workload clusters based on the reconciled object.
	// This function should be called during the deletion of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled.
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	ReconcileDelete(ctx context.Context, request reconcile.Request) (reconcile.Result, error)
}

var _ AdvancedProvider = &simpleProviderAdapter{}

// simpleProviderAdapter wraps the legacy Provider as an AdvancedProvider.
type simpleProviderAdapter struct {
	simple Provider
}

// IsLegacyAdapter reports whether the given provider is the adapter around
// the legacy Provider interface, which resolves cluster identity purely from
// the request name and therefore cannot be used in multicluster mode.
func IsLegacyAdapter(p AdvancedProvider) bool {
	_, ok := p.(*simpleProviderAdapter)
	return ok
}

// NewSimpleProviderAdapter wraps the legacy Provider as an AdvancedProvider.
func NewSimpleProviderAdapter(provider Provider) AdvancedProvider {
	return &simpleProviderAdapter{simple: provider}
}

// Access implements [AdvancedProvider].
func (a *simpleProviderAdapter) Access(ctx context.Context, request reconcile.Request, id string, _ ...any) (*clusters.Cluster, error) {
	switch id {
	case MCPClusterID:
		return a.simple.MCPCluster(ctx, request)
	case WorkloadClusterID:
		return a.simple.WorkloadCluster(ctx, request)
	default:
		return nil, fmt.Errorf("unsupported cluster id: %s", id)
	}
}

// AccessRequest implements [AdvancedProvider].
func (a *simpleProviderAdapter) AccessRequest(ctx context.Context, request reconcile.Request, id string, _ ...any) (*clustersv1alpha1.AccessRequest, error) {
	switch id {
	case MCPClusterID:
		return a.simple.MCPAccessRequest(ctx, request)
	case WorkloadClusterID:
		return a.simple.WorkloadAccessRequest(ctx, request)
	default:
		return nil, fmt.Errorf("unsupported cluster id: %s", id)
	}
}

// Cluster implements [AdvancedProvider].
func (a *simpleProviderAdapter) Cluster(_ context.Context, _ reconcile.Request, _ string, _ ...any) (*clustersv1alpha1.Cluster, error) {
	return nil, nil
}

// Reconcile implements [AdvancedProvider].
func (a *simpleProviderAdapter) Reconcile(ctx context.Context, request reconcile.Request, _ ...any) (reconcile.Result, error) {
	return a.simple.Reconcile(ctx, request)
}

// ReconcileDelete implements [AdvancedProvider].
func (a *simpleProviderAdapter) ReconcileDelete(ctx context.Context, request reconcile.Request, _ ...any) (reconcile.Result, error) {
	return a.simple.ReconcileDelete(ctx, request)
}

// AdvancedProvider is a light weight version of advanced.ClusterAccessReconciler
type AdvancedProvider interface {
	// Access returns an internal Cluster object granting access to the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no AccessRequest is registered for the cluster, or if some other error occurs.
	Access(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clusters.Cluster, error)
	// Cluster fetches the external Cluster object for the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no Cluster can be determined, or if some other error occurs.
	// The same additionalData must be passed into all methods of this ClusterAccessReconciler for the same request and id.
	Cluster(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.Cluster, error)
	// AccessRequest fetches the AccessRequest object for the cluster for the specified request with the specified id.
	// Will fail if the cluster is not registered or no AccessRequest is registered for the cluster, or if some other error occurs.
	// The same additionalData must be passed into all methods of this ClusterAccessReconciler for the same request and id.
	AccessRequest(ctx context.Context, request reconcile.Request, id string, additionalData ...any) (*clustersv1alpha1.AccessRequest, error)
	// Reconcile creates the ClusterRequests and/or AccessRequests for the registered clusters.
	// This function should be called during all reconciliations of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled.
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	// The duration is set by the WithRetryInterval method.
	// Any additional arguments provided are passed into all methods of the ClusterRegistration objects that are called.
	//
	// Note that Reconcile will not create any new resources if the current request is in deletion.
	// A request is considered to be in deletion if ReconcileDelete has been called for it at least once and not successfully finished (= with RequeueAfter == 0 and no error) since then.
	// This means that Reconcile can safely be called at the beginning of a deletion reconciliation without having to worry about re-creating already deleted resources.
	Reconcile(ctx context.Context, request reconcile.Request, additionalData ...any) (reconcile.Result, error)
	// ReconcileDelete deletes the ClusterRequests and/or AccessRequests for the registered clusters.
	// This function should be called during the deletion of the reconciled object.
	// ctx is the context for the reconciliation.
	// request is the object that is being reconciled.
	// It returns a reconcile.Result and an error if the reconciliation failed.
	// The reconcile.Result may contain a RequeueAfter value to indicate that the reconciliation should be retried after a certain duration.
	// The duration is set by the WithRetryInterval method.
	// Any additional arguments provided are passed into all methods of the ClusterRegistration objects that are called.
	ReconcileDelete(ctx context.Context, request reconcile.Request, additionalData ...any) (reconcile.Result, error)
}

// ClusterContext provides access to request-scoped clusters.
// These clusters include the managed control plane and workload clusters associated with a specific reconcile request.
// (Static clusters like the platform and onboarding clusters are provided to the reconciler when it is initialized.)
//
// More info on the deployment model:
// https://openmcp-project.github.io/docs/about/design/service-provider#deployment-model
type ClusterContext struct {
	// MCPCluster is the managed control plane that belongs to the current reconcile request
	MCPCluster *clusters.Cluster
	// MCPAccessSecretKey provides the object key to retrieve the MCP kubeconfig secret
	MCPAccessSecretKey client.ObjectKey
	// WorkloadCluster is the workload cluster that belongs the current reconcile request
	WorkloadCluster *clusters.Cluster
	// WorkloadAccessSecretKey provides the object key to retrieve the workload cluster kubeconfig secret
	WorkloadAccessSecretKey client.ObjectKey
}
