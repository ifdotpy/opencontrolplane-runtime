// Package workspace runs a service provider whose target is the tenant
// workspace itself (the "singleton per workspace" shape of the kcp-aware
// Service Provider Runtime ADR): enabling the service (APIBinding) is the
// order, there is no service object. For every workspace the multicluster
// provider engages, a Handler installs the service; when the workspace is
// disengaged (binding gone), the Handler removes it.
//
// The package also mints the credential such a service needs to act on the
// workspace: a ServiceAccount in the workspace with a long-lived token and a
// ClusterRoleBinding, rendered as a kubeconfig that points at the workspace's
// own URL (not the provider's virtual workspace).
package workspace

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdlatest "k8s.io/client-go/tools/clientcmd/api/latest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/logging"
)

// Workspace is one engaged tenant workspace.
type Workspace struct {
	// Name is the logical cluster name of the workspace.
	Name string
	// Client acts on the workspace through the provider's virtual workspace.
	Client client.Client
}

// Handler installs and removes the service for a workspace.
type Handler interface {
	// Ensure is called on engagement and retried with backoff until it returns nil.
	Ensure(ctx context.Context, ws Workspace) error
	// Remove is called when the workspace is disengaged (binding removed or
	// workspace deleted). The workspace client may no longer work.
	Remove(ctx context.Context, ws Workspace) error
}

// Runner drives a Handler from multicluster engagements. Add it to the
// multicluster manager.
type Runner struct {
	Handler Handler
	Log     logging.Logger

	mu     sync.Mutex
	active map[string]context.CancelFunc
}

var _ multicluster.Aware = (*Runner)(nil)

// Start satisfies manager.Runnable.
func (r *Runner) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Engage runs Ensure for the workspace and Remove when its context ends.
func (r *Runner) Engage(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	r.mu.Lock()
	if r.active == nil {
		r.active = map[string]context.CancelFunc{}
	}
	if cancel, ok := r.active[string(name)]; ok {
		cancel()
	}
	wsCtx, cancel := context.WithCancel(ctx)
	r.active[string(name)] = cancel
	r.mu.Unlock()

	ws := Workspace{Name: string(name), Client: cl.GetClient()}
	log := r.Log.WithValues("workspace", ws.Name)
	go func() {
		backoff := wait.Backoff{Duration: 2 * time.Second, Factor: 2, Steps: 12, Cap: 2 * time.Minute}
		err := wait.ExponentialBackoffWithContext(wsCtx, backoff, func(ctx context.Context) (bool, error) {
			if err := r.Handler.Ensure(ctx, ws); err != nil {
				log.Error(err, "ensuring service for workspace, retrying")
				return false, nil
			}
			return true, nil
		})
		if err == nil {
			log.Info("Service ensured for workspace")
		}
		<-wsCtx.Done()
		if errors.Is(ctx.Err(), context.Canceled) {
			// The manager itself is stopping; keep the installation.
			return
		}
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer rmCancel()
		if err := r.Handler.Remove(rmCtx, ws); err != nil {
			log.Error(err, "removing service for disengaged workspace")
			return
		}
		log.Info("Service removed for disengaged workspace")
	}()
	return nil
}

// TokenSpec describes the workspace credential to mint.
type TokenSpec struct {
	Namespace          string // created if missing
	ServiceAccountName string
	// ClusterRole bound to the service account in the workspace (e.g. cluster-admin).
	ClusterRole string
}

// MintKubeconfig ensures a ServiceAccount with a long-lived token in the
// workspace and returns a kubeconfig for the workspace's own URL, derived from
// the provider's REST config (same kcp front proxy, same CA).
func MintKubeconfig(ctx context.Context, ws Workspace, providerCfg *rest.Config, spec TokenSpec) ([]byte, error) {
	c := ws.Client
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: spec.Namespace}}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("namespace %q: %w", spec.Namespace, err)
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: spec.ServiceAccountName, Namespace: spec.Namespace}}
	if err := c.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("serviceaccount: %w", err)
	}
	secretName := spec.ServiceAccountName + "-token"
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: spec.Namespace,
			Annotations: map[string]string{corev1.ServiceAccountNameKey: spec.ServiceAccountName}},
		Type: corev1.SecretTypeServiceAccountToken,
	}
	if err := c.Create(ctx, sec); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("token secret: %w", err)
	}
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: spec.ServiceAccountName + "-" + spec.ClusterRole},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: spec.ServiceAccountName, Namespace: spec.Namespace}},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: spec.ClusterRole},
	}
	if err := c.Create(ctx, crb); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("clusterrolebinding: %w", err)
	}

	var token []byte
	err := wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		got := &corev1.Secret{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: spec.Namespace, Name: secretName}, got); err != nil {
			return false, nil
		}
		token = got.Data[corev1.ServiceAccountTokenKey]
		return len(token) > 0, nil
	})
	if err != nil {
		return nil, fmt.Errorf("waiting for service account token: %w", err)
	}

	server, err := WorkspaceURL(providerCfg.Host, ws.Name)
	if err != nil {
		return nil, err
	}
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["workspace"] = &clientcmdapi.Cluster{Server: server, CertificateAuthorityData: providerCfg.CAData}
	cfg.AuthInfos["service"] = &clientcmdapi.AuthInfo{Token: string(token)}
	cfg.Contexts["workspace"] = &clientcmdapi.Context{Cluster: "workspace", AuthInfo: "service", Namespace: spec.Namespace}
	cfg.CurrentContext = "workspace"
	return clientcmd.Write(*cfg)
}

// WorkspaceURL turns the provider's virtual-workspace host into the direct URL
// of a workspace: https://<kcp>/clusters/<logical cluster name>.
func WorkspaceURL(providerHost, logicalCluster string) (string, error) {
	u, err := url.Parse(providerHost)
	if err != nil {
		return "", fmt.Errorf("parsing provider host %q: %w", providerHost, err)
	}
	base := u.Scheme + "://" + u.Host
	if i := strings.Index(u.Path, "/services/"); i > 0 {
		base += u.Path[:i]
	}
	return base + "/clusters/" + logicalCluster, nil
}

var _ = clientcmdlatest.Version // keep the api/latest package linked for kubeconfig writing
