package workspace

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/logging"
)

// recordingHandler counts Ensure and Remove calls; Ensure fails failEnsure times first.
type recordingHandler struct {
	mu         sync.Mutex
	failEnsure int
	ensures    int
	removes    int
	workspaces []string
}

func (h *recordingHandler) Ensure(_ context.Context, ws Workspace) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ensures++
	h.workspaces = append(h.workspaces, ws.Name)
	if h.failEnsure > 0 {
		h.failEnsure--
		return errors.New("not yet")
	}
	return nil
}

func (h *recordingHandler) Remove(context.Context, Workspace) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removes++
	return nil
}

func (h *recordingHandler) counts() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ensures, h.removes
}

// stubCluster satisfies cluster.Cluster for the Runner, which only uses GetClient.
type stubCluster struct {
	cluster.Cluster
	c client.Client
}

func (s stubCluster) GetClient() client.Client { return s.c }

func newRunner(h Handler) *Runner {
	log, _ := logging.GetLogger()
	return &Runner{Handler: h, Log: log}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 15*time.Second, 50*time.Millisecond)
}

func TestRunnerEnsureIsRetriedUntilItSucceeds(t *testing.T) {
	h := &recordingHandler{failEnsure: 1}
	r := newRunner(h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, r.Engage(ctx, multicluster.ClusterName("ws-a"), stubCluster{c: fake.NewClientBuilder().Build()}))
	eventually(t, func() bool { e, _ := h.counts(); return e == 2 })
	_, removes := h.counts()
	assert.Equal(t, 0, removes, "Remove must not run while the engagement is alive")
	assert.Equal(t, []string{"ws-a", "ws-a"}, h.workspaces)
}

func TestRunnerRemovesServiceWhenProviderDisengages(t *testing.T) {
	h := &recordingHandler{}
	r := newRunner(h)
	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, r.Engage(ctx, multicluster.ClusterName("ws-a"), stubCluster{c: fake.NewClientBuilder().Build()}))
	eventually(t, func() bool { e, _ := h.counts(); return e == 1 })
	cancel() // the apiexport provider cancels the engagement when the binding is gone
	eventually(t, func() bool { _, rm := h.counts(); return rm == 1 })
}

func TestRunnerKeepsServiceWhenManagerStops(t *testing.T) {
	h := &recordingHandler{}
	r := newRunner(h)
	mgrCtx, stopMgr := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() { close(started); _ = r.Start(mgrCtx) }()
	<-started

	engCtx, cancelEng := context.WithCancel(context.Background())
	require.NoError(t, r.Engage(engCtx, multicluster.ClusterName("ws-a"), stubCluster{c: fake.NewClientBuilder().Build()}))
	eventually(t, func() bool { e, _ := h.counts(); return e == 1 })

	stopMgr()
	eventually(t, r.isStopping)
	cancelEng() // engagements end with the manager; the installation must stay
	time.Sleep(300 * time.Millisecond)
	_, removes := h.counts()
	assert.Equal(t, 0, removes)
}

func TestRunnerReengagementReplacesTheFirstWithoutRemove(t *testing.T) {
	h := &recordingHandler{}
	r := newRunner(h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cl := stubCluster{c: fake.NewClientBuilder().Build()}

	require.NoError(t, r.Engage(ctx, multicluster.ClusterName("ws-a"), cl))
	eventually(t, func() bool { e, _ := h.counts(); return e == 1 })
	// A second Engage for the same workspace (provider reconnect) cancels the
	// first one. The workspace is still engaged, so nothing is removed.
	require.NoError(t, r.Engage(ctx, multicluster.ClusterName("ws-a"), cl))
	eventually(t, func() bool { e, _ := h.counts(); return e == 2 })
	time.Sleep(300 * time.Millisecond)
	_, removes := h.counts()
	assert.Equal(t, 0, removes)
	cancel() // now the provider disengages: the current engagement removes once
	eventually(t, func() bool { _, rm := h.counts(); return rm == 1 })
}

func TestMintKubeconfig(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rbacv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	ws := Workspace{Name: "2gmk17txekqnrtfb", Client: c}
	providerCfg := &rest.Config{Host: "https://kcp.example.test/services/apiexport/abc/flux.services", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("CA")}}
	spec := TokenSpec{Namespace: "flux-system", ServiceAccountName: "flux", ClusterRole: "cluster-admin"}

	// kcp's token controller fills the token; the test does that once the Secret exists.
	go func() {
		sec := &corev1.Secret{}
		for {
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: "flux-system", Name: "flux-token"}, sec); err == nil {
				sec.Data = map[string][]byte{corev1.ServiceAccountTokenKey: []byte("tok3n")}
				_ = c.Update(context.Background(), sec)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	kubeconfig, err := MintKubeconfig(context.Background(), ws, providerCfg, spec)
	require.NoError(t, err)

	cfg, err := clientcmd.Load(kubeconfig)
	require.NoError(t, err)
	cl := cfg.Clusters[cfg.Contexts[cfg.CurrentContext].Cluster]
	assert.Equal(t, "https://kcp.example.test/clusters/2gmk17txekqnrtfb", cl.Server)
	assert.Equal(t, []byte("CA"), cl.CertificateAuthorityData)
	assert.Equal(t, "tok3n", cfg.AuthInfos[cfg.Contexts[cfg.CurrentContext].AuthInfo].Token)
	assert.Equal(t, "flux-system", cfg.Contexts[cfg.CurrentContext].Namespace)

	ctx := context.Background()
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "flux-system"}, &corev1.Namespace{}))
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "flux-system", Name: "flux"}, &corev1.ServiceAccount{}))
	sec := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Namespace: "flux-system", Name: "flux-token"}, sec))
	assert.Equal(t, corev1.SecretTypeServiceAccountToken, sec.Type)
	assert.Equal(t, "flux", sec.Annotations[corev1.ServiceAccountNameKey])
	crb := &rbacv1.ClusterRoleBinding{}
	require.NoError(t, c.Get(ctx, client.ObjectKey{Name: "flux-cluster-admin"}, crb))
	assert.Equal(t, rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cluster-admin"}, crb.RoleRef)
	assert.Equal(t, []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: "flux", Namespace: "flux-system"}}, crb.Subjects)

	// idempotent: everything exists already, the token is read again
	again, err := MintKubeconfig(ctx, ws, providerCfg, spec)
	require.NoError(t, err)
	assert.Equal(t, kubeconfig, again)
}

func TestWorkspaceURL(t *testing.T) {
	for _, tc := range []struct{ host, cluster, want string }{
		{"https://kcp.example.test/services/apiexport/abc/flux.services", "ws1", "https://kcp.example.test/clusters/ws1"},
		{"https://kcp.example.test", "ws1", "https://kcp.example.test/clusters/ws1"},
		{"https://kcp.example.test:6443/", "ws1", "https://kcp.example.test:6443/clusters/ws1"},
	} {
		got, err := WorkspaceURL(tc.host, tc.cluster)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
	_, err := WorkspaceURL("://bad", "ws1")
	assert.Error(t, err)
}
