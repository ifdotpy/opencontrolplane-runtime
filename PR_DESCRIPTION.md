<!-- local draft, not committed; PR target: openmcp-project/opencontrolplane-runtime, base main -->
# feat(serviceprovider): multicluster deployment mode for APIReconciler

## What

Adds an optional multicluster deployment mode to `APIReconciler`, as proposed in
openmcp-project/openmcp-operator#350.

- `refactor` commit: extracts a mode-independent reconcile core behind a private
  per-request seam (tenant client + access-request key). **No behavior change**:
  the classic single-onboarding-cluster path resolves both statically, and the
  existing envtest suite passes unchanged.
- `feat` commit: `MustBuildMulticluster()` + `SetupWithMulticlusterManager(mgr, name)`.
  The reconciler can be driven by a [multicluster-runtime](https://github.com/kubernetes-sigs/multicluster-runtime)
  manager whose provider supplies one logical cluster per tenant, e.g. kcp
  workspaces engaged through an APIExport virtual workspace
  ([kcp-dev/multicluster-provider](https://github.com/kcp-dev/multicluster-provider),
  the stack `api-syncagent` runs on). Objects are reconciled in place in the
  tenant's cluster and status is written back there; no object synchronization.

Cluster access objects (ClusterRequests/AccessRequests) on the platform cluster
get a stable per-tenant prefix: a name-safe logical cluster name (the kcp case)
is used verbatim, so the mapping is collision-free; anything else falls back to
the codebase's standard short hash. Without a cluster name the key is
unchanged; classic naming is untouched, and the downstream naming helpers
already shorten long names safely.

## Scope notes

- The classic mode is unchanged; both modes share one reconcile core, so the
  existing tests cover the shared path.
- Platform-side watches are not wired in the multicluster mode yet, and the
  gaps fail closed instead of silently degrading: a missing ProviderConfig is
  retried on a fixed interval (no config watch exists to recover it), and
  Secret/ConfigMap watching or the legacy `ClusterAccessReconciler` are
  rejected at build time. Cross-mode misuse fails fast at setup in both
  directions.
- When a tenant cluster disappears, in-flight requests trigger a best-effort
  cleanup of its platform-side access objects. A full garbage collector for
  disengaged clusters is a named follow-up.
- The library depends only on `sigs.k8s.io/multicluster-runtime` (v0.24.1,
  matching the repo's controller-runtime v0.24.1). The kcp provider stays in
  the service provider's `main.go`, no kcp dependency is added here.

## How to test

```sh
task envtest:setup
KUBEBUILDER_ASSETS=$PWD/bin/k8s/<version> go test -cover -race ./...
```

- Existing suite: green (classic path, envtest).
- New: `TestTenantAccessKey` (determinism, classic compatibility, verbatim vs
  hashed prefixes, cross-cluster collision-freedom) and
  `TestMulticlusterModeGuards` (build-time and setup-time fail-fast guards).
- Live: this branch was run against a live kcp installation using the
  library's own testdata Foo API on the multicluster path: objects in two
  tenant workspaces reconciled through one APIExport virtual workspace
  (finalizers added, status written in place, ProviderConfig read from the
  platform side); the missing-ProviderConfig requeue recovered within its
  interval after the config was recreated; deletion ran the full finalizer
  flow in both workspaces. Downstream check: service-provider-flux and
  service-provider-external-secrets compile and pass their unit tests against
  this branch unchanged. Evidence available; happy to demo on a call.

Refs openmcp-project/openmcp-operator#350

🤖 Generated with [Claude Code](https://claude.com/claude-code)
