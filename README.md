# openshift-ec-demo

A "zero-downtime EC site" demo on Red Hat OpenShift, based on
[Google Online Boutique](https://github.com/GoogleCloudPlatform/microservices-demo) v0.10.7.

| Path | What it is |
|---|---|
| `k8s/` | Everything Argo CD (OpenShift GitOps) keeps in sync: 11 microservices + Redis, Route, HPA, ImageStream, Pipeline |
| `src/frontend/` | Frontend source, built in-cluster by OpenShift Pipelines (Tekton + Buildah) |
| `argocd/application.yaml` | The Argo CD Application pointing at `k8s/` |
| `tools/` | Load generator and a PipelineRun template (not managed by Argo CD) |

## Flow

1. **GitOps** – commit to `k8s/` → OpenShift GitOps syncs → rolling update without downtime
   (try `FRONTEND_MESSAGE` / `BANNER_COLOR` in `k8s/boutique.yaml`).
2. **Pipelines** – change `src/frontend/` → start `build-frontend` → image pushed to the internal registry →
   ImageStream trigger rolls the frontend out automatically.
3. **Autoscaling** – raise load with `tools/loadgenerator.yaml` → HPA scales the frontend from 1 to 5 pods.

## Setup (once)

```sh
oc new-project boutique
oc apply -f tools/loadgenerator.yaml -n boutique
oc label namespace boutique argocd.argoproj.io/managed-by=openshift-gitops   # cluster-admin
oc tag --source=docker us-central1-docker.pkg.dev/online-boutique-ci/microservices-demo/frontend:v0.10.7 boutique/frontend:latest
oc apply -f argocd/application.yaml                                          # cluster-admin
```

## Changes from upstream

- No fixed `runAsUser`/`fsGroup` – OpenShift SCC `restricted-v2` assigns a random non-root UID.
- `Route` instead of a `LoadBalancer` Service.
- Smaller resource requests so it fits on OpenShift Local.
- Frontend image comes from an ImageStream built by OpenShift Pipelines.

Source files keep their original Apache-2.0 headers. See `LICENSE` and `NOTICE`.
This is a demo, not a real shop.
