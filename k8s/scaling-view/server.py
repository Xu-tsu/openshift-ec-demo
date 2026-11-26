"""Scaling Live View: a tiny dashboard backend for the OpenShift autoscaling demo.

Python 3.9 standard library only. Runs in-cluster with its ServiceAccount and reads:
  - frontend Pods            (Kubernetes API)
  - per-Pod CPU              (Metrics API, same source the HPA uses)
  - HPA status               (autoscaling/v2)
  - per-Pod requests/second  (OpenShift router / HAProxy metrics via Thanos Querier)
  - the latest build-frontend PipelineRun (OpenShift Pipelines)
Demo scenarios it can trigger:
  - load:   switch the load generator between "normal" and "spike"
  - chaos:  force-kill one frontend Pod (simulated server failure)
  - update: start the build-frontend Pipeline (new image -> rolling update via ImageStream trigger)
"""
import json
import os
import ssl
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NS = os.environ.get("TARGET_NAMESPACE", "boutique")
APP = os.environ.get("TARGET_DEPLOYMENT", "frontend")
ROUTE = os.environ.get("TARGET_ROUTE", "frontend")
LOADGEN = os.environ.get("LOADGEN_DEPLOYMENT", "loadgenerator")
PIPELINE = os.environ.get("PIPELINE", "build-frontend")
KEEP_RUNS = 2  # older PipelineRuns (and their PVCs) are pruned when a new one starts
THANOS = os.environ.get("THANOS_URL", "https://thanos-querier.openshift-monitoring.svc:9091")
PORT = int(os.environ.get("PORT", "8080"))
SA = "/var/run/secrets/kubernetes.io/serviceaccount"
API = "https://kubernetes.default.svc"
HERE = os.path.dirname(os.path.abspath(__file__))

LEVELS = {
    "normal": {"users": "10", "rate": "1", "replicas": 1},
    "spike": {"users": "300", "rate": "30", "replicas": 3},
}

API_CTX = ssl.create_default_context(cafile=SA + "/ca.crt")
PROM_CTX = ssl.create_default_context(cafile=SA + "/service-ca.crt")


def _token():
    with open(SA + "/token") as f:
        return f.read().strip()


def _call(url, ctx, method="GET", body=None, ctype=None, timeout=5):
    req = urllib.request.Request(url, method=method, data=body)
    req.add_header("Authorization", "Bearer " + _token())
    req.add_header("Accept", "application/json")
    if ctype:
        req.add_header("Content-Type", ctype)
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
        return json.loads(resp.read())


def kube(path, **kw):
    return _call(API + path, API_CTX, **kw)


def prom(query):
    url = THANOS + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    return _call(url, PROM_CTX)["data"]["result"]


def cpu_millicores(value):
    if value.endswith("n"):
        return int(value[:-1]) / 1e6
    if value.endswith("u"):
        return int(value[:-1]) / 1e3
    if value.endswith("m"):
        return float(value[:-1])
    return float(value) * 1000


def collect():
    out = {"time": time.time(), "namespace": NS, "deployment": APP, "errors": []}
    selector = urllib.parse.quote("app=" + APP)

    try:
        pods = kube("/api/v1/namespaces/%s/pods?labelSelector=%s" % (NS, selector))["items"]
    except Exception as e:  # noqa: BLE001 - report and keep going
        pods = []
        out["errors"].append("pods: %s" % e)

    usage = {}
    try:
        items = kube("/apis/metrics.k8s.io/v1beta1/namespaces/%s/pods?labelSelector=%s" % (NS, selector))["items"]
        for m in items:
            usage[m["metadata"]["name"]] = sum(cpu_millicores(c["usage"]["cpu"]) for c in m["containers"])
    except Exception as e:  # noqa: BLE001
        out["errors"].append("metrics: %s" % e)

    rps_by_pod, by_code, errors_5m = {}, {}, 0.0
    sel = 'exported_namespace="%s",route="%s"' % (NS, ROUTE)
    series = "haproxy_server_http_responses_total{%s}" % sel
    try:
        for r in prom("sum by (exported_pod) (rate(%s[1m]))" % series):
            rps_by_pod[r["metric"].get("exported_pod", "")] = float(r["value"][1])
        for r in prom("sum by (code) (rate(%s[1m]))" % series):
            by_code[r["metric"].get("code", "")] = float(r["value"][1])
        res = prom('sum(increase(haproxy_server_http_responses_total{%s,code=~"4xx|5xx"}[5m]))' % sel)
        errors_5m = float(res[0]["value"][1]) if res else 0.0
    except Exception as e:  # noqa: BLE001
        out["errors"].append("prometheus: %s" % e)

    total = sum(by_code.values())
    bad = by_code.get("4xx", 0.0) + by_code.get("5xx", 0.0)
    out["traffic"] = {
        "rps": round(total, 2),
        "error_rps": round(bad, 3),
        "success_rate": round(100.0 * (total - bad) / total, 2) if total > 0 else None,
        "errors_5m": int(round(errors_5m)),
    }

    plist = []
    for p in pods:
        md, st = p["metadata"], p.get("status", {})
        conds = {c["type"]: c["status"] for c in st.get("conditions", []) or []}
        if md.get("deletionTimestamp"):
            phase = "terminating"
        elif conds.get("Ready") == "True":
            phase = "ready"
        else:
            phase = "starting"
        req_m = 0.0
        containers = p.get("spec", {}).get("containers", [])
        for c in containers:
            cpu = c.get("resources", {}).get("requests", {}).get("cpu")
            if cpu:
                req_m += cpu_millicores(cpu)
        image = containers[0].get("image", "") if containers else ""
        version = image.split("@sha256:", 1)[1][:7] if "@sha256:" in image else image.rsplit(":", 1)[-1][:7]
        name = md["name"]
        plist.append({
            "name": name,
            "phase": phase,
            "created": md.get("creationTimestamp", ""),
            "cpu_m": round(usage.get(name, 0.0), 1),
            "cpu_request_m": req_m,
            "rps": round(rps_by_pod.get(name, 0.0), 2),
            "version": version,
        })
    plist.sort(key=lambda x: x["created"])
    out["pods"] = plist

    try:
        hpa = kube("/apis/autoscaling/v2/namespaces/%s/horizontalpodautoscalers/%s" % (NS, APP))
        spec, st = hpa["spec"], hpa.get("status", {})
        target, current = None, None
        for m in spec.get("metrics", []) or []:
            if m.get("type") == "Resource":
                target = m["resource"]["target"].get("averageUtilization")
        for m in st.get("currentMetrics", []) or []:
            if m.get("type") == "Resource":
                current = m["resource"].get("current", {}).get("averageUtilization")
        out["hpa"] = {
            "min": spec.get("minReplicas", 1),
            "max": spec.get("maxReplicas", 1),
            "current": st.get("currentReplicas", 0),
            "desired": st.get("desiredReplicas", 0),
            "cpu": current,
            "target": target,
        }
    except Exception as e:  # noqa: BLE001
        out["errors"].append("hpa: %s" % e)

    try:
        d = kube("/apis/apps/v1/namespaces/%s/deployments/%s" % (NS, LOADGEN))
        env = {e["name"]: e.get("value") for e in d["spec"]["template"]["spec"]["containers"][0].get("env", [])}
        replicas = d["spec"].get("replicas") or 0
        users = int(env.get("USERS") or 0) * replicas
        out["load"] = {"users": users, "generators": replicas, "level": "spike" if users > 100 else "normal"}
    except Exception as e:  # noqa: BLE001
        out["errors"].append("loadgenerator: %s" % e)

    try:
        d = kube("/apis/apps/v1/namespaces/%s/deployments/%s" % (NS, APP))
        st = d.get("status", {})
        out["rollout"] = {
            "replicas": st.get("replicas", 0),
            "updated": st.get("updatedReplicas", 0),
            "available": st.get("availableReplicas", 0),
            "complete": st.get("observedGeneration", 0) >= d["metadata"].get("generation", 0)
            and st.get("updatedReplicas", 0) == st.get("replicas", 0) == st.get("availableReplicas", 0),
        }
    except Exception as e:  # noqa: BLE001
        out["errors"].append("rollout: %s" % e)

    try:
        out["pipeline"] = latest_pipeline_run()
    except Exception as e:  # noqa: BLE001
        out["errors"].append("pipeline: %s" % e)

    return out


def _pipeline_runs():
    sel = urllib.parse.quote("tekton.dev/pipeline=" + PIPELINE)
    runs = kube("/apis/tekton.dev/v1/namespaces/%s/pipelineruns?labelSelector=%s" % (NS, sel))["items"]
    return sorted(runs, key=lambda r: r["metadata"].get("creationTimestamp", ""))


def latest_pipeline_run():
    runs = _pipeline_runs()
    if not runs:
        return None
    run = runs[-1]
    name = run["metadata"]["name"]
    conds = run.get("status", {}).get("conditions") or [{}]
    sel = urllib.parse.quote("tekton.dev/pipelineRun=" + name)
    tasks = {}
    for tr in kube("/apis/tekton.dev/v1/namespaces/%s/taskruns?labelSelector=%s" % (NS, sel))["items"]:
        task = tr["metadata"].get("labels", {}).get("tekton.dev/pipelineTask", tr["metadata"]["name"])
        tc = (tr.get("status", {}).get("conditions") or [{}])[0]
        tasks[task] = {"status": tc.get("status", "Unknown"), "reason": tc.get("reason", "Pending")}
    return {
        "name": name,
        "status": conds[0].get("status", "Unknown"),
        "reason": conds[0].get("reason", "Pending"),
        "start": run.get("status", {}).get("startTime") or run["metadata"].get("creationTimestamp"),
        "end": run.get("status", {}).get("completionTime"),
        "tasks": tasks,
    }


def chaos():
    """Force-kill one ready frontend Pod, like a server crash (no graceful shutdown)."""
    selector = urllib.parse.quote("app=" + APP)
    pods = kube("/api/v1/namespaces/%s/pods?labelSelector=%s" % (NS, selector))["items"]
    ready = [p for p in pods if not p["metadata"].get("deletionTimestamp")
             and any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", []) or [])]
    if len(ready) < 2:
        raise ValueError("稼働中の Pod が %d 台のため停止できません（冗長化には 2 台以上が必要）" % len(ready))
    victim = sorted(ready, key=lambda p: p["metadata"].get("creationTimestamp", ""))[0]["metadata"]["name"]
    kube("/api/v1/namespaces/%s/pods/%s" % (NS, victim), method="DELETE",
         body=json.dumps({"gracePeriodSeconds": 0}).encode(), ctype="application/json")
    return victim


def start_update():
    """Start the build-frontend Pipeline; the ImageStream trigger then rolls the new image out."""
    runs = _pipeline_runs()
    if runs and (runs[-1].get("status", {}).get("conditions") or [{}])[0].get("status") == "Unknown":
        raise ValueError("ビルドが実行中です: " + runs[-1]["metadata"]["name"])
    for old in runs[:-KEEP_RUNS] if len(runs) >= KEEP_RUNS else []:
        kube("/apis/tekton.dev/v1/namespaces/%s/pipelineruns/%s" % (NS, old["metadata"]["name"]), method="DELETE")
    body = {
        "apiVersion": "tekton.dev/v1",
        "kind": "PipelineRun",
        "metadata": {"generateName": PIPELINE + "-", "labels": {"app.kubernetes.io/created-by": "scaling-view"}},
        "spec": {
            "pipelineRef": {"name": PIPELINE},
            "taskRunTemplate": {"serviceAccountName": "pipeline"},
            "workspaces": [{"name": "source", "volumeClaimTemplate": {"spec": {
                "accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}}}],
        },
    }
    run = kube("/apis/tekton.dev/v1/namespaces/%s/pipelineruns" % NS, method="POST",
               body=json.dumps(body).encode(), ctype="application/json")
    return run["metadata"]["name"]


def set_load(level):
    lv = LEVELS[level]
    patch = {"spec": {"replicas": lv["replicas"], "template": {"spec": {"containers": [{
        "name": "main",
        "env": [{"name": "USERS", "value": lv["users"]}, {"name": "RATE", "value": lv["rate"]}],
    }]}}}}
    kube("/apis/apps/v1/namespaces/%s/deployments/%s" % (NS, LOADGEN), method="PATCH",
         body=json.dumps(patch).encode(), ctype="application/strategic-merge-patch+json")


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif path == "/api/state":
            self._send(200, json.dumps(collect()))
        elif path == "/healthz":
            self._send(200, "ok", "text/plain")
        else:
            self._send(404, '{"error":"not found"}')

    def do_POST(self):  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path not in ("/api/load", "/api/chaos", "/api/update"):
            return self._send(404, '{"error":"not found"}')
        # Simple CSRF guard: browsers cannot add this header cross-site without CORS.
        if self.headers.get("X-Requested-With") != "scaling-view":
            return self._send(403, '{"error":"forbidden"}')
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            if path == "/api/load":
                level = body.get("level")
                if level not in LEVELS:
                    return self._send(400, '{"error":"level must be normal or spike"}')
                set_load(level)
                return self._send(200, json.dumps({"ok": True, "level": level}))
            if path == "/api/chaos":
                return self._send(200, json.dumps({"ok": True, "pod": chaos()}, ensure_ascii=False))
            return self._send(200, json.dumps({"ok": True, "pipelinerun": start_update()}, ensure_ascii=False))
        except ValueError as e:
            self._send(409, json.dumps({"error": str(e)}, ensure_ascii=False))
        except Exception as e:  # noqa: BLE001
            self._send(500, json.dumps({"error": str(e)}, ensure_ascii=False))

    def log_message(self, fmt, *args):
        if not self.path.startswith(("/api/state", "/healthz")):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    print("scaling-view listening on :%d (namespace=%s, deployment=%s)" % (PORT, NS, APP), flush=True)
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()
