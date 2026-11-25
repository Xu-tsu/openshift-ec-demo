"""Scaling Live View: a tiny dashboard backend for the OpenShift autoscaling demo.

Python 3.9 standard library only. Runs in-cluster with its ServiceAccount and reads:
  - frontend Pods            (Kubernetes API)
  - per-Pod CPU              (Metrics API, same source the HPA uses)
  - HPA status               (autoscaling/v2)
  - per-Pod requests/second  (OpenShift router / HAProxy metrics via Thanos Querier)
It can also switch the load generator between "normal" and "spike".
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
        for c in p.get("spec", {}).get("containers", []):
            cpu = c.get("resources", {}).get("requests", {}).get("cpu")
            if cpu:
                req_m += cpu_millicores(cpu)
        name = md["name"]
        plist.append({
            "name": name,
            "phase": phase,
            "created": md.get("creationTimestamp", ""),
            "cpu_m": round(usage.get(name, 0.0), 1),
            "cpu_request_m": req_m,
            "rps": round(rps_by_pod.get(name, 0.0), 2),
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

    return out


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
        if self.path.split("?", 1)[0] != "/api/load":
            return self._send(404, '{"error":"not found"}')
        # Simple CSRF guard: browsers cannot add this header cross-site without CORS.
        if self.headers.get("X-Requested-With") != "scaling-view":
            return self._send(403, '{"error":"forbidden"}')
        try:
            length = int(self.headers.get("Content-Length") or 0)
            level = json.loads(self.rfile.read(length) or b"{}").get("level")
            if level not in LEVELS:
                return self._send(400, '{"error":"level must be normal or spike"}')
            set_load(level)
            self._send(200, json.dumps({"ok": True, "level": level}))
        except Exception as e:  # noqa: BLE001
            self._send(500, json.dumps({"error": str(e)}))

    def log_message(self, fmt, *args):
        if not self.path.startswith(("/api/state", "/healthz")):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    print("scaling-view listening on :%d (namespace=%s, deployment=%s)" % (PORT, NS, APP), flush=True)
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()
