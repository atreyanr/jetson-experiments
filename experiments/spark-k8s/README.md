# Spark on K8s (Jetson Single-Node)

Spark Connect server running on k3s, tuned for the Jetson Orin Nano 8GB.

## Architecture

```
JupyterLab (.venv)
  └─ sc://localhost:15002 (gRPC)
       └─ K8s Service (LoadBalancer)
            └─ Spark Connect Pod (driver)
                 └─ Executor Pod(s) — created dynamically via K8s API
```

Your notebook talks to the Connect server over gRPC. The server runs as
a K8s Deployment and creates/destroys executor pods on demand through the
K8s API. No YARN, no standalone cluster manager.

## Prerequisites

1. **k3s** installed and running:

   ```bash
   curl -sfL https://get.k3s.io | sh -
   sudo k3s kubectl get nodes          # should show Ready
   ```

   Optional — avoid typing `sudo k3s kubectl` every time:

   ```bash
   mkdir -p ~/.kube
   sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
   sudo chown $(id -u):$(id -g) ~/.kube/config
   alias kubectl='k3s kubectl'
   ```

2. **Spark image** available to k3s.

   The manifests default to `apache/spark:4.1.2` (multi-arch, includes
   ARM64). k3s pulls it automatically on first deploy.

   To use your existing Docker image instead:

   ```bash
   docker save your-spark-image:tag -o /tmp/spark.tar
   sudo k3s ctr images import /tmp/spark.tar
   ```

   Then update the image references in `k8s/02-spark-connect.yaml`.

3. **pyspark** in your notebook venv (you already have this).

## Deploy

```bash
sudo k3s kubectl apply -f k8s/
```

Or one at a time:

```bash
sudo k3s kubectl apply -f k8s/00-namespace.yaml
sudo k3s kubectl apply -f k8s/01-rbac.yaml
sudo k3s kubectl apply -f k8s/02-spark-connect.yaml
```

## Verify

```bash
# Pod running?
sudo k3s kubectl -n spark get pods -w

# Service bound to host ports?
sudo k3s kubectl -n spark get svc

# Connect server logs
sudo k3s kubectl -n spark logs -l app=spark-connect -f
```

Wait for the pod to show `1/1 Running` and the readiness probe to pass
(~30 s), then test from your notebook:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .getOrCreate()
)

spark.range(10).show()
```

Or run the smoke test:

```bash
source ../../.venv/bin/activate
python test-connection.py
```

## Spark UI

<http://localhost:4040> — jobs, stages, executor status.

## Tuning for 8 GB

Edit the ConfigMap in `k8s/02-spark-connect.yaml`:

| Setting | Default | Notes |
|---------|---------|-------|
| `spark.driver.memory` | 768m | Connect server overhead |
| `spark.executor.memory` | 768m | Per executor pod |
| `spark.executor.memoryOverhead` | 256m | JVM off-heap per executor |
| `spark.executor.instances` | 1 | Bump to 2 if memory allows |
| `spark.executor.cores` | 2 | Orin Nano has 6 CPU cores |

**Memory budget (~8 GB total):**

| Component | Allocation |
|-----------|------------|
| k3s system | ~400 MB |
| Spark driver pod | ~1 GB |
| Spark executor pod (×1) | ~1 GB |
| JupyterLab + venv | ~500 MB |
| OS + headless services | ~1 GB |
| **Remaining (CUDA work)** | **~4 GB** |

After editing, re-apply:

```bash
sudo k3s kubectl -n spark rollout restart deployment/spark-connect
```

## Teardown

```bash
sudo k3s kubectl delete namespace spark
```

This removes everything — pods, services, RBAC, quota.

## Scaling later

When you're ready to add nodes (more Jetsons, cloud VMs via Tailscale):

1. Join them to k3s:
   ```bash
   curl -sfL https://get.k3s.io | K3S_URL=https://<master>:6443 K3S_TOKEN=<token> sh -
   ```
2. Bump `spark.executor.instances` in the ConfigMap
3. Spark will schedule executor pods across the cluster automatically
