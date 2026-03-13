# Tansu Helm Chart

[Tansu](https://github.com/tansu-io/tansu) is an Apache Kafka-compatible broker with pluggable storage backends. This chart runs the Tansu broker as a Deployment with configurable replicas, autoscaling, and storage engine.

## Prerequisites

- Kubernetes 1.23+
- Helm 3+

## Install

```bash
helm repo add tansu https://tansu-io.github.io/tansu
helm install tansu ./helm/tansu
```

## Configuration

### Replicas and autoscaling

- **Replicas**: Set `replicaCount` (used when autoscaling is disabled).
- **Autoscaling**: Enable HPA with `autoscaling.enabled: true`, then set:
  - `autoscaling.minReplicas` / `autoscaling.maxReplicas`
  - `autoscaling.targetCPUUtilizationPercentage` (and optionally `targetMemoryUtilizationPercentage`)

Example:

```yaml
replicaCount: 2

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80
```

### Storage engine

Set `storageEngine.type` to one of: **memory**, **s3**, **postgres**, **sqlite**.

| Type      | Use case        | Required config |
|-----------|------------------|-----------------|
| `memory`  | Dev / tests      | `storageEngine.memory.prefix` (default `tansu`) |
| `s3`      | Production (S3) | `storageEngine.s3.bucket`, and credentials via `storageEngine.s3.existingSecret` (keys `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`). Optional: `endpoint`, `region`, `allowHttp` |
| `postgres`| Production       | `storageEngine.postgres.host`, `user`, `database`. Set `storageEngine.postgres.password` or use `storageEngine.postgres.existingSecret` (key `password`) for the DB password |
| `sqlite`  | Single-node / dev | `storageEngine.sqlite.path` (default `/data/tansu.db`). Uses `emptyDir` by default; for persistence use `extraVolumes` / `extraVolumeMounts` with a PVC |

Example – S3 with existing secret:

```yaml
storageEngine:
  type: s3
  s3:
    bucket: tansu
    region: eu-west-2
    endpoint: ""  # omit for default AWS
    allowHttp: "true"
    existingSecret: tansu-s3-credentials
```

Example – Postgres with password from secret:

```yaml
storageEngine:
  type: postgres
  postgres:
    host: postgres
    port: 5432
    database: postgres
    user: postgres
    existingSecret: tansu-postgres-password
    existingSecretPasswordKey: password
```

For a fully custom storage URL (e.g. postgres URL with special characters), use a Secret and reference it:

```yaml
storageUrlFromSecret:
  name: tansu-storage-url
  key: STORAGE_ENGINE
```

Create the secret with the exact URL, e.g.:

```bash
kubectl create secret generic tansu-storage-url --from-literal=STORAGE_ENGINE='postgres://user:pass@host:5432/db'
```

### Image

Default image: `ghcr.io/tansu-io/tansu`. Override with:

```yaml
image:
  repository: ghcr.io/tansu-io/tansu
  tag: "v1.0.0"
  pullPolicy: IfNotPresent
```

### Advertised listener

Clients use the advertised listener to connect. In-cluster default is `tansu:9092`. For a specific namespace or release name:

```yaml
advertisedListener: "tansu.my-namespace.svc.cluster.local:9092"
```

### Kafka LoadBalancer

To expose Kafka (TCP 9092) externally via a cloud LoadBalancer, enable the dedicated Kafka LoadBalancer service. The main service stays ClusterIP for in-cluster traffic.

```yaml
kafkaLoadBalancer:
  enabled: true
  port: 9092
  # annotations:  # optional, e.g. for AWS NLB
  #   service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
  #   service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"
```

Set `advertisedListener` to the external host and port clients will use (e.g. the LoadBalancer hostname or IP and `9092`), so broker metadata points clients to the right address.

### Ingress

Standard Ingress is HTTP/HTTPS only. The chart defaults the Ingress backend to the **metrics** port (9100). For Kafka (TCP 9092), use the Kafka LoadBalancer above or NodePort.

Enable and configure Ingress:

```yaml
ingress:
  enabled: true
  className: nginx
  hosts:
    - host: tansu.example.com
      paths:
        - path: /
          pathType: Prefix
          port: metrics
  tls:
    - secretName: tansu-tls
      hosts:
        - tansu.example.com
```

## Values reference

| Key | Default | Description |
|-----|---------|-------------|
| `replicaCount` | `1` | Number of replicas when autoscaling is disabled |
| `autoscaling.enabled` | `false` | Enable HorizontalPodAutoscaler |
| `autoscaling.minReplicas` | `1` | Minimum replicas under HPA |
| `autoscaling.maxReplicas` | `10` | Maximum replicas under HPA |
| `autoscaling.targetCPUUtilizationPercentage` | `70` | Target CPU % for HPA |
| `storageEngine.type` | `memory` | One of: `memory`, `s3`, `postgres`, `sqlite` |
| `image.repository` | `ghcr.io/tansu-io/tansu` | Container image |
| `image.tag` | (chart appVersion) | Image tag |
| `clusterId` | `tansu` | Kafka cluster ID |
| `advertisedListener` | `tansu:9092` | Advertised listener URL host:port |
| `service.kafkaPort` | `9092` | Kafka port |
| `service.metricsPort` | `9100` | Metrics port |
| `kafkaLoadBalancer.enabled` | `false` | Create a LoadBalancer/NodePort service for Kafka TCP |
| `kafkaLoadBalancer.serviceType` | `LoadBalancer` | `LoadBalancer` (cloud) or `NodePort` (local k8s) |
| `kafkaLoadBalancer.nodePort` | `null` | For NodePort: fixed port (e.g. 30092); omit for auto |
| `kafkaLoadBalancer.port` | `9092` | Service port |
| `kafkaLoadBalancer.annotations` | `{}` | Annotations (e.g. AWS NLB type) |
| `ingress.enabled` | `false` | Enable Ingress (HTTP; for metrics) |
| `ingress.className` | `""` | IngressClass name (e.g. nginx) |
| `ingress.hosts` | see values | Hosts and paths (default: metrics port) |
| `ingress.tls` | `[]` | TLS secret and hosts |

See [values.yaml](values.yaml) for all options.
