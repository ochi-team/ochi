# Monitoring setup

This folder contains a local Prometheus stack to observe Ochi metrics and container resource usage.

- `prometheus.yml` scrapes:
  - Ochi app metrics from `http://ochi:9014/metrics`
  - cAdvisor container metrics from `http://cadvisor:8080/metrics`
  - Prometheus self-metrics
- `docker-compose.yml` starts:
  - `ochi` (your app)
  - `prometheus` on `localhost:9090`
  - `cadvisor` on `localhost:8080`
  - `grafana` on `localhost:3000` with dashboards auto-provisioned

## Included dashboards

Grafana provisions `Ochi Monitoring Overview` at startup from:

- `monitoring/grafana/dashboards/ochi-overview.json`

It includes panels for all PromQL presets in this README:

- Request rate by path/status
- Ingress bytes rate by path/status
- Ochi container CPU usage
- Ochi container memory usage

## Run

From repository root:

```bash
docker compose -f monitoring/docker-compose.yml up --build
```

Then open:

- Prometheus UI: `http://localhost:9090`
- cAdvisor UI: `http://localhost:8080`
- Grafana UI: `http://localhost:3000` (login: `admin` / `admin`)

## Useful PromQL examples

If the provided dashboards are not enough the following promql might be used:

App-level request rate (by path/status):

```promql
sum by (path, status) (rate(dispatch_requests_total[1m]))
```

App-level ingress bytes rate:

```promql
sum by (path, status) (rate(dispatch_request_body_bytes_total[1m]))
```

Container CPU usage for Ochi (cores):

```promql
sum(rate(container_cpu_usage_seconds_total{name=~".*ochi.*"}[1m]))
```

Container memory usage for Ochi (bytes):

```promql
sum(container_memory_working_set_bytes{name=~".*ochi.*"})
```

Container memory usage (MiB):

```promql
sum(container_memory_working_set_bytes{name=~".*ochi.*"}) / 1024 / 1024
```
