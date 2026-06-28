---
title: "Guide: Grafana data source"
slug: docs/guides/grafana
sidebar:
  order: 2
---

#### Setup sample

Grafana data source is available in our [repo](https://github.com/ochi-team/ochi-grafana).

Folow the readme for local installation setup or look at the [docker-compose](https://github.com/ochi-team/ochi-grafana/blob/main/docker-compose.yml) sample for quick start.

## Grafana configuration with Docker Compose provisioning

The guide demos how to register Ochi Grafana plugin.

It requires to configure:
- `docker-compose.yml` that starts Grafana, allows the unsigned local plugin, and mounts the plugin plus provisioning files into the container
- `provisioning/datasources/ochi.yaml` tells Grafana to create the Ochi datasource on startup.

The required `docker-compose.yml` parts are:

```yaml
services:
  grafana:
    image: grafana/grafana:11.1.4
    ports:
      - '3000:3000'
    environment:
      GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS: ochi-logs-datasource
    volumes:
      - ./dist:/var/lib/grafana/plugins/ochi-logs-datasource
      - ./provisioning:/etc/grafana/provisioning
```

`GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS` matches the plugin id from `src/plugin.json`:

```text
ochi-logs-datasource
```

The `./dist` mount must contain the built plugin assets. 

To build it:

```bash
npm run build
```

The datasource provisioning file is `provisioning/datasources/ochi.yaml`:

```yaml
apiVersion: 1

datasources:
  - name: Ochi Logs
    uid: ochi-logs
    type: ochi-logs-datasource
    access: proxy
    url: http://host.docker.internal:9014
    isDefault: false
    editable: true
```

- `type` must be `ochi-logs-datasource`, same as GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS.
- `uid` is the stable datasource id used by dashboards.

On mac if you run it on the same host docker exposes host machine on `host.docker.internal` as we did in the provisioning above.

For Linux hosts, add this to the Grafana service if you want to use `host.docker.internal`:

```yaml
extra_hosts:
  - 'host.docker.internal:host-gateway'
```

### Supplying values with environment variables

Grafana provisioning files can read environment variables.

Example `docker-compose.yml`:

```yaml
services:
  grafana:
    image: grafana/grafana:11.1.4
    ports:
      - '${GRAFANA_PORT:-3000}:3000'
    environment:
      GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS: ochi-logs-datasource
      OCHI_URL: ${OCHI_URL:-http://host.docker.internal:9014}
      OCHI_TENANT_ID: ${OCHI_TENANT_ID:-}
    volumes:
      - ./dist:/var/lib/grafana/plugins/ochi-logs-datasource
      - ./provisioning:/etc/grafana/provisioning
```

Example `provisioning/datasources/ochi.yaml`:

```yaml
apiVersion: 1

datasources:
  - name: Ochi Logs
    uid: ochi-logs
    type: ochi-logs-datasource
    access: proxy
    url: ${OCHI_URL}
    jsonData:
      tenantId: ${OCHI_TENANT_ID}
    isDefault: false
    editable: true
```

Read Grafana [provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) for more.

### Tenancy

`jsonData.tenantId` is optional flag. When it is set, the datasource sends it as the `X-Scope-OrgID` header on health checks and queries.

Read [docs](../../reference/api/) for more info on multi-tenancy.

## Grafana Ochi Quick Start

Below is a docker compose sample to run Grafana and Ochi.

Grafana also requires datasource provisioning file described above.

We recommend running Ochi on the host machine instead of a container.

:::caution
  It's up to your judgement configure Grafana security (authentication, networking boundaries) and confirm Ochi persistence volumes.
:::

```yaml
services:
  grafana:
    image: grafana/grafana:11.1.4
    environment:
      GF_SECURITY_DISABLE_INITIAL_ADMIN_CREATION: true
      GF_AUTH_ANONYMOUS_ENABLED: true
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_AUTH_DISABLE_SIGNOUT_MENU: true
      GF_AUTH_DISABLE_LOGIN_FORM: true
    volumes:
      - ./dist:/var/lib/grafana/plugins/ochi-logs-datasource
      - ./provisioning:/etc/grafana/provisioning
      # optional mount if you want to monitor Ochi instance itelf with cadvisor
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports:
      - "3000:3000"

  ochi:
    build:
      context: ..
      dockerfile: Dockerfile
      args:
        OPTIMIZE: ReleaseSafe
        RELEASE: true
    command: ["/usr/local/bin/ochi"]
    cgroup_parent: ochi.slice
    volumes:
        - ochi:/app/.ochi
    ports:
      - "9014:9014"

  # below are optional services to monitor Ochi
  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.55.1
    # cAdvisor needs host-level visibility to report metrics for all containers
    privileged: true
    pid: host
    ports:
      - "8080:8080"
    volumes:
      # host root filesystem context for filesystem/path resolution
      - /:/rootfs:ro
      # runtime sockets for container runtime discovery and metadata
      - /var/run/docker.sock:/var/run/docker.sock:ro
      # host kernel and cgroup stats source: cpu, memory, blkio (block device io), network
      - /sys:/sys:ro
      - /sys/fs/cgroup:/sys/fs/cgroup:ro
      # docker storage metadata for layer/filesystem usage insights
      - /var/lib/docker:/var/lib/docker:ro
      # host containerd socket used by docker
      - /run/containerd/containerd.sock:/run/containerd/containerd.sock:ro
    devices:
      - /dev/kmsg:/dev/kmsg
    command:
      - --docker_only=true
      - --docker=unix:///var/run/docker.sock
      - --containerd=/run/containerd/containerd.sock
      - --containerd-namespace=moby
      - --housekeeping_interval=15s
      - --store_container_labels=true
      - --disable_root_cgroup_stats=true
      - --disable_metrics=advtcp,cpuset,cpu_topology,disk,diskIO,hugetlb,network,perf_event,percpu,pressure,referenced_memory,resctrl,sched,tcp,udp

  pyroscope:
    image: 'grafana/pyroscope:latest'
    ports:
      - '4040:4040'

  alloy:
    image: 'grafana/alloy:latest'
    privileged: true
    pid: host
    volumes:
      - ./alloy.config:/etc/alloy/config.alloy
      - /sys/kernel/tracing:/sys/kernel/tracing:ro
    command: run /etc/alloy/config.alloy
    extra_hosts:
      - "host.docker.internal:host-gateway"
    depends_on:
      - pyroscope

  prometheus:
    image: prom/prometheus:v3.4.2
    command:
      - --config.file=/etc/prometheus/prometheus.yml
      - --storage.tsdb.path=/prometheus
      # enables reload API
      - --web.enable-lifecycle
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    depends_on:
      - cadvisor
```
