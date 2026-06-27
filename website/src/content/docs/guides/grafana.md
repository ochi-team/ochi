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

