## Get started

Install [xk6]( https://github.com/grafana/xk6 )

Build k6 with Loki extension:
```sh
xk6 build --with github.com/grafana/xk6-loki@latest
```

Launch a debuggee from `monitoring/`, e.g.
```sh
docker compose -f monitoring/docker-compose.yml up --build
```

Run the load:
```sh
./k6 run load_loki.js
```
