## Log Generation Simulator

## Prerequisites

You have to first install [xk6](https://grafana.com/docs/loki/latest/send-data/k6/). You can do that with brew:
```bash
brew install xk6
```
Then generate the binary of k6 with the loki extension:
```bash
xk6 build --with github.com/grafana/xk6-loki@latest
```

## Debugging with Loki

### Step 1: Build Loki with debug symbols
```bash
git clone https://github.com/grafana/loki
cd loki
make loki-debug
```

### Step 2: Run Loki Under Delve in Headless Mode

```bash
dlv exec ./cmd/loki/loki-debug --listen=:40000 --headless=true --api-version=2 --accept-multiclient --continue -- -config.file=./cmd/loki/loki-local-config.yaml
```

### Step 3: Connect Your IDE
TODO: Add instructions for connecting your IDE (e.g., Neovim) to the
Delve debugger.


### Step 4: Run tests with k6
```
./k6 run generate.js
```

## Debugging with Victoria Metrics
