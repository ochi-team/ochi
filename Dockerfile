FROM ubuntu:24.04 AS build

ARG ZIG_VERSION=0.15.1

RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils ca-certificates && rm -rf /var/lib/apt/lists/*
RUN --mount=type=cache,target=/tmp/zig-cache \
    if [ ! -f /tmp/zig-cache/zig.tar.xz ]; then \
      curl -L -o /tmp/zig-cache/zig.tar.xz "https://ziglang.org/download/${ZIG_VERSION}/zig-aarch64-linux-${ZIG_VERSION}.tar.xz"; \
    fi && tar -xJf /tmp/zig-cache/zig.tar.xz -C /usr/local
RUN ln -s /usr/local/zig-aarch64-linux-${ZIG_VERSION}/zig /usr/local/bin/zig

WORKDIR /app

COPY build.zig build.zig.zon test_runner.zig ./
COPY src/ src/

RUN --mount=type=cache,target=/app/.zig-cache zig build test
