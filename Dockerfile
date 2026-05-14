FROM ubuntu:24.04 AS build


RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils ca-certificates git && rm -rf /var/lib/apt/lists/*
ARG ZIG_VERSION=0.16.0
# x86_64 or aarch64
ARG TARGETARCH=aarch64
RUN --mount=type=cache,target=/tmp/zig-cache \
    zig_dir="/usr/local/zig-${TARGETARCH}-linux-${ZIG_VERSION}" && \
    zig_tar="/tmp/zig-cache/zig-${TARGETARCH}.tar.xz" && \
    if [ ! -f "${zig_tar}" ]; then \
      curl -fsSL -o "${zig_tar}" "https://ziglang.org/download/${ZIG_VERSION}/zig-${TARGETARCH}-linux-${ZIG_VERSION}.tar.xz"; \
    fi && \
    tar -xJf "${zig_tar}" -C /usr/local && \
    ln -sf "${zig_dir}/zig" /usr/local/bin/zig

WORKDIR /app

COPY build.zig build.zig.zon test_runner.zig ./
COPY src/ src/

RUN --mount=type=cache,target=/app/.zig-cache /usr/local/bin/zig build -Doptimize=ReleaseSafe

# TODO: use lighter image (e.g. scratch, distroless, etc.)
FROM ubuntu:24.04 AS runtime

WORKDIR /app

COPY --from=build /app/zig-out/bin/Ochi /usr/local/bin/ochi

EXPOSE 9014

CMD ["/usr/local/bin/ochi"]
