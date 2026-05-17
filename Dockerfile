FROM ubuntu:24.04 AS build


RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils ca-certificates git && rm -rf /var/lib/apt/lists/*
ARG ZIG_VERSION=0.16.0
# x86_64 or aarch64
ARG TARGETARCH=aarch64
ARG OPTIMIZE=ReleaseSafe
ARG TRACY=false
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
RUN --mount=type=cache,target=/root/.cache/zig \
    --mount=type=cache,target=/app/.zig-cache \
    /usr/local/bin/zig build --fetch

COPY src/ src/

RUN --mount=type=cache,target=/root/.cache/zig \
    --mount=type=cache,target=/app/.zig-cache \
    /usr/local/bin/zig build -Doptimize=${OPTIMIZE} -Dtracy=${TRACY}

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=build /app/zig-out/bin/Ochi /usr/local/bin/ochi

EXPOSE 9014

CMD ["/usr/local/bin/ochi"]
