FROM ubuntu:24.04 AS build

RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils ca-certificates git && rm -rf /var/lib/apt/lists/*
ARG ZIG_VERSION=0.16.0
ARG TARGETARCH

RUN case "$TARGETARCH" in \
      amd64)  echo "ZIG_ARCH=x86_64"  > /tmp/zig-arch.env ;; \
      arm64)  echo "ZIG_ARCH=aarch64" > /tmp/zig-arch.env ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac

RUN --mount=type=cache,id=ochi-zig-tarballs,target=/tmp/zig-cache \
    . /tmp/zig-arch.env && \
    zig_dir="/usr/local/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}" && \
    zig_tar="/tmp/zig-cache/zig-${ZIG_ARCH}.tar.xz" && \
    if [ ! -f "${zig_tar}" ]; then \
      curl -fsSL -o "${zig_tar}" "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz"; \
    fi && \
    tar -xJf "${zig_tar}" -C /usr/local && \
    ln -sf "${zig_dir}/zig" /usr/local/bin/zig

WORKDIR /app

COPY build.zig build.zig.zon ./
RUN --mount=type=cache,id=ochi-zig-global,target=/root/.cache/zig \
    --mount=type=cache,id=ochi-zig-local,target=/app/.zig-cache \
    /usr/local/bin/zig build --fetch

COPY src/ src/
ARG OPTIMIZE=ReleaseSafe
ARG TRACY=false
RUN --mount=type=cache,id=ochi-zig-global,target=/root/.cache/zig \
    --mount=type=cache,id=ochi-zig-local,target=/app/.zig-cache \
    /usr/local/bin/zig build -Doptimize=${OPTIMIZE} -Dtracy=${TRACY}

# FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
FROM ubuntu:24.04 AS runtime

WORKDIR /app

COPY --from=build /app/zig-out/bin/Ochi /usr/local/bin/ochi

EXPOSE 9014

CMD ["/usr/local/bin/ochi"]
