# syntax=docker/dockerfile:1

ARG UBUNTU_VERSION=20.04

FROM ubuntu:${UBUNTU_VERSION} AS deps

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    curl \
    build-essential \
    cmake \
    libssl-dev \
    libboost-all-dev \
    libzmq3-dev \
    libgoogle-glog-dev \
    libcurl4-openssl-dev \
    libbsd-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

ARG EVENTPP_REPO=https://github.com/wqking/eventpp.git
ARG EVENTPP_REF=master
RUN git clone --depth 1 --branch "${EVENTPP_REF}" "${EVENTPP_REPO}" /tmp/eventpp \
    && cmake -S /tmp/eventpp -B /tmp/eventpp/build -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build /tmp/eventpp/build -j"$(nproc)" \
    && cmake --install /tmp/eventpp/build \
    && rm -rf /tmp/eventpp

ARG SIMDJSON_REPO=https://github.com/simdjson/simdjson.git
ARG SIMDJSON_REF=master
RUN git clone --depth 1 --branch "${SIMDJSON_REF}" "${SIMDJSON_REPO}" /tmp/simdjson \
    && cmake -S /tmp/simdjson -B /tmp/simdjson/build \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_BUILD_TYPE=Release \
    && cmake --build /tmp/simdjson/build -j"$(nproc)" \
    && cmake --install /tmp/simdjson/build \
    && rm -rf /tmp/simdjson \
    && ldconfig

FROM deps AS base-build

WORKDIR /workspace
COPY . /workspace

ARG JOBS=4
RUN cmake -S infra-controller -B infra-controller/build \
    && cmake --build infra-controller/build -j"${JOBS}" \
    && cmake --install infra-controller/build
RUN cmake -S unit-manager -B unit-manager/build \
    && cmake --build unit-manager/build -j"${JOBS}"
RUN cmake -S node/test -B node/test/build \
    && cmake --build node/test/build -j"${JOBS}"

FROM base-build AS vllm-build

ARG JOBS=4
ARG MINITSDB_SOURCE=third_party/mini-tsdb
RUN test -f "${MINITSDB_SOURCE}/CMakeLists.txt" \
    || (echo "mini-tsdb source not found at ${MINITSDB_SOURCE}. Copy it there or set MINITSDB_SOURCE." >&2; exit 1)
RUN cmake -S "${MINITSDB_SOURCE}" -B "${MINITSDB_SOURCE}/build-linux" \
    && cmake --build "${MINITSDB_SOURCE}/build-linux" -j"${JOBS}" --target minitsdb_core
RUN cmake -S node/vllm-client -B node/vllm-client/build \
        -DMINITSDB_ROOT="/workspace/${MINITSDB_SOURCE}" \
    && cmake --build node/vllm-client/build -j"${JOBS}"

FROM ubuntu:${UBUNTU_VERSION} AS runtime-base

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    bash \
    curl \
    libcurl4 \
    libgoogle-glog0v5 \
    libzmq5 \
    libbsd0 \
    libgssapi-krb5-2 \
    libnorm1 \
    libsodium23 \
    libpgm-5.2-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/vllmroute
RUN mkdir -p \
    /opt/vllmroute/unit-manager/build \
    /opt/vllmroute/node/test/build \
    /opt/vllmroute/node/vllm-client/build \
    /opt/vllmroute/runtime \
    /tmp/llm

COPY unit-manager/master_config.json /opt/vllmroute/unit-manager/master_config.json
COPY sample /opt/vllmroute/sample
COPY docker/entrypoint.sh /usr/local/bin/vllmroute-entrypoint
RUN chmod +x /usr/local/bin/vllmroute-entrypoint

EXPOSE 10001
ENTRYPOINT ["/usr/local/bin/vllmroute-entrypoint"]

FROM runtime-base AS mock-runtime

COPY --from=base-build /workspace/unit-manager/build/unit_manager /opt/vllmroute/unit-manager/build/unit_manager
COPY --from=base-build /workspace/node/test/build/test /opt/vllmroute/node/test/build/test
CMD ["mock"]

FROM runtime-base AS runtime

COPY --from=vllm-build /workspace/unit-manager/build/unit_manager /opt/vllmroute/unit-manager/build/unit_manager
COPY --from=vllm-build /workspace/node/test/build/test /opt/vllmroute/node/test/build/test
COPY --from=vllm-build /workspace/node/vllm-client/build/vllm_client /opt/vllmroute/node/vllm-client/build/vllm_client
CMD ["all"]
