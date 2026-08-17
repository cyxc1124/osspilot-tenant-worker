# osspilot-tenant-worker
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM alpine:3.22 AS runtime

ARG GIT_TAG=""
ARG GIT_COMMIT=""
ARG GIT_BRANCH=""
ARG BUILD_TIME=""

ENV GIT_TAG=$GIT_TAG \
    GIT_COMMIT=$GIT_COMMIT \
    GIT_BRANCH=$GIT_BRANCH \
    BUILD_TIME=$BUILD_TIME

RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -H -u 10001 app

WORKDIR /app
COPY --from=builder /out/worker /app/
COPY --chmod=755 deploy/docker-entrypoint.sh /docker-entrypoint.sh

LABEL org.opencontainers.image.source=https://github.com/cyxc1124/osspilot-tenant-worker
LABEL org.opencontainers.image.description="OssPilot 租户 worker"
LABEL org.opencontainers.image.title="osspilot-tenant-worker"
LABEL org.opencontainers.image.vendor="cyxc1124"
LABEL org.opencontainers.image.version=${GIT_TAG}
LABEL org.opencontainers.image.revision=${GIT_COMMIT}

USER app
EXPOSE 8080

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["worker"]
