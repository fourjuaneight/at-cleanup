FROM node:25.9.0-alpine AS frontend

WORKDIR /app/dashboard

RUN npm install -g pnpm@10.28.2

COPY dashboard/package.json dashboard/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

COPY dashboard/ ./
RUN pnpm build


FROM golang:1.26.2-alpine AS builder

ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

WORKDIR /app

COPY go.mod go.sum ./
COPY telemetry/ ./telemetry/
COPY version/ ./version/

RUN go mod download

COPY pkg/ ./pkg/
COPY cmd/search/ ./cmd/search/
COPY dashboard/embed.go ./dashboard/embed.go
COPY --from=frontend /app/dashboard/dist ./dashboard/dist

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
    -ldflags "-X github.com/fourjuaneight/at-cleanup/version.GitCommit=${GIT_COMMIT} -X github.com/fourjuaneight/at-cleanup/version.BuildTime=${BUILD_TIME}" \
    -o search ./cmd/search


FROM alpine:3.21

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/search .

CMD ["./search"]
