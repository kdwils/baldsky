FROM --platform=$BUILDPLATFORM golang:1.26 AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-X 'github.com/kdwils/baldsky/version.Version=${VERSION}'" -o baldsky

FROM gcr.io/distroless/static-debian12

WORKDIR /app

COPY --chown=1000:1000 --from=builder /app/baldsky /app/

USER 1000

ENTRYPOINT ["/app/baldsky"]
CMD ["serve"]