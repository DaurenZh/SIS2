# The public prebuilt MinIO images are unavailable. Build the pinned official
# source release instead of relying on an unrelated third-party image.
FROM golang:1.24.8-bookworm AS build
ENV GOBIN=/out
RUN go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z

FROM debian:bookworm-slim
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
