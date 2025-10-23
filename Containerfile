FROM --platform=linux/amd64 docker.io/library/debian:sid-slim
RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates && apt-get clean
COPY tdx2db /
RUN chmod +x /tdx2db
ENTRYPOINT ["/tdx2db"]
