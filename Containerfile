FROM --platform=linux/amd64 docker.io/library/debian:sid-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates tzdata && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY linux/amd64/tdx2db /
RUN chmod +x /tdx2db

ENV TZ=Asia/Shanghai

ENTRYPOINT ["/tdx2db"]
