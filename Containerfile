FROM --platform=linux/amd64 docker.io/library/debian:sid-slim
RUN apt-get update && apt-get install -y ca-certificates wget gzip && update-ca-certificates && apt-get clean
COPY linux/amd64/tdx2db /
COPY export_for_qlib /
RUN wget -q https://install.duckdb.org/v1.4.1/duckdb_cli-linux-amd64.gz && gzip -d duckdb_cli-linux-amd64.gz && mv duckdb_cli-linux-amd64 /bin/duckdb && ln -sf /bin/duckdb /duckdb
RUN chmod +x /tdx2db /export_for_qlib /bin/duckdb
ENTRYPOINT ["/tdx2db"]
