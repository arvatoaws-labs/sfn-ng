FROM rust as builder

# aws-lc-sys (pulled in by the AWS SDK via rustls) compiles a native C
# library and requires cmake, which is not present in the base rust image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/myapp
COPY . .
RUN cargo install --path .

FROM busybox
COPY --from=builder /usr/local/cargo/bin/sfn-ng /usr/bin/sfn-ng

CMD ["sfn-ng"]
