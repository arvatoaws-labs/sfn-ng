FROM rust as builder

WORKDIR /usr/src/myapp
COPY . .
RUN cargo install --path .

FROM busybox
COPY --from=builder /usr/local/cargo/bin/sfn-ng /usr/bin/sfn-ng

CMD ["sfn-ng"]
