FROM rust:1.85 AS dependencies
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y ca-certificates libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm src/main.rs

FROM rust:1.85
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

COPY --from=dependencies /usr/src/app/target target
COPY --from=dependencies /usr/local/cargo /usr/local/cargo
COPY Cargo.toml ./
COPY src ./src
# Remove cached binary so cargo recompiles with real source
RUN rm -f target/release/adam-demo target/release/deps/adam_demo-*
RUN cargo build --release

EXPOSE 3000

RUN useradd -r -s /bin/false appuser
RUN chown -R appuser:appuser /usr/src/app
USER appuser

CMD ["./target/release/adam-demo"]
