# 使用 Alpine Linux 构建静态链接的二进制文件
FROM rust:1.70-alpine as builder

# 安装必要的编译工具
RUN apk add --no-cache musl-dev

# 设置工作目录
WORKDIR /app

# 拷贝 Cargo.toml
COPY Cargo.toml ./

# 创建 src 目录并拷贝源代码
RUN mkdir -p src
COPY src/main.rs ./src/main.rs

# 设置 Rust 使用 musl 目标
ENV RUSTFLAGS="-C target-feature=+crt-static"

# 编译 Release 版本（静态链接）
RUN cargo build --release --target x86_64-unknown-linux-musl

# 第二阶段：使用 scratch 或最小化镜像
FROM scratch
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/process_tile_downstream_wo_json /s2_stack
ENTRYPOINT ["/s2_stack"]

# docker build -t s2_stack .
# docker create --name temp_container s2_stack
# docker cp temp_container:/s2_stack ./s2_stack
# docker rm temp_container
