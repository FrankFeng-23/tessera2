FROM rust:1.70

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy source code
COPY . .

# Build the binary
RUN cargo build --release --bin landsat_stack

# The output binary will be at target/release/landsat_stack