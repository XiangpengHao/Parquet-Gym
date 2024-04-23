FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    python3 curl cmake \
    ca-certificates lsb-release wget \
    gcc

# Install Arrow
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb &&\
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb &&\
    apt update &&\
    apt install -y -V libarrow-dev libparquet-dev

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY . /app

RUN uv venv && uv pip install -r requirements.txt 

CMD [".venv/bin/python", "benchmark.py"]
