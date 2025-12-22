FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y curl unzip ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp

ARG AW_VERSION="v0.13.2"
RUN curl -L -o aw.zip https://github.com/ActivityWatch/activitywatch/releases/download/${AW_VERSION}/activitywatch-${AW_VERSION}-linux-x86_64.zip \
    && unzip aw.zip \
    && rm aw.zip

RUN mv activitywatch/aw-server-rust/aw-server-rust /usr/local/bin/aw-server \
    && chmod +x /usr/local/bin/aw-server

# Web UIの移動
# RUN mv activitywatch/aw-webui /usr/share/aw-webui

RUN rm -rf activitywatch

ENV AW_HOST="0.0.0.0"
ENV AW_PORT="5600"
EXPOSE 5600

# 実行
CMD ["/usr/local/bin/aw-server", "--host", "0.0.0.0", "--port", "5600", "--webpath", "/usr/share/aw-webui"]
