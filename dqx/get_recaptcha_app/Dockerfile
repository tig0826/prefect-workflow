
# FROM python:3.11-slim
# # 必要なパッケージをインストール
# RUN apt-get update && apt-get install -y \
#     chromium \
#     chromium-driver \
#     wget \
#     gnupg \
#     fonts-liberation \
#     libnss3 \
#     libasound2 \
#     libatk1.0-0 \
#     libx11-xcb1 \
#     libxcomposite1 \
#     libxrandr2 \
#     libxi6 \
#     libxcb-dri3-0 \
#     libdrm2 \
#     libgbm1 \
#     libgtk-3-0 \
#     libxdamage1 \
#     && rm -rf /var/lib/apt/lists/*

# # Pythonライブラリをインストール
# RUN pip install --no-cache-dir flask selenium

# # 作業ディレクトリを設定
# WORKDIR /app

# # アプリケーションファイルをコピー
# COPY get_recaptcha_app.py /app/

# # Flask アプリケーションを実行
# CMD ["python", "get_recaptcha_app.py"]




# Base docker image
FROM debian:sid-slim

# Install Chrome
RUN apt-get update && apt-get install -y \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  hicolor-icon-theme \
  libcanberra-gtk* \
  libgl1-mesa-dri \
  libgl1-mesa-glx \
  libpango1.0-0 \
  libpulse0 \
  libv4l-0 \
  fonts-symbola \
  --no-install-recommends \
  && curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google.list \
  && apt-get update && apt-get install -y \
  google-chrome-stable \
  --no-install-recommends \
  && apt-get purge --auto-remove -y curl \
  && rm -rf /var/lib/apt/lists/*

# Download the google-talkplugin
RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/* \
  && curl -sSL "https://dl.google.com/linux/direct/google-talkplugin_current_amd64.deb" -o /tmp/google-talkplugin-amd64.deb \
  && dpkg -i /tmp/google-talkplugin-amd64.deb \
  && rm -rf /tmp/*.deb \
  && apt-get purge -y --auto-remove curl

# Add chrome user
RUN groupadd -r chrome && useradd -r -g chrome -G audio,video chrome \
    && mkdir -p /home/chrome/Downloads && chown -R chrome:chrome /home/chrome

COPY local.conf /etc/fonts/local.conf

# Run Chrome as non privileged user
USER chrome

# # Autorun chrome
# ENTRYPOINT [ "google-chrome" ]
# CMD [ "--user-data-dir=/data" ]

# Pythonライブラリをインストール
RUN pip install --no-cache-dir flask selenium

# 作業ディレクトリを設定
WORKDIR /app

# アプリケーションファイルをコピー
COPY get_recaptcha_app.py /app/

# Flask アプリケーションを実行
CMD ["python", "get_recaptcha_app.py"]


