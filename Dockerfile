# Dockerfile для f2b-bot на Amvera.
#
# Зачем кастомный build (раньше был нативный pip-toolchain):
# 2026-06-23 — добавили модуль dashamail_scheduler.py + cron-job
# dashamail_weekly_send_job в scheduler.py, которые используют Playwright
# для управления DashaMail web-UI. Playwright требует chromium-бинаря и
# набора системных библиотек, которых нет в стандартном python-runtime
# Amvera. Поэтому переход на Dockerfile + python:3.12-slim + apt-deps
# для chromium + playwright install chromium на этапе build.
#
# Версия Playwright в requirements.txt должна совпадать с тем, что image
# поддерживает (chromium ставится скриптом playwright install — версия
# chromium берётся из установленного python-пакета playwright).

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Системные библиотеки, нужные chromium-headless.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        fonts-liberation \
        libasound2 \
        libatk-bridge2.0-0 \
        libatk1.0-0 \
        libc6 \
        libcairo2 \
        libcups2 \
        libdbus-1-3 \
        libdrm2 \
        libexpat1 \
        libgbm1 \
        libglib2.0-0 \
        libgtk-3-0 \
        libnspr4 \
        libnss3 \
        libpango-1.0-0 \
        libx11-6 \
        libx11-xcb1 \
        libxcb1 \
        libxcomposite1 \
        libxdamage1 \
        libxext6 \
        libxfixes3 \
        libxkbcommon0 \
        libxrandr2 \
        wget \
        xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -m playwright install chromium

COPY . .

CMD ["python", "bot.py"]
