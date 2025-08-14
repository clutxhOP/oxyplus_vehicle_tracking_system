FROM python:3.11

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV OPENBLAS_NUM_THREADS=4
ENV OMP_NUM_THREADS=4
ENV MKL_NUM_THREADS=4

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget unzip gnupg ca-certificates fonts-liberation \
    libglib2.0-0 libnss3 libxss1 libasound2 \
    xdg-utils libxml2-dev libxslt1-dev python3-dev \
    chromium chromium-driver psmisc procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install psutil

RUN ln -sf /usr/share/zoneinfo/Asia/Dubai /etc/localtime && \
    echo "Asia/Dubai" > /etc/timezone

COPY . .

RUN mkdir -p alerts data logs \
    && chmod +x master.py start_system.sh \
    && chmod 755 /app

EXPOSE 5231

HEALTHCHECK --interval=5m --timeout=30s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:5231/health || exit 1

CMD ["./start_system.sh"]