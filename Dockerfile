FROM python:3.11-bullseye

WORKDIR /app


# Install Chromium, Chromium driver, and python3-distutils
RUN apt-get update && \
    apt-get install -y \
    chromium \
    chromium-driver \
    python3-distutils \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    libgbm1 \
    libxshmfence1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxtst6 \
    ca-certificates \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/chromium-browser/:$PATH"

# Copy only requirements first for better caching
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Now copy the rest of the code
COPY . ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "run_discogs_gui.py"]
