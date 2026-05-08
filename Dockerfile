FROM python:3.12-slim

WORKDIR /app

# Install system deps (optional, but often useful for guessit/ffmpeg-related stuff)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbd-strm.py .

ENV PYTHONUNBUFFERED=1

# tini as PID 1 to handle signals cleanly (optional but recommended)
ENTRYPOINT ["/usr/bin/tini", "--", "python", "dbd-strm.py"]
# Default target directory inside container; override when running if needed
CMD ["/data"]