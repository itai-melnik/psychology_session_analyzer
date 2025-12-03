FROM python:3.9-slim

WORKDIR /app

# NEW: Install FFmpeg (Required for moviepy)
RUN apt-get update && apt-get install -y ffmpeg

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# No "uvicorn" here because this is just a script that runs forever
CMD ["python", "main.py"]