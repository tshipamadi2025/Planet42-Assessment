FROM python:3.9

WORKDIR /app

COPY config/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt




