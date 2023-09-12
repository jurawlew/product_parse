FROM python:3.10

WORKDIR ./product_point
COPY . .

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip && pip install --no-cache-dir --upgrade -r requirements.txt
RUN apt update
RUN apt install -y wget
RUN mkdir --parents /usr/local/share/ca-certificates/Yandex/ && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
    --output-document /usr/local/share/ca-certificates/Yandex/YandexCA.crt && \
    chmod 655 /usr/local/share/ca-certificates/Yandex/YandexCA.crt \
