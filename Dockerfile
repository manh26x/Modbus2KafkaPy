FROM python:3.10-slim-buster

WORKDIR /app

COPY requirements.txt requirements.txt
COPY CauHinhKafka.json CauHinhKafka.json
COPY CauHinhModbus.json CauHinhModbus.json
COPY CauHinhThongSoPaChien.xlsx CauHinhThongSoPaChien.xlsx
COPY main.py main.py

RUN pip3 install -r requirements.txt

CMD [ "python3", "main.py"]