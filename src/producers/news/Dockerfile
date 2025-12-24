FROM python:3-slim
LABEL authors="Mattia Lunardi"
COPY requirements.txt ./
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /requirements.txt

RUN mkdir -p /opt/bigstocksbignews/
WORKDIR /opt/bigstocksbignews/

COPY src/ .

CMD ["python3", "./main.py"]