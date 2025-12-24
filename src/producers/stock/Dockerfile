FROM python:3-alpine
LABEL authors="Mattia Lunardi"
COPY requirements.txt ./
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /requirements.txt

RUN mkdir -p /opt/bigstocksbignews/
WORKDIR /opt/bigstocksbignews/

CMD ["python3", "./main.py"]