import json, os, sys

from kafka import KafkaProducer

from services.producers.stock.src.stock_producer import StockProducer

companies = ["MSFT", "AAPL", "NVDA"]

if __name__ == '__main__':
    print("Stock producer...")

    if os.environ.get("KAFKA_ADDRESS") == "" or os.environ.get("KAFKA_PORT"):
        print("Invalid Kafka address or port")
        sys.exit(1)
    if os.environ.get("POLL_INTERVAL") == "":
        print("Invalid poll interval, default to 75...")
        poll_interval = 75  # in seconds
    else:
        poll_interval = int(os.environ.get("POLL_INTERVAL"))

    bootstrap_servers = os.environ.get("KAFKA_ADDRESS") + ":" + os.environ.get("KAFKA_PORT")

    k = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer = StockProducer(poll_interval, companies, k)
    producer.run()


