import json, os, sys
from confluent_kafka import Producer
from services.producers.news.src.news_producer import NewsProducer
from newsapi import NewsApiClient

sources_ids = "bloomberg,business-insider,fortune,the-wall-street-journal,il-sole-24-ore"

if __name__ == '__main__':
    print("News producer")

    if not os.environ.get("NEWS_API_KEY"):
        print("Missing news api key")
        sys.exit(1)
    if not os.environ.get("KAFKA_ADDRESS") or not os.environ.get("KAFKA_PORT"):
        print("Invalid Kafka address or port")
        sys.exit(1)
    if not os.environ.get("POLL_INTERVAL"):
        print("Invalid poll interval, default to 300...")
        poll_interval = 300  # in seconds
    else:
        poll_interval = int(os.environ.get("POLL_INTERVAL"))
    bootstrap_servers = os.environ.get("KAFKA_ADDRESS") + ":" + os.environ.get("KAFKA_PORT")

    news_client = NewsApiClient(api_key=os.environ.get("NEWS_API_KEY"))

    producer = NewsProducer(
        poll_interval=poll_interval,
        news_client=news_client,
        kafka_client=Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "NewsProducer"
        }),
        sources=sources_ids,
    )
    producer.run()
