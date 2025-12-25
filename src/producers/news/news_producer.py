from datetime import datetime, timezone
from newsapi import NewsApiClient
from confluent_kafka import Producer
import time, json

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s", (str(msg), str(err)))

class NewsProducer:
    def __init__(self, poll_interval: int, news_client: NewsApiClient, kafka_client: Producer, sources: str):
        self.poll_interval = poll_interval
        self.news_client = news_client
        self.sources = sources
        self.kafka_client = kafka_client
        self.last_published_at = None

    def run(self):
        print(f"News polling service started at: {datetime.now(timezone.utc).isoformat()}, poll interval {self.poll_interval}")
        while True:
            print(f"Fetching top headlines for these ids: {self.sources}")
            articles = self.news_client.get_top_headlines(sources=self.sources)["articles"]
            for article in articles:
                published_at = article["publishedAt"]
                if self.last_published_at and published_at <= self.last_published_at:
                    continue
                res = {
                    "event_type": "news_article",
                    "source": article["source"]["name"],
                    "source_id": article["source"]["id"],
                    "title": article["title"],
                    "description": article["description"],
                    "url": article["url"],
                    "published_at": article["publishedAt"],
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                self.kafka_client.produce(topic="news_raw", key=article["source"]["name"], value=json.dumps(res), callback=acked)
                self.kafka_client.poll(0)

            self.kafka_client.flush()
            self.last_published_at = max(a["publishedAt"] for a in articles)
            print(f"Sleeping for {self.poll_interval}")
            time.sleep(self.poll_interval)
