from kafka import KafkaProducer
import yfinance as yf
import json
import time

def build_kafka_event(symbol, interval, bar_timestamp, ingested_at, open, high, low, close, volume, currency):
    return {
        "event_type": "stock_price_bar",
        "source": "yahoo-finace",
        "symbol": symbol,
        "interval": interval,
        "bar_timestamp": bar_timestamp,
        "ingested_at": ingested_at,
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "currency": "USD"
    }




class StockProducer:
    def __init__(self, poll_interval: int, companies: list, kafka: KafkaProducer):
        self.poll_interval = poll_interval
        self.companies = companies
        self.kafka = kafka


    def run(self):
        while True:
            for company in self.companies:
                print(f"Pulling data for {company}")
                dat = yf.Ticker(company)
                history = dat.history(period='5m', interval='1m') # poll interval?
                # handle no history in the last 5 minutes
                latest_bar = history.iloc[-1]
                print(latest_bar)
                # extract the data we need/want into a json event
                res = {}

                # handle no kafka producer
                future = self.kafka.send(
                    topic="stock_prices_raw",
                    key=company.encode("utf-8"),
                    value=res
                )
                result = future.get(timeout=60)


                print(f"Poll complete, sleeping for {self.poll_interval} seconds...")
            time.sleep(self.poll_interval)
