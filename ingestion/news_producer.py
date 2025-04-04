# news_producer.py

import requests
import json
import time
from kafka import KafkaProducer

# Replace with your actual API key
NEWSAPI_KEY = "d61650377f4d4125902cca1fde687e0e"

# Kafka config
KAFKA_TOPIC = "tech-news"
KAFKA_SERVER = "localhost:9092"

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_tech_news():
    url = (
        f"https://newsapi.org/v2/top-headlines?"
        f"category=technology&language=en&pageSize=5&apiKey={NEWSAPI_KEY}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        articles = response.json().get("articles", [])
        return articles
    else:
        print("Error fetching news:", response.status_code, response.text)
        return []

def send_news_to_kafka(articles):
    for article in articles:
        message = {
            "title": article.get("title"),
            "description": article.get("description"),
            "publishedAt": article.get("publishedAt"),
            "url": article.get("url"),
            "source": article.get("source", {}).get("name")
        }
        producer.send(KAFKA_TOPIC, value=message)
        print("✅ Sent:", message["title"])

if __name__ == "__main__":
    while True:
        print("\n🔄 Fetching latest tech news...")
        news_articles = fetch_tech_news()
        if news_articles:
            send_news_to_kafka(news_articles)
        else:
            print("No new articles.")
        time.sleep(300)  # wait 5 minutes before next fetch
