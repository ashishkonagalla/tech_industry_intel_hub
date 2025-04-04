# nlp_consumer.py

from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from hashlib import sha256
import json
import re
import spacy

# Configs
SOURCE_TOPIC = "tech-news"
TARGET_TOPIC = "enriched-tech-news"
KAFKA_SERVER = "localhost:9092"

# Simple dictionaries for enrichment
COMPANIES = ["Apple", "Google", "NVIDIA", "Microsoft", "Intel", "Amazon", "Meta"]
TOPICS = {
    "AI chip": ["chip", "gpu", "accelerator"],
    "Layoffs": ["layoff", "cut", "fired", "job loss"],
    "Acquisition": ["acquire", "buyout", "merge", "merger"],
    "Regulation": ["lawsuit", "ban", "antitrust", "fine"]
}
RISK_KEYWORDS = ["lawsuit", "hacked", "cut", "delay", "fined", "violation", "disruption"]

# Kafka setup
consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

nlp = spacy.load("en_core_web_sm")
def extract_companies(text):
    doc = nlp(text)
    orgs = set(ent.text for ent in doc.ents if ent.label_ == "ORG")
    matched = [company for company in COMPANIES if company.lower() in text.lower()]
    return list(orgs.union(matched))


def extract_topic(text):
    for topic, keywords in TOPICS.items():
        if any(word in text.lower() for word in keywords):
            return topic
    return "General"

analyzer = SentimentIntensityAnalyzer()
def calculate_sentiment(text):
    scores = analyzer.polarity_scores(text)
    compound = scores['compound']
    if compound >= 0.05:
        return "Positive"
    elif compound <= -0.05:
        return "Negative"
    else:
        return "Neutral"

def risk_score(text):
    score = 0
    text_lower = text.lower()

    for keyword in RISK_KEYWORDS:
        if keyword in text_lower:
            score += 2  # weight each match

    # Contextual boost
    if "lawsuit" in text_lower or "fined" in text_lower:
        score += 3
    elif "hacked" in text_lower or "disruption" in text_lower:
        score += 2

    return min(score, 10)  # cap at 10

print("🔄 Starting NLP Consumer...")

for message in consumer:
    article = message.value
    combined_text = f"{article.get('title', '')} {article.get('description', '')}"
    event_id = sha256((article.get('title', '') + article.get('publishedAt', '')).encode()).hexdigest()

    enriched = {
        "event_id": event_id,
        "timestamp": article.get("publishedAt"),
        "headline": article.get("title"),
        "url": article.get("url"),
        "source": article.get("source"),
        "company": extract_companies(combined_text),
        "topic": extract_topic(combined_text),
        "sentiment": calculate_sentiment(combined_text),
        "risk_score": risk_score(combined_text)
    }

    producer.send(TARGET_TOPIC, enriched)
    print("✅ Enriched and sent:", enriched["headline"])
