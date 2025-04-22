import threading
import json
import utillogger
from kafka import KafkaConsumer
import time

logger = utillogger.setup(__name__)

def safe_json_deserializer(m):
    try:
        return json.loads(m.decode("utf-8"))
    except Exception as e:
        logger.warning(f"Broker not ready - retrying in 3s...: {e}")
        return None

def consume_topics(topics: list[str],
                   group_id : str = "news-price-group",
                   bootstrap_servers:list[str] = ["localhost:9092", "localhost:9093", "localhost:9094"], 
                ) -> None:
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers = bootstrap_servers,
        group_id = group_id,
        value_deserializer = safe_json_deserializer,
        key_deserializer = lambda m: m.decode("utf-8") if m else None,
        enable_auto_commit = True,
    )
    logger.info(f"Started listening to: {topics}")
    for attempt in range(3):
        try:
            for msg in consumer:
                if msg.value is None:
                    continue
                logger.info(f"This is the result: {msg.topic} | {msg.key}: {msg.value}")
        except Exception as e:
            logger.warning(f"Broker not ready — retrying in 3s...: {e}")
            time.sleep(3)
            continue

def test_main():
    economy_topics = ["news.economy", "price.economy"]
    tech_topics = ["news.technology","price.technology"]

    economy_thread = threading.Thread(
        target = consume_topics, args = (economy_topics, "news-price-group"), name = "EconomyConsumer"
    )
    tech_thread = threading.Thread(
        target = consume_topics, args = (tech_topics, "news-price-group"), name = "TechConsumer"
    )
    economy_thread.start()
    tech_thread.start()
    economy_thread.join()
    tech_thread.join()

if __name__== "__main__":
    test_main()