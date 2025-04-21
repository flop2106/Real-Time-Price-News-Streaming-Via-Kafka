import json
from kafka import KafkaProducer
import utillogger

logger = utillogger.setup(__name__)

def setupProducer(list_server: list[str] = ["localhost:9093"],
                  encoder: str = "utf-8") -> KafkaProducer:
    return (
    KafkaProducer(
    bootstrap_servers = list_server,
    key_serializer = lambda k: k.encode(encoder),
    value_serializer= lambda v: json.dumps(v).encode(encoder),
    api_version =(3,4)
    ))

def publish(producer: KafkaProducer, topic: str, key: str, payload: dict)-> None:
    try:
        future = producer.send(topic, key = key, value = payload)
        record_metadata = future.get(timeout = 10)
        logger.info(
        f"Published to {record_metadata.topic}"
        f"[partition {record_metadata.partition} @ offset {record_metadata.offset}]"            
        )
    except Exception as e:
        logger.exception(f"Failed To Publish to {topic}: {e}")
    

if __name__ == "__main__":
    setup = setupProducer()


    # example messages
    
    list_update = [("news.economy",   "economy",{"headline": "Q1 GDP beats forecasts", "ts": 1713657600}),
    ("price.economy",  "economy",{"symbol": "US500",          "price": 4.21,            "ts": 1713657600}),
    ("news.technology",      "tech",   {"headline": "New AI chip unveiled",  "ts": 1713657600}),
    ("price.technology",     "tech",   {"symbol": "NASDAQ",            "price": 650.34,          "ts": 1713657600})
    ]
    for args in list_update:
        publish(setup, *args)

    setup.flush()
    setup.close()