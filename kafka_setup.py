from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, BrokerNotAvailableError
import utillogger

logger = utillogger.setup(__name__)

def setup(listServer: list[str] = ["localhost:9092", "localhost:9093"],
          clientId: str = "admin_demo",
          topicName: list[str] = ["news.economy", "price.economy", "news.technology", "price.technology"],
          topicPartitions: list[int] = [2, 2, 2, 3],
          topicRF: list[int] = [2, 2, 2, 2]) -> None:
          
    admin = KafkaAdminClient(
    bootstrap_servers = listServer,
    client_id = clientId,
    api_version =(3,4)
    )

    topics = []
    for idx, val in enumerate(topicName):
        topics.append(NewTopic(name = val, 
                               num_partitions = topicPartitions[idx],
                               replication_factor = topicRF[idx]))

    # Create topic if not exists
    try:
        admin.create_topics(new_topics = topics, validate_only = False)
        print("Topic Created successfully")
        logger.info("Topic Created successfully")
    except TopicAlreadyExistsError as e:
        print(f"Already Exists Error: {e}")
        logger.info(f"Already Exists Error: {e}")
    except Exception as e:
        print(f"Errors in creating Topics: {e}")
        logger.info(f"Errors in creating Topics: {e}")
    finally:
        admin.close()

if __name__=="__main__":
    setup()