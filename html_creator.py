import kafka_consumer
import utillogger
import threading
import time

logger = utillogger.setup(__name__)
def create_html(topics:list[str], key:str):
    header = f"""
    <!DOCTYPE html>
    <html>
    <body>
        <h1> {topics} </h1>  
    """
    news_html = ""
    price_html = ""
    consumer = kafka_consumer.consume_topics(topics)
    for attempt in range(3):
        try:
            for msg in consumer:
                if msg.value is None:
                    continue
                logger.info(f"This is the result: {msg.topic} | {msg.key}: {msg.value}")
                if "news" in msg.topic:
                    news_html = news_html + f'''
                    <p></p>
                    <h3>{msg.value['headline']}</h3>
                    <a href="{msg.value['url']}">{msg.value['url']}</a>
                    <p>{str(msg.value['content'])}</p>
                    '''
                else:
                    price_html = price_html + f'''
                    <p>{msg.value['symbol']} {msg.value['ts']} {msg.value['price']}</p>
                    '''
                html_footing = "</body></html>"
                with open(f"{key}.html","w",encoding="utf-8" ) as index:
                    try:
                        index.write(header + 
                                    "<h3>Price List</h3>" +
                                    price_html +
                                    "<h3>News List</h3>" + 
                                    news_html +
                                    html_footing)
                    except:
                        index.write(header)
        except Exception as e:
            logger.warning(f"Broker not ready — retrying in 3s...: {e}")
            time.sleep(3)
            continue
       
   
    

economy_thread = threading.Thread(
    target = create_html, args = (["news.economy", "price.economy"],
                           "economy"
                            )
)
tech_thread = threading.Thread(
    target = create_html, args = (["news.technology","price.technology"],
                           "technology"
                            )
)

economy_thread.start()
tech_thread.start()

economy_thread.join()
tech_thread.join()

