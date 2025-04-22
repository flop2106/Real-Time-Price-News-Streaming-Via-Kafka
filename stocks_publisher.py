from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import time
import threading
import kafka_producer
import utillogger
producer = kafka_producer.setupProducer()
logger = utillogger.setup(__name__)

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu" )
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(options=options)

def getprice(url:str, topic: str, key: str, symbol: str)-> None:
    driver.get(url)
    driver.implicitly_wait(10)
    
    try:
        logger.info("Fetching prices every 5s for 1 minute....\n")
        start_time = time.time()

        while time.time() - start_time < 60:
            actions = ActionChains(driver)
            actions.move_by_offset(1, 1).click().perform()
            price = driver.execute_script(
                 'return document.querySelector("[data-test=\'instrument-price-last\']")?.textContent.trim();'
            )
            print(f"Price For {topic} received: {price}")
            if price:
                payload = {
                    "symbol": symbol,
                    "ts": str(time.strftime('%Y-%m-%d %H:%M:%S')),
                    "price": price
                }
                kafka_producer.publish(producer, topic, key, payload)
                logger.info(f"Published to Kafka: {payload}")
            else:
                logger.info("Price element Not Found")
            time.sleep(5)
        
        print("\n Done- Stopped after 1 minute")
    
    except Exception as e:
        print(f"Error: {e}")



        
economy_thread = threading.Thread(
    target = getprice, args = ("https://www.investing.com/indices/us-spx-500-futures?",
                               "price.economy",
                               "economy",
                               "US500")
)
tech_thread = threading.Thread(
    target = getprice, args = ("https://www.investing.com/indices/nasdaq-composite",
                               "price.technology",
                               "tech",
                               "NASDAQ")
)

economy_thread.start()
tech_thread.start()

economy_thread.join()
tech_thread.join()

producer.flush()
producer.close()