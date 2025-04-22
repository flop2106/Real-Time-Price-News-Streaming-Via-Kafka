# File: com/project1/trading_automation/model/news.py

import news_api_handler
import threading
import utillogger
import pandas as pd
import kafka_producer
producer = kafka_producer.setupProducer()
logger = utillogger.setup(__name__)

class News:
    def __init__(self, title: str, url: str, content: str):
        self.title = title
        self.url = url
        self.content = content

    def summarize(self) -> str:
        """
        Returns a summary of the content using the helper function from the news_api_handler.
        """
        return news_api_handler.summarize_news(self.content)

    @classmethod
    def fetch_news(cls, query: str, max_results: int):
        """
        Retrieves raw news data via the NewsAPIHandler and returns a list of News instances.
        
        Args:
            query (str): The search keyword.
            max_results (int): Maximum number of articles to retrieve.
        
        Returns:
            list[News]: A list of News objects created from the API response.
        """
        raw_news_list = news_api_handler.get_news(query, max_results)
        news_title = []
        news_content = []
        news_url = []
        for raw_news in raw_news_list:
            title = raw_news.get("title", "")
            url = raw_news.get("url", "")
            content = raw_news.get("content", "No Content Available")
            news_title.append(title), news_content.append(content), news_url.append(url)
        
        return pd.DataFrame({"Title":news_title,
                             "url": news_url,
                             "news_content": news_content})

    def __str__(self):
        return f"Title: {self.title}\nURL: {self.url}\nContent: {self.summarize()}\n"

def main(query: str, no_of_result: int, topic: str, key:str):
    result_df = News.fetch_news(query, no_of_result)
    for i in range(len(result_df)):
        payload = {"headline":result_df["Title"].tolist()[i],
                   "url": result_df["url"].tolist()[i],
                   "content": result_df["news_content"].tolist()[i],}
        kafka_producer.publish(producer, topic, key, payload )

economy_thread = threading.Thread(
    target = main, args = ("US economy",
                           10,
                           "news.economy", 
                           "economy"
                            )
)
tech_thread = threading.Thread(
    target = main, args = ("Technology",
                           10,
                           "news.technology", 
                           "technology"
                            )
)

economy_thread.start()
tech_thread.start()

economy_thread.join()
tech_thread.join()