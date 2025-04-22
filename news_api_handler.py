import os
import logging
import requests
from dotenv import load_dotenv

# Configure the logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
env_path = r"C:\Users\nurar\OneDrive\Desktop\Java Data Pipeline Platform\Java_Data_Pipeline\java-datapipeline-platform\python\News_OpenAI_Summary\.env"
load_dotenv(dotenv_path = env_path)
API_KEY = os.getenv('newsapi')
BASE_URL = "https://newsapi.org/v2/everything"

def get_news(query: str, max_results: int):
    """
    Retrieve news articles based on a search query.
    Returns a list of dictionaries, each with keys: title, url, and content.
    """
    news_list = []
    try:
        params = {
            "q": query,
            "pageSize": max_results,
            "sortBy": "publishedAt",
            "apiKey": API_KEY
        }
        # For logging purposes, build the full URL string
        request_url = BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        logger.info("Request URL: %s", request_url)
        
        response = requests.get(BASE_URL, params=params, headers={"Content-Type": "application/json"})
        #logger.info("API Response: %s", response.text)
        
        data = response.json()
        articles = data.get("articles")
        if not articles or not isinstance(articles, list):
            logger.warning("No articles found in response.")
            return news_list
        
        for article in articles:
            title = article.get("title", "")
            url = article.get("url", "")
            content = article.get("content")
            if not content:
                content = "No Content Available"
            
            news_item = {
                "title": title,
                "url": url,
                "content": content
            }
            news_list.append(news_item)
    except Exception as e:
        logger.warning("Error fetching news: %s", str(e))
    return news_list

def summarize_news(content: str) -> str:
    """
    Return a summary (first 100 characters followed by ellipsis if longer).
    """
    if content is None:
        return "No Content Available"
    return content[:100] + "..." if len(content) > 100 else content

# For testing when running this module directly:
if __name__ == "__main__":
    sample_news = get_news("bp tariff", 5)
    for article in sample_news:
        print("Title:", article["title"])
        print("Content:", summarize_news(article["content"]))
        print("URL:", article["url"])
        print("-" * 40)
