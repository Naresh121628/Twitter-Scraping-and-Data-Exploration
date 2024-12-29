import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import pymongo
import streamlit as st
from datetime import datetime
import time
from typing import List, Dict, Any
import logging
import random

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwitterScraper:
    def __init__(self, mongo_uri: str = "mongodb+srv://Naresh:Aswini1216@cluster1.my0b6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1"):
        """Initialize the Twitter scraper with MongoDB connection"""
        self.nitter_instances = [
            "https://nitter.net",
            "https://nitter.1d4.us",
            "https://nitter.kavin.rocks",
            "https://nitter.unixfox.eu",
            "https://nitter.domain.glass",
        ]
        try:
            self.client = pymongo.MongoClient(mongo_uri)
            self.db = self.client["TwitterScraper"]
            self.collection = self.db["ScrapedData"]
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def get_working_instance(self) -> str:
        """Try to find a working Nitter instance"""
        random.shuffle(self.nitter_instances)
        for instance in self.nitter_instances:
            try:
                response = requests.get(f"{instance}/", timeout=5)
                if response.status_code == 200:
                    return instance
            except:
                continue
        raise Exception("No working Nitter instance found")

    def scrape_tweets(self, keyword: str, start_date: str, end_date: str, tweet_limit: int) -> List[Dict[str, Any]]:
        """Scrape tweets using Nitter instance"""
        scraped_tweets = []
        max_retries = 3
        retry_delay = 5  # seconds
        
        instance = self.get_working_instance()
        search_url = f"{instance}/search"
        
        logger.info(f"Using Nitter instance: {instance}")
        
        for attempt in range(max_retries):
            try:
                params = {
                    'q': f"{keyword} since:{start_date} until:{end_date}",
                    'f': 'tweets'
                }
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                
                tweets_count = 0
                next_page = None
                
                while tweets_count < tweet_limit:
                    if next_page:
                        params['cursor'] = next_page
                    
                    response = requests.get(search_url, params=params, headers=headers)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find all tweet containers
                    tweet_items = soup.find_all('div', class_='timeline-item')
                    
                    if not tweet_items:
                        break
                    
                    for tweet in tweet_items:
                        if tweets_count >= tweet_limit:
                            break
                            
                        try:
                            tweet_data = {
                                "date": tweet.find('span', class_='tweet-date').a['title'],
                                "id": tweet.get('data-tweet-id', ''),
                                "content": tweet.find('div', class_='tweet-content').get_text(strip=True),
                                "user": tweet.find('a', class_='username').get_text(strip=True),
                                "reply_count": tweet.find('span', class_='tweet-stat').get_text(strip=True) if tweet.find('span', class_='tweet-stat') else '0',
                                "retweet_count": tweet.find_all('span', class_='tweet-stat')[1].get_text(strip=True) if len(tweet.find_all('span', class_='tweet-stat')) > 1 else '0',
                                "like_count": tweet.find_all('span', class_='tweet-stat')[2].get_text(strip=True) if len(tweet.find_all('span', class_='tweet-stat')) > 2 else '0',
                            }
                            
                            scraped_tweets.append(tweet_data)
                            tweets_count += 1
                            
                            # Show progress in Streamlit
                            if tweets_count % 10 == 0:
                                st.text(f"Scraped {tweets_count} tweets...")
                                
                        except Exception as e:
                            logger.error(f"Error parsing tweet: {str(e)}")
                            continue
                    
                    # Look for next page cursor
                    next_link = soup.find('div', class_='show-more').find('a') if soup.find('div', class_='show-more') else None
                    if not next_link:
                        break
                    next_page = next_link.get('href', '').split('cursor=')[-1]
                    
                    # Add delay between requests
                    time.sleep(random.uniform(1, 3))
                
                logger.info(f"Successfully scraped {len(scraped_tweets)} tweets")
                return scraped_tweets
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    st.error(f"Failed to scrape tweets after {max_retries} attempts. Error: {str(e)}")
                    return []

    def save_to_mongodb(self, keyword: str, scraped_data: List[Dict[str, Any]]) -> bool:
        """Save scraped data to MongoDB with error handling"""
        try:
            document = {
                "Scraped_Word": keyword,
                "Scraped_Date": datetime.now().strftime("%Y-%m-%d"),
                "Scraped_Data": scraped_data,
            }
            self.collection.insert_one(document)
            logger.info("Successfully saved data to MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to save to MongoDB: {str(e)}")
            return False

def main():
    st.set_page_config(page_title="Twitter Data Scraper", layout="wide")
    
    st.title("Twitter Data Scraper")
    st.markdown("""
    This application allows you to scrape Twitter data based on keywords and date ranges.
    Please note that the scraping process might take some time depending on the number of tweets requested.
    """)

    # Initialize scraper
    try:
        scraper = TwitterScraper()
    except Exception as e:
        st.error(f"Failed to initialize the scraper: {str(e)}")
        return

    # User Inputs in a form
    with st.form("scraping_form"):
        col1, col2 = st.columns(2)
        with col1:
            keyword = st.text_input("Enter Keyword or Hashtag to Search:")
            tweet_limit = st.number_input("Number of Tweets to Scrape:", 
                                        min_value=1, 
                                        max_value=1000,  # Reduced limit for Nitter
                                        value=100,
                                        step=1)
        with col2:
            start_date = st.date_input("Start Date:")
            end_date = st.date_input("End Date:")
        
        submit_button = st.form_submit_button("Scrape Tweets")

    if submit_button:
        if not keyword:
            st.error("Please enter a keyword to search.")
            return
            
        if start_date >= end_date:
            st.error("End date must be after start date.")
            return

        with st.spinner("Scraping tweets... This may take a few minutes."):
            tweets = scraper.scrape_tweets(
                keyword, 
                start_date.strftime("%Y-%m-%d"), 
                end_date.strftime("%Y-%m-%d"), 
                tweet_limit
            )

        if tweets:
            st.success(f"Successfully scraped {len(tweets)} tweets!")
            
            # Display data
            df = pd.DataFrame(tweets)
            st.dataframe(df)

            # Save and download options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("Save to Database"):
                    if scraper.save_to_mongodb(keyword, tweets):
                        st.success("Data saved to MongoDB!")
                    else:
                        st.error("Failed to save to database.")
            
            with col2:
                csv = df.to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    "tweets.csv",
                    "text/csv",
                    use_container_width=True
                )
            
            with col3:
                json_data = json.dumps(tweets, default=str)
                st.download_button(
                    "Download JSON",
                    json_data,
                    "tweets.json",
                    "application/json",
                    use_container_width=True
                )

if __name__ == "__main__":
    main()


