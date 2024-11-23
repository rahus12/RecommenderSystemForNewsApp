import firebase_admin
from firebase_admin import credentials, firestore
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd

class FetchAllNews:
    def __init__(self):
        self.db = self.initialize_firestore()
        self.last_fetch_time = None
        

    def initialize_firestore(self):
        """Initialize Firestore client"""
        cred = credentials.Certificate('ServiceAccountKey.json')
        firebase_admin.initialize_app(cred)
        return firestore.client()



    def fetch_all_news_articles(self) -> list[dict]:
        """Fetch analytics articles newer than last fetch time and save to database"""
        try:
            article_ref = self.db.collection('articles')
                
            new_news_articles = []
            for doc in article_ref.stream():            
                _data = doc.to_dict()
                _data['id'] = doc.id
                new_news_articles.append(_data)       
                
            return new_news_articles
            
        except Exception as e:
            print(f"Error fetching articles: {str(e)}")
            return []

    def fetch_news(self):
        # Initialize Firestore and SQLite database    
        
        # Set initial fetch time
        # self.last_fetch_time = datetime.now(pytz.UTC) - timedelta(minutes=10)
        
        # Define fetch interval (in seconds)
        FETCH_INTERVAL = 10000
        
        print(f"Starting article fetcher")
        
        
        try:
            # Fetch new articles and save to database
            new_articles = self.fetch_all_news_articles()

            # pd.DataFrame(new_articles).to_csv(f'articles {datetime.now().strftime("%d-%b-%Y (%H%M%S)")}.csv', index=True)
            pd.DataFrame(new_articles).to_csv(f'../articles.csv', index=True)
            
            # Update last fetch time
            self.last_fetch_time = datetime.now(pytz.UTC)
            
            # Sleep for the specified interval
            time.sleep(FETCH_INTERVAL)
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
            # Wait a bit before retrying
            time.sleep(10)
        finally:
            # Clean up database connection
            # sql_conn.close()
            print("done")

FetchAllNews().fetch_news()

