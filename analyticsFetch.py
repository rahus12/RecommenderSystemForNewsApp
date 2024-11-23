import firebase_admin
from firebase_admin import credentials, firestore
import time
from datetime import datetime, timedelta
import pytz
import sqlite3
import pandas as pd
import requests


class AnalyticsFetcher:
    def __init__(self):
        self.db = self.initialize_firestore()
        self.last_fetch_time = None
        self.conn = self.initialize_database()
        self.data = pd.read_csv("articles.csv")
        self.url = "http://127.0.0.1:5000/recommend"
        self.headers = {"Content-Type": "application/json"}
        self.cred = credentials.Certificate("ServiceAccountKey.json")
        # firebase_admin.initialize_app(self.cred)
        self.firestore = firestore.client()
        self.doc_ref = self.firestore.collection('recommendations')

    def initialize_database(self):
        """Initialize SQLite database and create table if it doesn't exist"""
        conn = sqlite3.connect('article_analytics.db')
        cursor = conn.cursor()
        
        cursor.execute('DROP TABLE IF EXISTS article_analytics')

        # Create table if it doesn't exist - note the new normalized_time_spent field
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS article_analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                userId TEXT NOT NULL,
                articleId TEXT NOT NULL,
                category TEXT,
                timeSpent INTEGER NOT NULL,
                normalized_time_spent FLOAT NOT NULL,
                content_length INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sentiment TEXT                       
            )
        ''')
        
        conn.commit()
        return conn

    def calculate_normalized_time(self,time_spent: int, content: str) -> float:
        """Calculate normalized time spent reading an article"""
        content_length = len(content.strip().split())
        if content_length == 0:  # Avoid division by zero
            return 0.0
        return time_spent / content_length

    def save_to_database(self, analytics_articles, articles):
        """Save the combined article analytics data to SQLite database with normalized time"""
        cursor = self.conn.cursor()
        
        # Create dictionaries to easily look up article data by ID
        article_lookup = {article['id']: article for article in articles}
        
        # Prepare data for batch insert
        records = []
        for analytics in analytics_articles:
            article_id = analytics['articleId']
            article = article_lookup.get(article_id)
            
            if article and 'content' in article:
                time_spent = analytics['timeSpent']
                content = article['content']
                normalized_time = self.calculate_normalized_time(time_spent, content)
                content_length = len(content.strip().split())
                
                record = (
                    analytics['userId'],
                    article_id,
                    article.get('category', 'unknown'),
                    time_spent,
                    normalized_time,
                    content_length,
                    article.get('sentiment', 'unknown')
                )
                records.append(record)
            else:
                print(f"Warning: Skipping record for article {article_id} - missing content")
        
        # Batch insert records
        cursor.executemany('''
            INSERT INTO article_analytics (
                userId, 
                articleId, 
                category, 
                timeSpent, 
                normalized_time_spent,
                content_length,
                sentiment
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', records)
        
        self.conn.commit()
        return len(records)

    def initialize_firestore(self):
        """Initialize Firestore client"""
        cred = credentials.Certificate('ServiceAccountKey.json')
        firebase_admin.initialize_app(cred)
        return firestore.client()

    def fetch_article(self, analytics_articles) -> list[dict]:
        """Fetch articles using the article_id from the analytics articles from Firestore"""
        article_ref = self.db.collection('articles')
        articles = []

        for analytics_article in analytics_articles:
            article_doc = article_ref.document(analytics_article.get('articleId')).get()
            if article_doc.exists:
                article_data = article_doc.to_dict()
                article_data['id'] = article_doc.id
                articles.append(article_data)
            else:
                print(f"article with id {analytics_article.get('articleId')} not found")

        return articles

    def fetch_new_articles(self) -> list[dict]:
        """Fetch analytics articles newer than last fetch time and save to database"""
        try:
            analytics_ref = self.db.collection('article_analytics')
            
            # Query for documents newer than last fetch
            new_articles_query = analytics_ref.where(
                'timestamp', '>', self.last_fetch_time
            ).order_by('timestamp', direction=firestore.Query.DESCENDING)
            
            new_analytics_articles = []
            for doc in new_articles_query.stream():            
                _data = doc.to_dict()
                _data['id'] = doc.id
                new_analytics_articles.append(_data)

            if new_analytics_articles:
                print(f"{datetime.now()}: Found {len(new_analytics_articles)} new articles")
                article_list = self.fetch_article(new_analytics_articles)
                
                # Save the combined data to SQLite database
                records_saved = self.save_to_database(new_analytics_articles, article_list)
                print(f"Saved {records_saved} records to database")
                
                # Print some stats about the normalized times
                if records_saved > 0:
                    cursor = self.conn.cursor()
                    cursor.execute('''
                        SELECT AVG(normalized_time_spent), MIN(normalized_time_spent), 
                            MAX(normalized_time_spent)
                        FROM article_analytics
                        WHERE created_at >= datetime('now', '-1 minute')
                    ''')
                    avg, min_time, max_time = cursor.fetchone()
                    print(f"Recent normalized time stats - Avg: {avg:.4f}, Min: {min_time:.4f}, Max: {max_time:.4f}")
                
                return article_list
            return []
        except Exception as e:
            print(f"Error fetching articles: {str(e)}")
            return []    
    
    def get_top_articleIds_by_user(self, user_id: str, limit: int = 5) -> list[dict]:
        """Get top articles by user"""
        cursor = self.conn.cursor()
        cursor.execute(f'''
            select articleId from article_analytics
            where userId = "{user_id}"
            ORDER by normalized_time_spent DESC
            limit {limit} 
        ''', (user_id, limit))
        data = cursor.fetchall()
        articleIds = [d[0] for d in data]
        return articleIds
    
    def get_user_ids(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT userId FROM article_analytics;")
        users = cursor.fetchall()
        userIds = [user[0] for user in users]
        return userIds
    
    def get_recommendations(self) -> None:
        userIds = self.get_user_ids()

        for userId in userIds:
            response = []
            try:
                articleIds = self.get_top_articleIds_by_user(userId)
                articles = self.data[self.data['id'].isin(articleIds)]
                for index in range(len(articles)):
                    payload = {
                        "title": articles.get('title', " ").iloc[index],
                        "content": articles.get('content', " ").iloc[index], 
                    }
                    # send this to the Flask server running the ML code
                    response.append(requests.post(self.url, json=payload, headers=self.headers).json()) # this gives a list of recommendations(dict)
                recommendations = [item for sublist in response for item in sublist.get('recommendations', "")]
                self.doc_ref.add({
                    "userId": userId,
                    "recommendations": recommendations
                })

            except Exception as e:
                print(f"Error fetching recommendations for user {userId}: {str(e)}")
                continue
    def fetch_analytics(self):
        # Initialize Firestore and SQLite database        
        
        # Set initial fetch time
        # get the analytics of last 10 mins
        self.last_fetch_time = datetime.now(pytz.UTC) - timedelta(minutes=10)
        
        # Define fetch interval (in seconds)
        FETCH_INTERVAL = 10
        
        print(f"Starting article fetcher. Will check every {FETCH_INTERVAL} seconds...")
        
        try:
            while True:
                try:
                    # Fetch new articles and save to Sqlite3 database
                    new_articles = self.fetch_new_articles()
                    
                    # Update last fetch time
                    self.last_fetch_time = datetime.now(pytz.UTC)
                    
                    # Run the recommendation part only if new analytics are found
                    # if new_articles:
                    #     self.get_recommendations()

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
            self.conn.close()



if __name__ == "__main__":
    analyticsFetcher = AnalyticsFetcher()
    analyticsFetcher.fetch_analytics()


