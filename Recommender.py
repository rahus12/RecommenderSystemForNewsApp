import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import faiss


class Recommender:
    def __init__(self):
        self.docs = None
        self.model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")
        self.embeddings = None 
        self.index = None

    def read_docs(self, path):
        self.docs = pd.read_csv(path, index_col=0)
        self.docs = self.docs.drop(columns="sentiment", axis=1)
        self.docs = self.docs.dropna()
        
    
    def generate_embeddings(self):
        self.embeddings = self.model.encode(self.docs[["title","content", "category"]].values, show_progress_bar=True)
    
    def create_vectorDb(self):
        self.index = faiss.IndexFlatL2(self.embeddings.shape[1])
        self.index.add(self.embeddings)
        faiss.write_index(self.index, "news_index.index", 3)
    
    def load_vectorDb(self):
        self.index = faiss.read_index("news_index.index")
        
    def SimilarArticles(self,article, index, top_k = 5):
        # print(top_k)
        article_embed = self.model.encode(article)
        dist, idxs = index.search(article_embed, top_k)
        
        similar_articles = [self.docs.iloc[idx]['id'] for idx in range(len(idxs[0]))]
        return similar_articles

    def get_similar_articles(self, article: dict, top_k: int = 5) -> list[dict]:
        """
        Find similar articles based on input article
        
        Args:
            article (dict): Article with title, content and category
            top_k (int): Number of similar articles to return
            
        Returns:
            list: List of similar article IDs with similarity scores
        """
        try:
            # Combine article fields similar to training data
            article_text = f"{article['title']} {article['content']} {article['category']}"
            
            # Generate embedding for input article
            article_embed = self.model.encode([article_text])
            
            # Find similar articles
            distances, idxs = self.index.search(article_embed, top_k)
            
            # Prepare results
            similar_articles = [self.docs.iloc[idx]['id'] for idx in idxs[0]]
            
            return similar_articles
            
        except Exception as e:
            print(f"Error finding similar articles: {str(e)}")
            raise

if __name__ == "__main__":
    recommender = Recommender()
    recommender.read_docs("articles.csv")
    recommender.generate_embeddings()
    recommender.create_vectorDb()

