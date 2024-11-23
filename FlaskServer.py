from flask import Flask, request, jsonify
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import faiss
import os
from typing import List, Dict
import logging
from Recommender import Recommender

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

recommender = Recommender()


def initialize():
    """Initialize the recommender system before first request"""
    try:
        # Check if index exists, if not create it
        if not os.path.exists("news_index.index"):
            logger.info("Creating new vector database...")
            recommender.read_docs("articles.csv")  # Replace with your data path
            recommender.generate_embeddings()
            recommender.create_vectorDb()
        else:
            logger.info("Loading existing vector database...")
            recommender.read_docs("articles.csv")  # Replace with your data path
            recommender.load_vectorDb()
    except Exception as e:
        logger.error(f"Error initializing recommender: {str(e)}")
        raise

@app.route('/recommend', methods=['POST'])
def get_recommendations():
    """Endpoint to get article recommendations"""
    try:
        data = request.get_json()
        
        # Validate input
        required_fields = ['title', 'content', 'category']
        if not any(field in data for field in required_fields):
            return jsonify({
                'error': 'Missing required fields. Please provide title, content, and category'
            }), 400
            
        # Get number of recommendations (optional parameter)
        top_k = data.get('top_k', 5)
        
        # Get recommendations
        similar_articles = recommender.get_similar_articles(data, top_k)
        
        return jsonify({
            'recommendations': similar_articles
        })
        
    except Exception as e:
        logger.error(f"Error processing recommendation request: {str(e)}")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    initialize()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)