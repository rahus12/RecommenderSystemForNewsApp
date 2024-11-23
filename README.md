# This is the Analytics part of the News App
Steps to run:

1. From the backup folder run "FetchAllFiles" to get the csv output of all the articles, name it prefereablly as "articles.csv"
2. Run the FlaskServer.py from the main/root folder. This will read the article.csv file and create a vector db if one doesnt exist
   Then it will be ready to serve recommendations. Keep this running
3. Run the analyticsFetch.py file: This will fetch latest analytics documents from the firestore database. If it finds new articles it will write them to a Sqlite Database and then does some pre-processing
   Finally it will call send articles to your flaskServer and get recommendations
   Once it gets the recommendations, it will write these back to the FireStore database
