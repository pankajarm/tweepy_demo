# Import the ususal supsects
import pandas as pd
import json
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Imports the Google Cloud client library
from google.cloud import language

# Import firebase admin
import firebase_admin
from firebase_admin import credentials

cred = credentials.Certificate("./keyfile.json")

#Variables that contains the user credentials to access Twitter API
access_token = "736415964175142912-NE1KtKJM0I0gNBUpz1a6kgbD1J7i072"
access_token_secret = "L30sEYBK1a4qr44UrhIwH8d78R6gwepDXAheVr6irUUKa"
consumer_key = "OSgkiY847Hj52aR2iMmIbhFyL"
consumer_secret = "rN2hjF7bbDZ1JYAdCr3LVAyvPhzQflDWvG6ezR9RQMvz5UKfIq"

# variable that contain Google NL API Key
cloud_api_key = "AIzaSyCtExzo8spyfnaSmh8e9UueG2CP8tLzNZw"
# variable that contain Firebase project id
project_id = "twitter-streaming-154fa"
# variale that contain Big Query DataSet and Table name
bigquery_dataset = "twitter_handles"
bigquery_table ="ai_dl_ml"



# let's make tweets data
tweets_data = []

# Instantiates a google cloud client
language_client = language.Client()

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        # print type(data)
        filtered_tweet = self.apply_filter(data)
        try:
            if filtered_tweet != None:
                self.call_NL_API(filtered_tweet)
        except Exception as e:
            print ("Error:", e)
        return True

    def on_error(self, status):
        print status

    def apply_filter(self, data):
        # make unicode stream data tweet into json
        tweet = json.loads(data)
        if (tweet['lang'] == 'en'):
            #  ((event.text != undefined) && (event.text.substring(0,2) != 'RT')
            if (tweet['text'] != None and tweet['text'][:2] != 'RT'):
                return tweet
        pass


    def call_NL_API(self, tweet):
        print tweet
        response, body = self.call_NL_API_Auth(cloud_api_key)
        if response == '200':
            # send the data to Firebase
            self.writeDataToFirebase(tweet, body)
            # send the data to bigquery
            self.writeDataToBigQuery(tweet, body)


    def call_NL_API_Auth(self, cloud_api_key):
        # The text to analyze
        text = 'Hello, world!'
        document = language_client.document_from_text(text)
        # Detects the sentiment of the text
        sentiment = document.analyze_sentiment().sentiment
        return response

    def mname(self, arg):
        pass







# Class StdOutListener Ends

if __name__ == '__main__':

    # iniitlilize firebase admin
    firebase_admin.initialize_app(cred)
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords:
    # 'ai,artificial intelligence,deep learning,deepmind,machine learning
    stream.filter(track=['ai,artificial intelligence,deep learning,deepmind,machine learning'])
