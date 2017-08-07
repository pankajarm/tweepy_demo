# Import the ususal supsects
import pandas as pd
import json
import requests

# Import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# Imports the Google Cloud client libraries
from google.cloud import language, bigquery
from google.cloud.bigquery import SchemaField
from bigquery import get_client, schema_from_record

# Import firebase admin
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db


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

# Settings for Firebase
cred = credentials.Certificate("./keyfile.json")
app = firebase_admin.initialize_app(cred, {
    'databaseURL': "https://" + project_id + ".firebaseio.com"
})
# print ("firebase app name", app.name)
ref = db.reference('restricted_access/secret_document')
# print(ref.get())
tweetRef = ref.child('latest')
hashtagRef = ref.child('hashtags')

# Settings for bigquery client libaray
# using http://tylertreat.github.io/BigQuery-Python/pages/client.html#bigqueryclient-class
# client = get_client(json_key_file="./keyfile.json", readonly=True)
# print ("i got client:", client)

# # Submit an async query.
# job_id, _results = client.query('SELECT name FROM [bigquery-public-data:usa_names.usa_1910_current] where number > 9000')
# # Check if the query has finished running.
# complete, row_count = client.check_job(job_id)
# # Retrieve the results.
# results = client.get_query_rows(job_id)
# print ("here are your resutls:", results)

# schema = [
#     {'type': 'FLOAT', 'name': 'score', 'mode': 'NULLABLE'},
#     {'type': 'STRING', 'name': 'user', 'mode': 'NULLABLE'},
#     {'type': 'STRING', 'name': 'text', 'mode': 'NULLABLE'},
#     {'type': 'FLOAT', 'name': 'magnitude', 'mode': 'NULLABLE'},
#     {'type': 'STRING', 'name': 'id', 'mode': 'NULLABLE'},
#     {'type': 'STRING', 'name': 'user_time_zone', 'mode': 'NULLABLE'},
#     {'type': 'FLOAT', 'name': 'user_followers_count', 'mode': 'NULLABLE'},
#     {'type': 'RECORD', 'name': 'hashtags', 'mode': 'REPEATED'},
#     {'type': 'RECORD', 'name': 'entities', 'mode': 'REPEATED'},
#     {'type': 'RECORD', 'name': 'tokens', 'mode': 'REPEATED'}
# ]

# schema = [
#     {'name': 'foo', 'type': 'STRING', 'mode': 'nullable'},
#     {'name': 'bar', 'type': 'FLOAT', 'mode': 'nullable'}
# ]
#
# # now create a table
# created = client.create_table(dataset=bigquery_dataset, table="sample_table", schema=schema)
# print ("created:", created)
# if created:
#     print ("yeah, table created")
# else:
#     print ("OOPS no table right now")

# # Check if a table exists.
# exists = client.check_table(bigquery_dataset, table="sample_table")
# if exists:
#     print (bigquery_dataset, "Exists")
#     print (bigquery_table, "Exists")
# # Get a table's full metadata. Includes numRows, numBytes, etc.
# metadata = client.get_table(bigquery_dataset, table="sample_table")
# print ("metadata:", metadata)

# Instantiates a client
# bq_client = bigquery.Client(project=project_id)
# # get dataset and table
# bq_dataset = bq_client.dataset(bigquery_dataset)
# bq_table = bq_dataset.table(name="sample_table")
# bq_table.schema = [
# SchemaField('id','STRING'),
# SchemaField('text','STRING'),
# SchemaField('user','STRING'),
# SchemaField('user_time_zone','STRING'),
# SchemaField('user_followers_count','INTEGER'),
# SchemaField('hashtags','STRING'),
# SchemaField('tokens','STRING'),
# SchemaField('score','STRING'),
# SchemaField('magnitude','STRING'),
# SchemaField('entities','STRING')
# ]
# bq_SCHEMA = bq_table.schema
# print('Dataset Name:{} \T Table Name:{}'.format(bq_dataset.name, bq_table.name))
# print ("bq_table SCHEMA", bq_SCHEMA)


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
        # print tweet
        response, body = self.call_NL_API_Auth(tweet)
        if response == 200 and len(body["sentences"]) != 0:
            # send the data to Firebase
            # self.writeDataToFirebase(tweet, body)
            # send the data to bigquery
            self.writeDataToBigQuery(tweet, body)
        else:
            print ("Error getting response from annotateText REST API")

    def call_NL_API_Auth(self, tweet):
        # the request call to NL language API
        # using https://cloud.google.com/natural-language/docs/reference/rest/v1beta2/documents/annotateText
        # annotateText REST API
        textUrl = "https://language.googleapis.com/v1beta2/documents:annotateText?key="+cloud_api_key

        # The text to analyze
        text = "Hello, world, I am happy to announce we are officially live"
        requestBody = {
                        "document":
                        {
                            "type": "PLAIN_TEXT",
                            "content": tweet['text']
                        },
                        "features":
                        {
                          "extractSyntax": "true",
                          "extractEntities": "true",
                          "extractDocumentSentiment": "true",
                          "extractEntitySentiment": "true",
                        },
                        "encodingType": "UTF8",
                    }

        # now make the final request.post call with options as data
        # print textUrl
        # print json.dumps(requestBody)
        # Google NL annotateText REST API v1beta2 accepts JSON-Encoded POST/PATCH data
        req = requests.post(textUrl, data=json.dumps(requestBody))

        if req.status_code == 200:
            # print json.loads(req.text)
            return req.status_code, json.loads(req.text)

        print "Error!!! connecting NL API status code:", req.status_code
        pass

    def writeDataToFirebase(self, tweet, body):
        # print tweet
        # print body
        tweetDataForFirebase = {
                                'id': tweet["id_str"],
                                'text': tweet["text"],
                                'user': tweet["user"]["screen_name"],
                                'user_time_zone': tweet["user"]["time_zone"],
                                'user_followers_count': tweet["user"]["followers_count"],
                                'hashtags': tweet["entities"]["hashtags"],
                                'tokens': body["tokens"],
                                'score': body["documentSentiment"]["score"],
                                'magnitude': body["documentSentiment"]["magnitude"],
                                'entities': body["entities"]
        }
        # print tweetDataForFirebase
        # let's save this data to firebase
        tweetRef.push(tweetDataForFirebase)
        print ("Firebase saved the tweet with NL aPI results")
        pass

    def writeDataToBigQuery(self, tweet, body):
        tweetDataForBigQuery = {
                                'id': tweet["id_str"],
                                'text': tweet["text"],
                                'user': tweet["user"]["screen_name"],
                                'user_time_zone': tweet["user"]["time_zone"],
                                'user_followers_count': tweet["user"]["followers_count"],
                                'hashtags': str(tweet["entities"]["hashtags"]),
                                'tokens': str(body["tokens"]),
                                'score': body["documentSentiment"]["score"],
                                'magnitude': body["documentSentiment"]["magnitude"],
                                'entities': str(body["entities"])
        }
        # print (json.dumps(tweetDataForBigQuery))
        # print (schema_from_record(tweetDataForBigQuery))
        # let's save this data to bigquery
        self.stream_data(dataset_name=bigquery_dataset, table_name=bigquery_table, json_data= tweetDataForBigQuery)
        # inserted = client.push_rows(dataset=bigquery_dataset, table=bigquery_table, rows=tweetDataForBigQuery, template_suffix=)
        # bq_table.insert_data((tweetDataForBigQuery))
        # if inserted:
        #     print ("bigquery saved the tweet with NL aPI results:", inserted)
        # else:
        #     print ("Error inseting rows in bigquery:", inserted)
        pass

    def stream_data(self, dataset_name, table_name, json_data):
        bigquery_client = bigquery.Client()
        dataset = bigquery_client.dataset(dataset_name)
        table = dataset.table(table_name)
        data = json.loads(json_data)

        # Reload the table to get the schema.
        table.reload()

        rows = [data]
        errors = table.insert_data(rows)

        if not errors:
            print('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
        else:
            print('Errors:')
            print(errors)



# Class StdOutListener Ends

if __name__ == '__main__':

    # iniitlilize firebase admin
    # firebase_admin.initialize_app(cred)

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords:
    # 'ai,artificial intelligence,deep learning,deepmind,machine learning
    stream.filter(track=['ai,artificial intelligence,deep learning,deepmind,machine learning'])
