import sys
import json
from pyspark import SparkContext, SparkConf
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
from textblob import TextBlob

esConnection = Elasticsearch()


def getHashTag(text):
    if "trump" in text.lower():
        return "#trump"
    elif "corona" in text.lower():
        return "#corona"


def getSentimentValue(text):
    sentimentAnalyser = SentimentIntensityAnalyzer()
    polarity = sentimentAnalyser.polarity_scores(text)
    if(polarity["compound"] > 0):
        return "positive"
    elif(polarity["compound"] < 0):
        return "negative"
    else:
        return "neutral"


def getSentiment(time, rdd):
    data = rdd.collect()
    for temp in data:
        # add text and sentiment info to elasticsearch
        esConnection.index(index="hash_tags_sentiment_analysis",doc_type="tweet-sentiment-analysis", body=temp)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Incorrect parameters: sentimentAnalysis.py <broker_list> <topic>")
        exit(-1)
    spark = SparkSession.builder.appName("SentimentAnalysis").config("spark.eventLog.enabled", "false").getOrCreate()
    sparkContext = spark.sparkContext
    streamingContext = StreamingContext(sparkContext,20)

    brokers, topic = sys.argv[1:]
    directStream = KafkaUtils.createDirectStream(streamingContext, [topic], {"metadata.broker.list": brokers})

    encodedTweets = directStream.map(lambda x: str(x[1].encode("ascii", "ignore")))
    tweets = encodedTweets.map(lambda x: (x, getSentimentValue(x), getHashTag(x))).map(lambda x: {"message": x[0], "sentiment": x[1], "hashTag": x[2]})

    tweets.foreachRDD(getSentiment)
    
    streamingContext.start()
    streamingContext.awaitTermination()
