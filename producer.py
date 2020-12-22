import sys
from kafka import KafkaProducer
import tweepy


class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        text = ""
        if hasattr(status, "extended_tweet"):
            text = status.extended_tweet["full_text"]
        elif not hasattr(status,"retweets_status"):
            text = status.text
        if len(text)>0:
            print(text)
            kafka_producer = connect_kafka_producer()
            publish_message(kafka_producer, 'mytweets', text)

    def on_error(self, status_code):
        print("Encountered streaming error with code ", status_code, "\n")
        sys.exit()


def publish_message(producer_instance, topic_name, value):
    key_bytes = bytes('foo', encoding='utf-8')
    value_bytes = bytes(value, encoding='utf-8')
    producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
    producer_instance.flush()


def connect_kafka_producer():
    _producer = None
    _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10), linger_ms=10)
    return _producer


def connectToTwitterAPI(params,hashtags):
    if(len(params) > 0):
        API_KEY = params[0]
        API_SECRET_KEY = params[1]
        ACCESS_TOKEN = params[2]
        ACCESS_TOKEN_SECRET = params[3]

        auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth)

        streamListener = StreamListener()
        stream = tweepy.Stream(auth=api.auth, listener=streamListener, tweet_mode='extended')
        stream.filter(track=hashtags,languages = ['en'])
    else:
        print("Incorrect parameters.\n")
        getInputs()


if __name__ == "__main__":
    API_KEY = "o9623imTrnVpvvp3pVnKfAWBd"
    API_SECRET_KEY = "0gRWIIKAUUC7IuwpiH0bNiL6dLtdLO3jUFdNUeC6Zd6xBx3wQw"
    ACCESS_TOKEN = "314596040-7BSgxNYmP5Bv4uYaHqapiePLAncjdw2z1cclU0sQ"
    ACCESS_TOKEN_SECRET = "ApVbDPeWiM17mEay6sL4xeoxG8FgQcCSyx9dphSWDIBgM"
    params = [API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET]
    hashtags = ["#trump","#coronavirus"]
    connectToTwitterAPI(params,hashtags)
