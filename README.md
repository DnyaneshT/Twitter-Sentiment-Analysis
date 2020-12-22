# Twitter-Sentiment-Analysis
Twitter Sentiment Analysis using Apache Spark Streaming
![Framework](Output/Framework.png)

**Scrapper**<br>
Used a scrapper in python using Tweepy to scrape the tweets based on certain keywords

**Kafka**<br>
Used Kafka to store the tweets according to topics

**Spark Streaming**<br>
I created a Kafka consumer and periodically collect filtered tweets from scrapper. For each hash tag, performed sentiment analysis using Sentiment Analyzer

**Sentiment Analyzer**<br>
Used Vader Sentiment

**Elasticsearch**<br>
Store the tweets and their sentiment information for further visualization purpose

**Kibana**<br>
Used kibabana, a visualization tool to show the tweets' sentiment classification result in a real-time manner
