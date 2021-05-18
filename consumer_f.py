import jsonpickle
import tweepy
from json import loads
from pyspark.sql import SparkSession
from pyspark import SQLContext, conf
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
import findspark
from textblob import TextBlob


findspark.init()
from pyspark.streaming.kafka import KafkaUtils

consumer_key= ""
consumer_secret= ""
access_token= ""
access_token_secret= ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)



#Define the function to reply to tweets based on sentiment analysis
def reply_tweet(tweet_text, tweet_id):

    analysis = TextBlob(tweet_text)
    # set sentiment
    if analysis.sentiment.polarity > 0:
        reply = "00Thank you for your support"
        feedback = 'positive'

    elif analysis.sentiment.polarity == 0:
        reply = "00Thank you for interest. For more information visit our website"
        feedback = 'neutral'
    else:
        reply = "00Sorry to hear that! contact us please"
        feedback = 'negative'
    print("replying...")
    try:
        api.update_status(status = reply, in_reply_to_status_id = tweet_id , auto_populate_reply_metadata=True)
    except:
        print("..")
    return(feedback)


def exrow(row):
#Neglect the key, extract the value of kafka rdd
    r_data = row[1]
    #decoding and transfering to json dict
    r_string = loads(r_data).encode('utf-8')
    r_object = jsonpickle.decode(r_string)
    #assign dict to values
    username = r_object["username"]
    tweet_id = r_object["tweet_id"]
    tweet_text = r_object["tweet_txt"]
    tweet_create = r_object["tweet_create"]
    tweet_retweets = r_object["tweet_retweets"]
    tweet_favorites = r_object["tweet_favorites"]
    user_location = r_object["user_location"]
    user_verified = r_object["user_verified"]
    user_followers = r_object["user_followers"]
    user_friends = r_object["user_friends"]
    user_n_status = r_object["user_n_status"]
    user_create = r_object["user_create"]
    #Calling Sentiment analysis function to process wheather the tweet in positive, negative or neutral
    
    feedback = reply_tweet(tweet_text, tweet_id)
    #Appending every values we need to list and return it
    data = [username, tweet_id, tweet_text,tweet_create,tweet_retweets,tweet_favorites,user_location,user_verified,user_followers, user_friends, user_n_status, user_create, feedback]
    return data


#Map your function on each rdd
def exrdd(rdd,spark):
    print('empty rdd')
    #Check if rdd is empty
    if not rdd.isEmpty():
        print('rdd exist')
        rdd = rdd.map(exrow)
      #Carry your data in a list
          data = [username, tweet_id, tweet_text,tweet_create,tweet_retweets,tweet_favorites,user_location,user_verified,user_followers, user_friends, user_n_status, user_create, feedback]
          #Create Schema for tweet data to save
          IntegerType
        schema = StructType([
        StructField("username", StringType(),True),
        StructField("tweet_id", StringType(),True),
        StructField("tweet_text", StringType(),True),
        StructField("tweet_create", DateType(),True),
        StructField("tweet_retweets", IntegerType(),True),
        StructField("tweet_favorites", IntegerType(),True),
        StructField("user_location", StringType(),True),
        StructField("user_verified", BooleanType,True),
        StructField("user_followers", IntegerType(),True),
        StructField("user_friends", IntegerType(),True),
        StructField("user_n_status", IntegerType(),True),
        StructField("user_create", DateType(),True),
        StructField("feedback", StringType(),True)
        ])
        print('creating df..')
        #Create DataFrame using spark session of your rdd& schema created
        df = spark.createDataFrame(rdd, schema)
        df.show()
        print('writing to parquet..')
        #Write dataframe in parquet file and assign the file to specific hive table
        df.write.mode("append").parquet("hdfs://sandbox-hdp.hortonworks.com:8020//apps//hive//warehouse//your_database.db//your_table//")

)



spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = spark.sparkContext

sc.setLogLevel("WARN")


ssc = StreamingContext(sc, 5)
topic_name = ''
kafkaStream = KafkaUtils.createDirectStream(ssc, topics=[topic_name], kafkaParams={"metadata.broker.list": "sandbox-hdp.hortonworks.com:6667"})

kafkaStream.foreachRDD(lambda rdd:exrdd(rdd,spark))

ssc.start()
ssc.awaitTermination()
