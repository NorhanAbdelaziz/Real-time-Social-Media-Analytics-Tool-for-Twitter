import jsonpickle as jsonpickle
from time import sleep
from json import dumps
from kafka import KafkaProducer
import tweepy as tw

#Twitter API Autherizations 
consumer_key= ""
consumer_secret= ""
access_token= ""
access_token_secret= ""

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

# Set the word or Hashtag that you wanna search 
search_words = ""

#Create empty dict to carry the data retrieved
record = {}
tweet_list = []
while True:
    tweets = tw.Cursor(api.search,q=search_words).items()
    for tweet in tweets:
        username = tweet.user.screen_name
        tweet_id = tweet.id_str
        tweet_txt = tweet.text
        tweet_create = tweet.created_at
        tweet_retweets = tweet.retweet_count
        tweet_favorites = tweet.favorite_count
        user_location = tweet.user.location
        user_verified = tweet.user.verified
        user_followers = tweet.user.followers_count
        user_friends = tweet.user.friends_count
        user_n_status = tweet.user.statuses_count
        user_create = tweet.user.created_at


#Check if we already have this tweet before
        if tweet_id not in tweet_list:
            
            tweet_list.append(tweet_id)
            
            #Append your tweet data in the dict created
            record = {"username" : username, "tweet_id" : tweet_id, "tweet_txt" : tweet_txt, "tweet_create" : tweet_create,  "tweet_retweets" : tweet_retweets, "tweet_favorites" : tweet_favorites, "user_location": user_location, "user_verified" : user_verified, "user_followers" : user_followers, "user_friends" : user_friends, "user_n_status" : user_n_status, "user_create" : user_create}
           
           #Set your server weathe it's local or in VM (I'am working in hortonworks)
            producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],value_serializer=lambda x:dumps(x).encode('utf-8'))
            row = jsonpickle.encode(record)
           topic_name = ''
           producer.send(topic_name, value=row)
            print("tweet : "+tweet.text+" "+tweet.id_str+" "+tweet.user.screen_name)
            print("record : "+record["tweet_txt"])
            sleep(5)


