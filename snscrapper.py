import snscrape.modules.twitter as sntwitter
import pandas as pd
from textblob import TextBlob


def reply_tweet(tweet_content):
    analysis = TextBlob(tweet_content)
    if analysis.sentiment.polarity > 0:
        feedback = 'positive'

    elif analysis.sentiment.polarity == 0:
        feedback = 'neutral'
    else:
        feedback = 'negative'
    return(feedback)

tweets_list = []

for i,tweet in enumerate(sntwitter.TwitterSearchScraper('search_word since:date until:date').get_items()):
    feedback = reply_tweet(tweet.content)


    tweets_list.append([tweet.user.username, tweet.id, tweet.content, tweet.date,tweet.retweetCount, tweet.likeCount, tweet.user.location, tweet.user.verified, tweet.user.followersCount, tweet.user.friendsCount, tweet.user.statusesCount, tweet.user.created, tweet.source ,feedback])

   
tweets_df2 = pd.DataFrame(tweets_list, columns=["username", "tweet_id", "tweet_text", "tweet_create", "tweet_retweets", "tweet_favorites", "user_location", "user_verified", "user_followers", "user_friends", "user_n_status", "user_create", "tweet_source","feedback"])
tweets_df2.to_csv('path', mode = 'a')
