import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from preprocess_tweets import preprocess
from insertToHana import insertIntoTable
import sys
# import insertToHana

consumer_key = 'GulHjuomtnilISVouWenn0t5O'
consumer_secret = 'MqARy73MI0zC0TwHuA6ImqfNUVYrMKCya6zPKlCekSkM54BVni'
access_token = '90368754-a4w8Et5lrZ572bQjnsj5BfvbYm0dNokHDAaoH0LNG'
access_secret = 'dovJnqoWo7wBMetbfDpe7i9YyPGe5jsOXUoBbw907UJ3K'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth)


class MyListener(StreamListener):
    
    counter=0

    def check_data(self, data):
        print(data)

    def on_data(self, data):
        try:
            with open('live_tweets_hana.json', 'a') as f:
                f.write(data)
                #self.check_data(data)
                insertIntoTable(data)
                self.counter+=1
                sys.stdout.write('\r'+'Inserted '+str(self.counter)+' tweets into Table...')
                sys.stdout.flush()
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
 
    def on_error(self, status):
        print(status)
        return True
 
twitter_stream = Stream(auth, MyListener())
twitter_stream.filter(track=['#python'])
