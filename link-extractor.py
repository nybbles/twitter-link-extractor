#!/usr/bin/env python
tweepy_lib_loc = ""

import sys
sys.path.append(tweepy_lib_loc)

import tweepy as twpy
from tweepy.error import TweepError

class TwitterLinkExtractor(twpy.streaming.StreamListener):
    def on_error(self, status_code):
        err_str = "Error! %d" % (status_code)
        raise TweepError(err_str)
    
    def on_timeout(self):
        raise TweepError("Timeout!")

    def on_status(self, status):
        self.extract_link(status)
        print "%s at %s: %s" % \
              tuple(map(lambda x: x.encode('latin-1'),
                        (status.author.screen_name,
                         str(status.created_at), status.text)))

    def extract_link(self, status):
        extract_link(status.text)

def extract_link(text):
    pass

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

twitterauth = \
    twpy.auth.OAuthHandler(consumer_key, consumer_secret, secure=True)
twitterauth.set_access_token(access_token, access_token_secret)

twitterlistener = TwitterLinkExtractor()
twitterstream = twpy.streaming.Stream(twitterauth, twitterlistener)

twitterstream.filter(track=["vancouver"])
