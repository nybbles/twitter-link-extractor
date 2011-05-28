#!/usr/bin/env python
tweepy_lib_loc = ""

import sys
sys.path.append(tweepy_lib_loc)

import tweepy as twpy
from tweepy.error import TweepError

class TwitterLinkExtractor(twpy.streaming.StreamListener):
    extracted_link_count = {}
    link_count_limit = None

    def __init__(self, link_count_limit=10):
        super(TwitterLinkExtractor, self).__init__()
        self.link_count_limit = link_count_limit

    def on_error(self, status_code):
        err_str = "Error! %d" % (status_code)
        raise TweepError(err_str)
    
    def on_timeout(self):
        raise TweepError("Timeout!")

    def on_status(self, status):
        self.extract_links(status)

    def extract_link(self, status):
        for link in extract_links(status.text):
            updated_link_count = extracted_link_count.get(link, 0) + 1
            
            if updated_link_count == link_count_limit:
                self.output_extracted_link(link)

            extracted_link_count[link] = updated_link_count

    def output_extracted_link(self, link):
        print link.encode('latin_1', 'replace')

# This should be based on http://tools.ietf.org/html/rfc1808.html,
# section 2.2, to detect all possible URLs, but it isn't, because this
# is going to take a lot less time and seems to be what Twitter does.
import re
url_extractor_re = re.compile("(?P<url>https?://[^\s]+)", re.I)
def extract_links(text):
    links_iter = url_extractor_re.finditer(text)
    links_remain = True

    try:
        while links_remain:
            yield links_iter.next().group('url')
    except StopIteration:
        return

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
