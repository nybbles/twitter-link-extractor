#!/usr/bin/env python
tweepy_lib_loc = ""

import sys
sys.path.append(tweepy_lib_loc)

import tweepy as twpy
from tweepy.error import TweepError

class StatusListener(twpy.streaming.StreamListener):
    on_status_cb = None

    def __init__(self, on_status_cb=None):
        assert(on_status_cb is not None)

        super(StatusListener, self).__init__()
        self.on_status_cb = on_status_cb

    def on_error(self, status_code):
        err_str = "Error! %d" % (status_code)
        raise TweepError(err_str)
    
    def on_timeout(self):
        raise TweepError("Timeout!")

    def on_status(self, status):
        self.on_status_cb(status)

import linkstore

class LinkExtractor(object):
    link_count_limit = None

    status_auth = None
    status_listener = None
    status_stream = None

    link_store = None
    
    def __init__(self,
                 consumer_key, consumer_secret, access_token,
                 access_token_secret, track, link_count_limit=10):
        self.link_store = linkstore.LinkStore(track_words=track)

        self.status_auth = \
            twpy.auth.OAuthHandler(consumer_key, consumer_secret, secure=True)
        self.status_auth.set_access_token(access_token, access_token_secret)

        on_status_cb = lambda x: self.on_status(x)
        self.status_listener = StatusListener(on_status_cb)

        self.status_stream = \
            twpy.streaming.Stream(self.status_auth, self.status_listener)
        self.status_stream.filter(track=track, async=True)

    def __del__(self):
        self.stop()

    def on_status(self, status):
        self.extract_links(status)

    def extract_links(self, status):
        for link in extract_links(status.text, status.truncated):
            self.link_store.store_link_tweet(link, status)
            
    def stop(self):
        self.status_stream.disconnect()

# This should be based on http://tools.ietf.org/html/rfc1808.html,
# section 2.2, to detect all possible URLs, but it isn't, because this
# is going to take a lot less time and seems to be what Twitter does.
import re
url_extractor_re = re.compile("(?P<url>https?://[^\s#@]+)", re.I | re.U)
def extract_links(text, is_truncated):
    links_iter = url_extractor_re.finditer(text)

    try:
        while True:
            match = links_iter.next()
            
            if is_truncated:
                if match.end('url') == 140 - 4:
                    return # probably truncated URL
                
            result = match.group('url')

            if result[-1] == u'\u2026':
                return # url ends with ellipsis
            
            yield result
    except StopIteration:
        return

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

tle = LinkExtractor(consumer_key, consumer_secret,
                    access_token, access_token_secret,
                    ["vancouver"], link_count_limit=10)
