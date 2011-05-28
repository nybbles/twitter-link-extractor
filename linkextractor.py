#!/usr/bin/env python
tweepy_lib_loc = ""

import pymongo

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
import urlresolver

class LinkExtractor(object):
    running = False
    track_words = None
    link_count_limit = None

    status_auth = None
    status_listener = None
    status_stream = None

    link_store = None
    url_resolver = None
    
    def __init__(self,
                 consumer_key, consumer_secret, access_token,
                 access_token_secret, track_words, link_count_limit=10):
        self.link_count_limit = link_count_limit
        self.track_words = track_words

        conn = pymongo.Connection('localhost', 27017)
        self.link_store = linkstore.LinkStore(track_words=track_words, conn=conn)
        self.url_resolver = urlresolver.URLResolver(conn=conn)

        self.status_auth = \
            twpy.auth.OAuthHandler(consumer_key, consumer_secret, secure=True)
        self.status_auth.set_access_token(access_token, access_token_secret)

        on_status_cb = lambda x: self.on_status(x)
        self.status_listener = StatusListener(on_status_cb)

        self.status_stream = \
            twpy.streaming.Stream(self.status_auth, self.status_listener)

    def run(self):
        if self.running:
            return

        self.running = True
        self.status_stream.filter(track=self.track_words, async=True)

    def __del__(self):
        self.stop()

    def on_status(self, status):
        self.extract_and_store_links(status)

    def extract_and_store_links(self, status):
        for link in extract_links(status.text, status.truncated):
            link, _ = self.url_resolver.lookup_url(link)
            self.link_store.store_link_tweet(link, status)
            
    def stop(self):
        self.status_stream.disconnect()
        self.running = False

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
                
            yield match.group('url')
    except StopIteration:
        return

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

track_words = ["vancouver"]

tle = LinkExtractor(consumer_key, consumer_secret,
                    access_token, access_token_secret,
                    track_words, link_count_limit=10)
tle.run()

urlr = urlresolver.URLResolver()
urlr_linkstore = linkstore.LinkStore(track_words, conn=urlr.get_mongodb_conn())
urlr.run(cbs=[lambda orig, resolved: \
              urlr_linkstore.merge_resolved_link(orig, resolved)]);
