import pymongo
from pymongo import Connection

import urllib2

class LinkStore(object):
    track_words = []
    links = None
    resolved_urls = None
    
    def __init__(self, track_words):
        self.track_words = track_words
        
        conn = Connection('localhost', 27017)
        tle_db = conn['tle']
        self.links = tle_db[self.get_coll_name()]
        self.resolved_urls = tle_db["resolved_urls"]

    def get_coll_name(self):
        return "%".join(self.track_words)

    def resolve_url(self, url):
        result = self.resolved_urls.find_one({"url" : url}, {"resolved_url" : 1})
        
        if result is not None:
            return result['resolved_url']

        resolved_url = urllib2.urlopen(url, None, 1).geturl()
        
        new_entry = {"url" : url, "resolved_url" : resolved_url}
        self.resolved_urls.insert(new_entry)

        return resolved_url

    def store_link_tweet(self, link, tweet):
        link = self.resolve_url(link)
        
        tweet = tweet_to_json(tweet)

        criteria = {"link" : link}
        update = {"$inc" : {"nlinks" : 1},
                  "$push" : {"tweets" : tweet}}

        self.links.update(criteria, update, upsert=True)

import datetime

def tweet_to_json(tweet):
    unmodified_keys = \
        ["coordinates", "favorited", "geo", "id",
         "in_reply_to_screen_name",
         "in_reply_to_status_id", "in_reply_to_user_id",
         "place",
         "retweet_count", "retweeted",
         "source", "source_url", "text", "truncated"]
    
    result = {}
    for k in unmodified_keys:
        result[k] = tweet.__getattribute__(k)

    result["author"] = get_tweet_author(tweet)
    result["retweeted_status"] = get_retweeted_status(tweet)
    result["created_at"] = get_tweet_creation_date(tweet)

    return result

def get_tweet_author(tweet):
    return {"id" : tweet.author.id,
            "screen_name" : tweet.author.screen_name}

def get_retweeted_status(tweet):
    if "retweeted_status" in tweet.__dict__:
        return {"id" : tweet.retweeted_status.id}
    else:
        return None

def get_tweet_creation_date(tweet):
    return str(tweet.created_at)
