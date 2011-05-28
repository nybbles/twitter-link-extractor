import pymongo
from pymongo import Connection

class LinkStore(object):
    track_words = []
    links = None

    def __init__(self, track_words, conn = Connection('localhost', 27017)):
        self.track_words = track_words
        
        conn = Connection('localhost', 27017)
        tle_db = conn['tle']
        self.links = tle_db[self.get_coll_name()]

    def get_coll_name(self):
        return "%".join(self.track_words)
        
    def store_link_tweet(self, link, tweet):
        tweet = tweet_to_json(tweet)

        query = {"link" : link}
        update = {"$inc" : {"ntweets" : 1},
                  "$push" : {"tweets" : tweet}}

        self.links.update(query, update, upsert=True)

    def remove_link(self, link):
        query = {"link" : link}
        self.links.remove(query)

    def add_empty_link(self, link):
        self.links.insert({"link" : link, "ntweets" : 0, "tweets" : []})

    def merge_resolved_link\
            (self, original_link, resolved_link, upsert_resolved=True):
        query = {"link" : original_link}
        result = self.links.find_and_modify(query, remove=True)

        if result is None:
            return

        query = {"link" : resolved_link}
        update = {"$inc" : {"ntweets" : result["ntweets"]},
                  "$pushAll" : {"tweets" : result["tweets"]}}

        # Upsert allowed in case this is the first original link to be
        # resolved to the resolved link (so the entry does not exist).
        self.links.update(query, update, upsert=upsert_resolved)

    # Untested: A way of resolving links in bulk - just update all of
    # urls with the resolved urls and then do map/reduce.
    def merge_resolved_links(self, resolved_link):
        self.links.map_reduce(
            "function() {\
                 emit(this.link, {ntweets : this.ntweets, tweets : this.tweets});\
            }",
            "function(key, values) {\
                 var result = {ntweets : 0, tweets : []};\
                 values.forEach(function(value) {\
                     result.ntweets += value.ntweets;\
                     result.tweets = result.tweets.concat(value.tweets);\
                 }\
                 return result;\
            }",
            self.get_coll_name(),
            reduce_output=self.get_coll_name())

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
