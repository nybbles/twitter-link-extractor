import pymongo
from pymongo import Connection

import urllib2
import logging

class URLResolver(object):
    resolved_urls = None

    def __init__(self, conn = Connection('localhost', 27017)):
        tle_db = conn['tle']
        self.resolved_urls = tle_db["resolved_urls"]

    # lookup_url(url) ===> (resolved_url, is_resolved)
    def lookup_url(self, url):
        query = {"url" : url}
        result = \
            self.resolved_urls.find_one\
            (query, {"resolved_url" : 1, "resolved" : 1})
        
        if result is not None:
            if result["resolved"]:
                return result['resolved_url'], True
            else:
                return url, False

        new_entry = {"url" : url, "resolved" : False}
        self.resolved_urls.insert(new_entry)

        return url, False
        
    def resolve_url(self, url):
        try:
            resolved_url = urllib2.urlopen(url, None, 1).geturl()
        except urllib2.URLError as e:
            logging.info("Failed to resolve %s: %s" % (url, e.reason))
            return None

        criteria = {"url" : url, "resolved" : False}
        update = {"$set" : {"resolved_url" : resolved_url,
                            "resolved" : True}}
        
        self.resolved_urls.update(new_entry)
