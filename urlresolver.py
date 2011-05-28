import pymongo
from pymongo import Connection

import urllib2
import time
import logging

from threading import Thread

class URLResolver(object):
    running = False
    
    resolved_urls = None

    def __init__(self, conn = Connection('localhost', 27017)):
        tle_db = conn['tle']
        self.resolved_urls = tle_db["resolved_urls"]

    def get_mongodb_conn(self):
        return self.resolved_urls.database.connection

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
        return resolved_url

    def resolve_unresolved_urls(self, cbs=[]):
        query = {"resolved" : False}

        while self.running:
            result = self.resolved_urls.find_one(query)

            if result is None:
                time.sleep(10)
                continue

            url = result["url"]
            resolved_url = self.resolve_url(url)

            if resolved_url is None:
                continue

            for callback in cbs:
                callback(url, resolved_url)

    def run(self, cbs=[]):
        if self.running:
            return

        self.running = True
        Thread(target=lambda : self.resolve_unresolved_urls(cbs)).start()

    def stop(self):
        self.running = False
