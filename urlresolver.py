import pymongo
from pymongo import Connection

import urllib2
import time
import logging

from threading import Thread

import re

class URLResolutionError(Exception):
    url = None
    err = None
    
    def __init__(self, url, err):
        self.url = url
        self.err = err

    def __str__(self):
        return self.url.encode("latin-1", "replace") + " " + repr(self.err)

class URLResolver(object):
    running = False
    
    resolved_urls = None

    inaccessible_url_cb = None
    resolved_url_cb = None

    def __init__(self,
                 conn = Connection('localhost', 27017),
                 resolved_url_cb = None,
                 inaccessible_url_cb = None):
        self.resolved_url_cb = resolved_url_cb
        self.inaccessible_url_cb = inaccessible_url_cb

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

    def handle_failed_resolve(self, err, url, code=None):
        if code is None:
            code = err.code

        if code == 404 or code == 403 or code == 401:
            # resource not found/forbidden/unauthorized
            logging.info("Failed to resolve %s: Not found" % (url))
            if self.inaccessible_url_cb:
                self.inaccessible_url_cb(url, code)
                query = {"url" : url}
                self.resolved_urls.remove(query)
            else:
                raise err
        elif code == 408 or code == 503 or code == 500: # request timeout
            logging.info("Failed to resolve %s: Timeout" % (url))
            
            query = {"url" : url, "resolved" : False}
            update = {"$inc" : {"timeouts" : 1}}
            self.resolved_urls.update(query, update)
        elif (code == 302 or code == 301) and \
             re.search("The HTTP server returned a redirect error that would lead to an infinite loop.", str(err)):
            self.handle_failed_resolve(err, url, 404)
        else:
            raise err
        
    def resolve_url(self, url, timeout=1):
        try:
            resolved_url = urllib2.urlopen(url, None, timeout).geturl()
        except urllib2.HTTPError as e:
            self.handle_failed_resolve(e, url)
            return None
        except urllib2.URLError as e:
            if str(e) == "<urlopen error timed out>":
                self.handle_failed_resolve(e, url, 408)
                return None
            elif str(e) == "<urlopen error [Errno 8] nodename nor servname provided, or not known>":
                # This happens when DNS resolution fails, apparently.
                self.handle_failed_resolve(e, url, 404)
                return None
            elif str(e) == "<urlopen error _ssl.c:484: The handshake operation timed out>" or \
                 str(e) == "<urlopen error The read operation timed out>":
                # SSL handshake timeout and read timeout
                self.handle_failed_resolve(e, url, 500)
                return None
            else:
                raise URLResolutionError(url, e)
        except UnicodeError as e:
            raise URLResolutionError(url, e)

        self.set_url_as_resolved(url, resolved_url)
        return resolved_url

    def set_url_as_resolved(self, url, resolved_url):
        query = {"url" : url, "resolved" : False}
        update = {"$set" : {"resolved_url" : resolved_url,
                            "resolved" : True}}
        
        self.resolved_urls.update(query, update)
        return resolved_url

    def resolve_unresolved_urls(self):
        sort = [("timeouts", 1)]
        query = {"resolved" : False}

        while self.running:
            result = self.resolved_urls.find_one(query, sort=sort)

            if result is None:
                time.sleep(10)
                continue

            timeouts = result.get("timeouts", 0)
            url = result["url"]
            
            if timeouts >= 10:
                # After too many timeouts, treat resource as not found.
                self.handle_failed_resolve("timeout limit", url, 404)
            elif timeouts > 0:
                timeout = timeouts * 5
            else:
                timeout = 1

            resolved_url = self.resolve_url(url, timeout)

            if resolved_url is None:
                continue

            if self.resolved_url_cb:
                self.resolved_url_cb(url, resolved_url)

    def run(self):
        if self.running:
            return

        self.running = True
        Thread(target=self.resolve_unresolved_urls).start()

    def stop(self):
        self.running = False
