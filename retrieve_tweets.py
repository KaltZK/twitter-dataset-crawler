#  -*- coding: utf-8 -*-
import json
import csv
import os
import re
import time
import traceback
from glob import glob
from threading import Thread, Event
from Queue import Queue
from Queue import Empty as QueueEmpty

from unqlite import UnQLite
import tweepy
import pandas as pd
import numpy as np


THREAD_NUM = 16
DELAY_TIME = 0.5
PAUSE_TIME = 15.0
MAX_RECOVER_TIMES = 40
TWEET_CHUNK_SIZE = 100

GLOBAL_PAUSE = True


db = UnQLite('llt/tmp/db.unqlite')

class TweetThread(Thread):
    def __init__(self, id_queue, cnt_queue, tkn_queue, rec_queue, stop_evt):
        super(TweetThread, self).__init__()
        self.id_queue = id_queue
        self.cnt_queue = cnt_queue
        self.tkn_queue = tkn_queue
        self.rec_queue = rec_queue
        self.stop_evt = stop_evt
        self.setDaemon(True)

class TweetSaveThread(TweetThread):
    def __init__(self, filename, *args):
        super(TweetSaveThread, self).__init__(*args)
        self.filename = filename
    def run(self):
        f = open(self.filename, 'a')
        while not self.stop_evt.is_set():
            try:
                status = self.cnt_queue.get_nowait()
            except QueueEmpty, e:
                continue
            f.write(json.dumps(status))
            f.write("\n")
        f.close()
        print "SAVING STOPPED."

class TweetDownThread(TweetThread):
    NETWORK_ERROR_KEYWORDS = [
        'bad handshake',
        'NewConnectionError',
        'ProxyError'
    ]
    CRITICAL_TWITTER_ERROR_CODES = [
        38
    ] 
    def __init__(self, api_info, proxy, delay, pause_evt, pause_time, *args):
        super(TweetDownThread, self).__init__(*args)
        self.delay = delay
        self.pause_evt = pause_evt
        self.pause_time = pause_time
        consumer_key, consumer_secret, access_token, access_secret = api_info
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        self.api = tweepy.API(
                auth,
                wait_on_rate_limit=True, 
                wait_on_rate_limit_notify=True, 
                retry_count=5, 
                retry_delay=5,
                proxy = proxy,
                retry_errors=set([401, 404, 500, 503]))
    def run(self):
        non_text_re = re.compile(r'RT @.+?:|.?@\S+|https?://[^/]+?/.+|#\S+|^\s+|\s$|\n+')
        while not self.stop_evt.is_set():
            try:
                req = self.id_queue.get_nowait()
            except QueueEmpty, e:
                continue
            tweetid_list, idx, rec_t = req
            try:
                statuses_list = self.api.statuses_lookup(tweetid_list)
                print "\t^",
                for status in statuses_list:
                    text = status.text.strip()
                    if len(non_text_re.sub('', text)) < 10:
                        print "IGN:", text
                    else:
                        status_data = dict(
                            id   = status.id,
                            date = status.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                            text = text,
                            favourites_count = status.user.favourites_count,
                            statuses_count = status.user.statuses_count,
                            verified = status.user.verified,
                            following = status.user.following,
                            listed_count = status.user.listed_count,
                            followers_count = status.user.followers_count,
                            retweet_count = status.retweet_count
                        )
                        self.cnt_queue.put(status_data)
                        print '.',
                print "$"
            except tweepy.TweepError as e:
                print(e)
                traceback.print_exc()
                if isinstance(e.message, list):
                    if any( er['code'] in self.CRITICAL_TWITTER_ERROR_CODES for er in e.message):
                        print "==!!A CRITICAL ERROR HAPPENS.!!=="
                        print "Everything Goes Black"
                        break
                elif any( em in e.message for em in self.NETWORK_ERROR_KEYWORDS ):
                    self.pause_evt.set()
                    print "!!CONNECTION ERROR!!"
                    self.rec_queue.put((tweetid_list, idx, rec_t+1))
                    time.sleep(self.pause_time)
                    self.pause_evt.clear()

            finally:            
                self.tkn_queue.put(True)
                if GLOBAL_PAUSE and self.pause_evt.is_set():
                    print "PAUSED for %d s" % self.pause_time
                    time.sleep(self.pause_time)
                else:
                    pass
                    # time.sleep(self.delay)
                self.tkn_queue.put(True)


def retrieve_tweets(input_file, output_file, pool_size, accounts, proxies):
    stop_flag = False

    save_ct = 0

    print "On Dataset:", input_file

    pn = 0
    if db.exists(input_file):
      pn = int(db[input_file])
    else:
      db[input_file] = 0
      db.commit()
    
    print "From", pn

    df = pd.read_csv(input_file)
    length, col_n = df.shape
    chunks = np.array_split(df.iloc[pn:, 0], (length - pn) / TWEET_CHUNK_SIZE + 1)
    print "Index Loaded."
    print "Chunk Number:", len(chunks)

    if pn >= length:
        print "Everything up-to-date."
        return True

    id_queue = Queue()
    cnt_queue= Queue()
    tkn_queue= Queue()
    rec_queue= Queue()
    stop_evt = Event()
    pause_evt= Event()
    threads  = [TweetDownThread(
                    accounts[i%len(accounts)],
                    proxies[i%len(proxies)],
                    DELAY_TIME,
                    pause_evt,
                    PAUSE_TIME,
                    id_queue,
                    cnt_queue,
                    tkn_queue,
                    rec_queue,
                    stop_evt
                    ) for i in xrange(pool_size)]
    save_thread = TweetSaveThread(output_file, id_queue, cnt_queue, tkn_queue, rec_queue, stop_evt)
    
    save_thread.start()
    for t in threads: 
        t.start()
        time.sleep(DELAY_TIME / pool_size)
    for _ in xrange(pool_size): 
        tkn_queue.put(True)
    
    print "Threads Ready."
    
    def _tweet(tweetid_list, p, rec_t = 0):
        id_queue.put( (tweetid_list, p, rec_t) )
        time.sleep(DELAY_TIME)
    
    def _clear_rec_queue():
        while not rec_queue.empty():
            rtids, idx, rec_t = rec_queue.get()
            if rec_t == MAX_RECOVER_TIMES:
                print "Task Gave Up (Recover Limit Reached):", rtids
            else:
                print "Recover (", rec_t , "):", rtids
                _tweet(rtids, idx, rec_t)

    try:
        for tweet_id_list in chunks:
            tkn_queue.get()
            _clear_rec_queue()
            print "Task Submitted =>", pn, '->', pn+TWEET_CHUNK_SIZE, "<="
            _tweet(list(tweet_id_list), pn)
            pn += TWEET_CHUNK_SIZE
            db[input_file] = pn
            save_ct += 1
            if save_ct % 50 == 0:
                db.commit()
            if not all(t.isAlive() for t in threads):
                raise Exception("Threads are dead.")
        print "Finished."
    except KeyboardInterrupt, e:
        stop_evt.set()
        print "Stopped."
        stop_flag = True
    finally:
        db.commit()
        print "Progress Saved."
    return not stop_flag

if __name__ == "__main__":
    import random
    db.begin()
    tokens = [
        map(tk.__getitem__, (
            'consumer_key', 
            'consumer_secret', 
            'access_token', 
            'access_secret'
        ))
        for tk in json.load(open('config/tokens.json'))
    ]
    for f in glob('llt/twitter-events-2012-2016/*.ids'):
        print f
        if not retrieve_tweets(
            f, 
            'llt/Data2/%s' % os.path.basename(f),
            THREAD_NUM,
            tokens,
            # proxies = [None],
            # proxies = ['127.0.0.1:49999'],
            proxies = ['127.0.0.1:12305'],
            # proxies = random.sample(open("config/proxies").read().split("\n"), THREAD_NUM)
        ): break