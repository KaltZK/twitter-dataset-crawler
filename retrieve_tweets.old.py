#  -*- coding: utf-8 -*-
import pandas as pd
import json
from threading import Thread
from Queue import Queue
import tweepy
import csv
import os
import re
import traceback
from glob import glob
from unqlite import UnQLite
consumer_key = 'boQ6Sgzco6Rv0ucn1UZTOp1XV'
consumer_secret = 'jhTWNIMK0xHsmw6wQcKpdwpMSLXMTjXihFpDAV9s66xmHKiCpH'
access_token = '915645899065352193-5Sp2F701ITRSC1F42d5hXnGatWca5WO'
access_secret = 'm40tSzeAoRt5qecaa1u31ij1hYIGkyWxljxGMfRmni2lW'

non_text_re = re.compile(r'RT @.+?:|.?@\S+|https?://[^/]+?/.+|#\S+')

db = UnQLite('llt/tmp/db.unqlite')

class TweetDownThread(Thread):
    def __init__(self, id_queue: Queue, ct_queue: Queue):
        super(TweetDownThread, self).__init__()
        self.id_queue = id_queue
        self.ct_queue = ct_queue
    def run(self):
        tweetid = self.id_queue.get()
        while tweetid is not None:
            
            tweetid = self.id_queue.get()


def retrieve_tweets(input_file, output_file):
    """
    Takes an input filename/path of tweetIDs and outputs the full tweet data to a csv
    """

    if db.exists(input_file):
      pn = int(db[input_file])
    else:
      pn = 0
      db[input_file] = 0
      db.commit()
    
    print "Begin with",pn


    # Authorization with Twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=5, retry_delay=5,
                     retry_errors=set([401, 404, 500, 503]))

    df = pd.read_csv(input_file)
    print "Index Loaded"
    output = open(output_file, 'a')


    for tweetid in df.iloc[pn:, 0]:
    #
    #     # csvFile = open(output_file, 'a')
    #     #csvFile = open(output_file, 'wb')
    #     #csvWriter = csv.writer(csvFile)
    #
        # Create output file
        # 将正则表达式编译成Pattern对象
        p1 = re.compile(r'RT @.+?:')  ##转发标志
        p2 = re.compile(r'.?@\S+')  ##at某人
        p3 = re.compile(r'https?://[^/]+?/.+')  # 链接
        p4 = re.compile(r'#\S+')  ##话题标签
        try:
            print tweetid
            status = api.get_status(tweetid)
            try:
              m_s = p4.sub('', p3.sub('', p2.sub('', p1.sub('', status.text.strip().replace('\n','')))))
            except UnicodeEncodeError, e:
              print repr(status.text)
              # exit(-1)
              raise e
            if len(m_s) < 10:
                continue
            else:
                lineInfos = 
                lineInfos = "{\"id\": \"" + str(status.id) + "\"," + "\"date\": \"" + str(status.created_at) + "\"," + "\"text\" :\"" + status.text.strip().replace('\n',' ') + "\","
                lineInfos += "\"favourites_count\" :\"" + str(status.user.favourites_count) + "\"," + "\"statuses_count\" :\"" + str(status.user.statuses_count) + "\"," + "\"verified\" :\"" + str(status.user.verified)+"\","
                lineInfos +=   "\"following\" :\"" + str(status.user.following) + "\"," + "\"listed_count\" :\"" + str(status.user.listed_count) + "\"," + "\"followers_count\" :\"" + str(status.user.followers_count)+"\","
                lineInfos +=  "\"friends_count\" :\"" + str(status.user.friends_count)  + "\"," + "\"favorite_count\" :\"" +  str(status.favorite_count)  + "\"," + "\"retweeted\" :\"" +  str(status.retweeted)+"\","
                lineInfos +=  "\"retweet_coun\" :\"" + str(status.retweet_count) + "\"}"
                output.write(lineInfos.encode('utf-8') + "\n")
                print lineInfos
    #
    #         csvWriter.writerow([status.text.decode("utf-8").encode("gb2312"),
    #                             status.created_at,
    #                             # status.geo,
    #                             # status.lang,
    #                             # status.place,
    #                             # status.coordinates,
    #                             status.user.favourites_count,
    #                             status.user.statuses_count,
    #                             # status.user.description,
    #                             # status.user.location,
    #                             # status.user.id,
    #                             # status.user.created_at,
    #                             status.user.verified,
    #                             status.user.following,
    #                             # status.user.url,
    #                             status.user.listed_count,
    #                             status.user.followers_count,
    #                             # status.user.default_profile_image,
    #                             status.user.utc_offset,
    #                             status.user.friends_count,
    #                             # status.user.default_profile,
    #                             # status.user.name,
    #                             # status.user.lang,
    #                             # status.user.screen_name,
    #                             # status.user.geo_enabled,
    #                             # status.user.profile_background_color,
    #                             # status.user.profile_image_url,
    #                             # status.user.time_zone,
    #                             status.id,
    #                             status.favorite_count,
    #                             status.retweeted,
    #                             # status.source,
    #                             # status.favorited,
    #                             status.retweet_count])
        except Exception as e:
            print(e)
            traceback.print_exc()
            
        pn += 1
        db[input_file] = pn
        db.commit()

if __name__ == "__main__":
    # filename = "2016-sismoecuador.ids"
    # filename = "2016-panamapapers.ids"
    db.begin()
    # filename = "2015-charliehebdo.ids"
    # retrieve_tweets('llt/twitter-events-2012-2016/'+filename,
                    # 'llt/Data/TweetDataset/' + filename)
    for f in glob('llt/twitter-events-2012-2016/*.ids'):
      print f
      retrieve_tweets(f, 'llt/Data/TweetDataset/%s'%os.path.basename(f))
    # inputfile = "F:/llt/twitter-events-2012-2016/"
    # outputfile = "F:/llt/Data/TweetDataset/"
    # pathDir = os.listdir(inputfile)
    # for allDir in pathDir:
    #     child = os.path.join('%s%s' % (inputfile, allDir))
    #     retrieve_tweets(child, outputfile + allDir + '.csv')
    # retrieve_tweets('F:/llt/OriginalData/tweet_id/Uttarakhand Flood.csv', 'F:/llt/OriginalData/DisasterDataset/Uttarakhand Flood_tweets1.csv')
    # retrieve_tweets('F:/llt/OriginalData/tweet_id/Typhoon Hagupit.csv', 'F:/llt/OriginalData/DisasterDataset/Typhoon Hagupit_tweets1.csv')
    # retrieve_tweets('F:/llt/OriginalData/tweet_id/Sandy Hook Shootout.csv', 'F:/llt/OriginalData/DisasterDataset/Sandy Hook Shootout_tweets1.csv')
    # retrieve_tweets('F:/llt/OriginalData/tweet_id/Nepal Earthquake.csv', 'F:/llt/OriginalData/DisasterDataset/Nepal Earthquake_tweets1.csv')
    # retrieve_tweets('F:/llt/OriginalData/tweet_id/Hyderabad Blast.csv', 'F:/llt/OriginalData/DisasterDataset/Hyderabad Blast_tweets1.csv')
    # retrieve_tweets('F:/llt/OriginalData/tweet_id/Harda Train Derailment.csv', 'F:/llt/OriginalData/DisasterDataset/Harda Train Derailment_tweets1.csv')