# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
import hashlib
import sys
import json
import pymongo
from scrapy.conf import settings
#from pybloom import BloomFilter
import time

class ScrapyGooglenewsPipeline(object):

    def __init__(self):

        #connect to MongoDB
        self.connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        #self.bf = BloomFilter(capacity=10000, error_rate=0.0001)

        self.md5_seen = set()

    def open_spider(self, spider):
        self.db = self.connection[settings['MONGODB_DB']]
        self.collection = self.db[settings['MONGODB_COLLECTION'] +'_' + time.strftime("%d/%m/%Y")]

    def close_spider(self, spider):
        self.connection.close()

    #convert url to md5 as id_document
    def url2md5(self, url):
        m = hashlib.md5()
        m.update(url)
        return m.hexdigest()

    def process_item(self, item, spider):
        reload(sys)
        sys.setdefaultencoding('utf-8')
        
        url = item['link'].encode('utf-8')
        md5 = self.url2md5(url)

        if md5 in self.md5_seen:
            raise DropItem("Duplicate item found: %s" % md5)
        else:
            self.md5_seen.add(md5)
##################################################
            #store document as json file
            '''
            article = {
                'id': md5,
                'link': url,
                'author': item['author'],
                'title': item['title'],
                'text': item['text']
            }

            if item['text']:
                path = '/wsu/home/fk/fk01/fk0176/nytimes_2014/data/rawData'
                if not os.path.isdir(path):
                    os.mkdir(path)

                with open(path + '/%s' % md5 + '.json', 'w') as f:
                    json.dump(article, f)
            '''
##################################################
            #store document into MongoDB

            title_md5 = self.url2md5(item['title']) #filter doc by title
            article = {
                '_id': title_md5,
                'link': url,
                'domain': item['domain'],
                'title': item['title'],
                'text': item['text'],
                #'authors': item['authors'],
                #'data': repr(item['date'])
            }

            if (len(item['text'].split(' ')) > 100) and any(item['title']):
                self.collection.insert(article)
                print "==================Articles added to MongoDB database!==============="

