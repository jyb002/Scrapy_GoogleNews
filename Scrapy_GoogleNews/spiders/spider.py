__author__ = 'yingbo'

import scrapy
from Scrapy_GoogleNews.items import ScrapyGooglenewsItem
import logging
from newspaper import Article


class googlenews_spider(scrapy.Spider):
    name = "Scrapy_GoogleNews"
    start_urls = ["https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=w",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=n",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=b",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=tc",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=e",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=s",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=snc",
                  "https://news.google.com/news/section?cf=all&pz=1&ned=us&topic=m",
                  ]
    baseURL = "https://news.google.com"

    def parse(self, response):
        for href in response.xpath('//div[@class="moreLinks"]/a/@href').extract():
            full_url = self.baseURL + href
            yield scrapy.Request(full_url, callback = self.parse_news)

    def parse_news(self, response):
        item = ScrapyGooglenewsItem()
        #only log the warning info from request
        logging.getLogger("requests").setLevel(logging.WARNING)

        for href in response.xpath('//h2[@class="title"]/a/@href').extract():
            item['link'] = href
            #use newspaper-0.0.8 to scrape the webpage, then get clean text.
            article = Article(item['link'])
            article.download()
            article.parse()
            item['title'] = article.title
            item['text'] = article.text
            #item['authors'] = article.authors
            #item['date'] = article.publish_date

            if response.url.split('&')[-1] == 'topic=w':
                item['domain'] = 'World'
            if response.url.split('&')[-1] == 'topic=n':
                item['domain'] = 'U.S.'
            if response.url.split('&')[-1] == 'topic=b':
                item['domain'] = 'Business'
            if response.url.split('&')[-1] == 'topic=tc':
                item['domain'] = 'Technology'
            if response.url.split('&')[-1] == 'topic=e':
                item['domain'] = 'Entertainment'
            if response.url.split('&')[-1] ==  'topic=s':
                item['domain'] = 'Sports'
            if response.url.split('&')[-1] ==  'topic=snc':
                item['domain'] = 'Science'
            if response.url.split('&')[-1] ==  'topic=m':
                item['domain'] = 'Health'

            yield item
