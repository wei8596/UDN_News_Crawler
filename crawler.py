# -*- coding: UTF-8 -*-

"""
Author:范真瑋
Date:2020/08/20
udn Web Crawler
"""

import requests as rq
from bs4 import BeautifulSoup
import re
import pandas as pd
import random
import time
import sys
import os
import json
import threading
from collections import deque


class UdnCrawler(object):
    def __init__(self):
        # user Info.(OS, browser...)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/84.0.4147.125 Safari/537.36 '
        }
        # store all the urls
        self.urlDeQueue = deque()
        # request page no.
        self.pageNo = 0
        # url's domain
        self.domain = 'https://udn.com'
        # store all the news data
        self.dataList = pd.DataFrame(data=None, columns=['Title', 'Category', 'Journalist', 'Time', 'Content'])
        # the path of the saving file
        self.path = 'udnNews/'
        # for rename dataframe index
        self.dfIndex = 0
        # # of threads
        self.thread_num = 5
        # thread lock
        self.lock = threading.Lock()
        # notify threads to exit
        self.exitGetFlag = 0
        self.exitCrawlFlag = 0

    # get news page's Title, Category, Journalist, Time, Content
    def getNewsData(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        # Title Category Journalist Time Content
        title = ''
        category = ''
        journalist = ''
        time = ''
        content = ''

        if soup.find('section', attrs={'itemprop': 'articleBody'}):
            # join all paragraphs into one string
            content = ''.join(i.text for i in soup.find('section', attrs={'itemprop': 'articleBody'})
                              .find_all('p', recursive=False))
            if soup.find('h1', attrs={'class': 'article-content__title'}):
                title = soup.find('h1', attrs={'class': 'article-content__title'}).text
            if soup.find_all('a', attrs={'class': 'breadcrumb-items'}):
                category = soup.find_all('a', attrs={'class':'breadcrumb-items'})[1].text
            if soup.find('span', attrs={'class': 'article-content__author'}):
                if soup.find('span', attrs={'class': 'article-content__author'}).find('a'):
                    journalist = soup.find('span', attrs={'class': 'article-content__author'}).find('a').text
            if soup.find('time', attrs={'class': 'article-content__time'}):
                time = soup.find('time', attrs={'class': 'article-content__time'}).text
        # 會員專屬內容 (no content)
        else:
            if soup.find('script', type='application/ld+json'):
                # string
                script = str(soup.find('script', type='application/ld+json').string)
                # remove space, \n, \r, [, ] at the beginning and at the end of the string
                script = script.strip(' \n\r[]')
                script = json.loads(script)
                title = script['headline']
                category = script['articleSection']
                journalist = script['author']['name']
                time = script['datePublished']
                # transfer time format
                day = re.match(r'[\d\-]+', time).group(0)
                time = re.search(r'(\d\d:\d\d)', time).group(0)
                time = day + ' ' + time
                content = '會員專屬內容'

        data = pd.DataFrame(data=[{'Title': title,
                                   'Category': category,
                                   'Journalist': journalist,
                                   'Time': time,
                                   'Content': content
                                   }])
        self.lock.acquire()
        # rename dataframe index
        self.dfIndex += 1
        if self.dfIndex % 50 == 0:
            print(str(self.dfIndex) + ' news')
        data.rename(index={0: self.dfIndex}, inplace=True)
        self.lock.release()

        return data

    # get @num of news links
    def getNewsLinks(self, num=0):
        while not self.exitGetFlag:
            self.lock.acquire()
            # go to the next page
            self.pageNo += 1
            self.lock.release()
            # dynamic web page request
            url = 'https://udn.com/api/more?page={}&id=&channelId=1&cate_id=0&type=breaknews'.format(self.pageNo)
            # prevent detection
            random.seed(time.time())
            time.sleep(random.uniform(1, 2))
            # add user Info.(OS, browser...)
            response = rq.get(url, headers=self.headers)
            # check the request
            if response.status_code == rq.codes.ok:
                text = response.text
                # replace escape character
                wordFilter = '\\'
                text = text.replace(wordFilter, '')
                # find all urls
                selector = re.findall(r'/news/story/\d+/\d+', text)
                for newsUrl in selector:
                    length = len(self.urlDeQueue)
                    # get enough urls
                    if length >= num:
                        # notify threads to exit
                        self.exitGetFlag = 1
                        break
                    else:
                        self.lock.acquire()
                        if (length + 1) % 50 == 0:
                            print('# of urls: ' + str(length + 1))
                        self.urlDeQueue.append(newsUrl)
                        self.lock.release()
            # request failed
            else:
                print('request failed')
                sys.exit(1)

    # crawl news data
    def crawlNews(self):
        while not self.exitCrawlFlag:
            self.lock.acquire()
            # if urlDeQueue is not empty
            if self.urlDeQueue:
                url = self.urlDeQueue.popleft()
                # add the domain
                url = self.domain + url
                self.lock.release()
                # prevent detection
                random.seed(time.time())
                time.sleep(random.uniform(1, 2))
                # add user Info.(OS, browser...)
                response = rq.get(url, headers=self.headers)
                # check the request
                if response.status_code == rq.codes.ok:
                    data = self.getNewsData(response=response)
                    if data is None:
                        continue
                    else:
                        # the append method doesn't work in-place
                        # only self.dataList.append(data) is not working
                        self.lock.acquire()
                        self.dataList = self.dataList.append(data)
                        self.lock.release()
                # request failed
                else:
                    print('request failed')
                    sys.exit(1)
            else:
                # notify threads to exit
                self.exitCrawlFlag = 1
                # urlDeQueue is empty
                self.lock.release()

    # multi-thread crawler
    def crawl(self, num=0):
        self.multiThread(func=self.getNewsLinks, args=(num, ))
        self.multiThread(func=self.crawlNews)

    # save news data to the excel
    def saveToExcel(self, fileName=''):
        print('-----Saving Start-----')
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        self.dataList.to_excel(self.path + fileName)
        print('-----Saving End-----')

    # use multi-thread
    def multiThread(self, func, args=()):
        threads = []
        for i in range(self.thread_num):
            t = threading.Thread(target=func, args=args)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()


if __name__ == '__main__':
    udnCrawler = UdnCrawler()
    while 1:
        num = input('Please input # of news: ')
        if num.isdigit():
            num = int(num)
            print('-----Crawler Start-----')
            tStart = time.time()    # start
            udnCrawler.crawl(num=num)
            tEnd = time.time()  # end
            print('-----Crawler End-----')
            print('It costs %f seconds' % (tEnd - tStart))
            # save to excel
            udnCrawler.saveToExcel(fileName='news.xlsx')
            break
        else:
            print('Please input a number!')
