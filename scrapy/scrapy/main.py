# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from threading import Thread
from bs4 import BeautifulSoup
import re
import request
import json
import pymysql
import urllib2
'''本爬虫目标爬取bilibili两千多部纪录片的url和名称;
   结构上分为五个部分：调度器(bilibili_spider)，URL池(url_pool)，下载器(html_download)，解析器(url_get)， 数据库交互(UrlToMySQL).'''


FARTHER_URL = 'https://bangumi.bilibili.com/media/web_api/search/result?style_id=-1&producer_id=-1&order=2&st=3&sort=0\
               &page=%s&season_type=3&pagesize=20'  # 目标url

CHILD_URL_NUMBER = 100  # 爬取页数

MYSQL_CONNECT = {                # MySQL连接参数
        'database': 'html_data',
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'charset': 'utf8mb4'
    }


class UrlToMySQL(object):
    # 数据库交互模型，封装了增删查的方法，生成实例时自动初始化连接

    def __init__(self):
        self.connection = pymysql.connect(database=MYSQL_CONNECT['database'],
                                          host=MYSQL_CONNECT['host'],
                                          port=MYSQL_CONNECT['port'],
                                          user=MYSQL_CONNECT['user'],
                                          password=MYSQL_CONNECT['password'],
                                          charset=MYSQL_CONNECT['charset'])
        self.cursor = self.connection.cursor()

    def url_insert(self, url_content=None, tag=None):
        # 数据库插入接口,接收纪录片url(url_content)和名称(tag)
        insert_sql = '''INSERT INTO htmls (html_content, tag) VALUES ("{v1}", "{v2}");'''.format(v1=url_content, v2=tag)
        print 'good job!' + insert_sql
        self.cursor.execute(insert_sql)

    def url_get(self, url_id=None):
        # 数据库查询接口,接收纪录片id，返回查询结果
        get_sql = '''SELECT html_id, html_content, tag FROM htmls WHERE html_id = {v1};'''.format(v1=url_id)
        html = self.cursor.execute(get_sql)
        return html

    def url_delete(self, url_id):
        # 向数据库删除接口
        delete_sql = '''DELETE FROM htmls where html_id = {v1};'''.format(v1=url_id)
        self.cursor.execute(delete_sql)


def url_pool(father_url):
    # url池，存储未爬取的url以列表形式返回
    waiting_pool = []  # 待爬取的url池
    [waiting_pool.append(father_url % str(url)) for url in range(CHILD_URL_NUMBER)]  # 将自增的页码放入url中并添加到url池
    return waiting_pool


def url_get(json_data):
    # url解析器，接收json数据，解析出目标url，以列表形式返回
    data_dict = json.loads(json_data)
    data_list = data_dict['result']['data']
    return data_list


def html_download(url):
    # html下载器，返回json数据
    html_content = urllib2.urlopen(url).read()
    return html_content


def bilibili_spider():
    # 调度器，控制程序各部分运行
    sql_cursor = UrlToMySQL()
    urls = url_pool(FARTHER_URL)
    # 循环访问每个url，得到页面json数据，再遍历json数据，得到目标url和name
    for url in urls:

        html_content = html_download(url)
        html_number = urls.index(url) + 1  # 打印页码
        data_list = url_get(html_content)
        for data in data_list:

            target_url = data['link']
            target_name = data['title']
            sql_cursor.url_insert(target_url, target_name)
            sql_cursor.connection.commit()

        print 'page %s had done.' % html_number


if __name__ == '__main__':

    bilibili_spider()
