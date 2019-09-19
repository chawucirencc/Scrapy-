# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import redis


class DyttPipeline(object):
    def process_item(self, item, spider):
        """
        写入到 mysql 数据库中...
        """
        insert_data = [item["movie_date"], item["movie_name"], item["movie_url"]]
        # print(insert_data)
        db = pymysql.connect(host="localhost", user="root", password="password", database="world")
        db_curs = db.cursor()
        insert_sql = """INSERT INTO dytt_1(movie_date, movie_name, movie_url) VALUES (%s, %s, %s);"""
        data = [insert_data]

        try:
            db_curs.executemany(insert_sql, data)
            db.commit()
            # print("ok")
        except Exception as err:
            db.rollback()
            print(err)
        select_sql = 'select movie_name FROM dytt_1'
        db_curs.execute(select_sql)
        result = db_curs.fetchall()
        print(len(result))
        db_curs.close()
        return item


class DyttPipeline_redis(object):
    def process_item(self, item, spider):
        """
        写入到 redis 数据库中...
        使用 key: [list] 的形式。可能存在重复数据。
        """
        # insert_data = [item["movie_date"], item["movie_name"], item["movie_url"]]
        # print(insert_data)
        pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
        r = redis.Redis(connection_pool=pool)
        r.lpush('movie_date', item['movie_date'])              
        # print(len(r.lrange('movie_date', 0, -1)))
        r.lpush('movie_name', item['movie_name'])        
        # print(len(r.lrange('movie_name', 0, -1)))
        r.lpush('movie_url', item['movie_url'])
        print(len(r.lrange('movie_url', 0, -1)))
        return item
