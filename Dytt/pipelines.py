# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


class DyttPipeline(object):
    def process_item(self, item, spider):
        # print(item["movie"])
        # print("*" * 100)
        # print(item)
        insert_data = [item["movie_date"], item["movie_name"], item["movie_url"]]
        print(insert_data)
        
        db = pymysql.connect(host="localhost", user="root", password="password", database="world")
        db_curs = db.cursor()
        insert_sql = """INSERT INTO dytt_1(movie_date, movie_name, movie_url) VALUES (%s, %s, %s);"""
        data = [insert_data]
        # print(data)
        try:
            db_curs.executemany(insert_sql, data)
            db.commit()
            print("ok")
        except:
            db.rollback()
            print("insert error")
        db_curs.close()
        print("This is a message")
        return item
