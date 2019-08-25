# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


class DyttPipeline(object):
    def process_item(self, item, spider):
        # print(item["movie"])
        print("*" * 20)
        
        db = pymysql.connect(host="localhost", user="root", password="password", database="world")
        db_curs = db.cursor()
        insert_sql = """INSERT INTO dytt(movie_name, movie_date) VALUES (%s, %s);"""
        data = [item["movie"]]
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
