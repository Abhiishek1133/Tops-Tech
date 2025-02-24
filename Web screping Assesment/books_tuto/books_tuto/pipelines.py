# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import pymysql
from itemadapter import ItemAdapter

class BooksTutoPipeline:
    def __init__(self):
        self.create_connection()
        self.create_table()


    def create_connection(self):
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='Abhi@1133',
            database='ad',
        )
        self.curr = self.conn.cursor()
   
    def create_table(self):
        self.curr.execute("""
            CREATE TABLE IF NOT EXISTS b_tb (
              id INT AUTO_INCREMENT PRIMARY KEY,
              title TEXT,
              price VARCHAR(255),
              rating TEXT
            )
         """)
        self.conn.commit()
   
    def process_item(self, item, spider):
        self.curr.execute("""
             INSERT INTO b_tb (title, price, rating)
             VALUES(%s, %s, %s)
        """, (
              item['title'][0] if item['title'] else '',
              item['price'][0] if item['price'] else '',
              item['rating'][0] if item['rating'] else ''
        ))
        self.conn.commit()
        return item
   
    def close_spider(self, spider):
        self.conn.close()

