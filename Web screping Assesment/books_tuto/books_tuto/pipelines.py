# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymysql
import logging

class BooksTutoPipeline:
    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        try:
            self.conn = pymysql.connect(
                host='localhost',
                user='root',
                password='Abhi@1133',
                database='ds',
            )
            self.curr = self.conn.cursor()
            logging.info("MySQL connection established successfully.")
        except Exception as e:
            logging.error(f"Error connecting to MySQL: {e}")
            raise e

    def create_table(self):
        try:
            self.curr.execute("""
                CREATE TABLE IF NOT EXISTS b_tb (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title TEXT,
                    price VARCHAR(255),
                    rating VARCHAR(255)
                )
            """)
            self.conn.commit()
            logging.info("Table created or already exists.")
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            raise e

    def process_item(self, item, spider):
        try:
            logging.info(f"Processing item: {item}")  # Logging to see the data being processed

            sql_query = """
                INSERT INTO b_tb (title, price, rating)
                VALUES(%s, %s, %s)
            """
            values = (
                item['title'] if item['title'] else '',
                item['price'] if item['price'] else '',
                item['rating'] if item['rating'] else ''
            )

            logging.info(f"Executing SQL: {sql_query}, with values: {values}")

            self.curr.execute(sql_query, values)
            self.conn.commit()  # Commit the transaction
            logging.info(f"Item inserted: {item['title']}")
        except pymysql.MySQLError as e:
            logging.error(f"Error inserting item {item['title']}: {e}")
            self.conn.rollback()  # In case of error, rollback the transaction

        return item

    def close_spider(self, spider):
        try:
            self.conn.close()
            logging.info("MySQL connection closed.")
        except Exception as e:
            logging.error(f"Error closing MySQL connection: {e}")
