import scrapy
from ..items import BooksTutoItem
class BooksSpider(scrapy.Spider):
    name = "books"
    start_urls = [
        'http://books.toscrape.com/'
    ]
    def parse(self, response):
        all= response.css('article.product_pod')
        items= BooksTutoItem()
        for i in all:
            title= i.css('h3 a::attr(title)').extract()
            price= i.css('p.price_color::text').extract()
            rating= i.css('p.star-rating::attr(class)').extract()
            items['title']=title
            items['price']=price
            items['rating']=rating
            yield items
        next_page= response.css('li.next a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
