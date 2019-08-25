# -*- coding: utf-8 -*-
import scrapy
from Dytt.items import DyttItem
# from Dytt.settings import USER_AGENT


class DyttSpider(scrapy.Spider):
    name = 'dytt'
    allowed_domains = ['dytt8.net']
    start_urls = ['https://www.dytt8.net/html/gndy/dyzz/index.html']

    def parse(self, response):
        # print(response.text)
        # url_if = response.xpath("//div[@class='x']//a[text()='下一页']/@href").extract() # 做判断用
        url = response.xpath("//div[@class='x']//a[text()='下一页']/@href").extract() # 提取url地址用
        ul_num = len(response.xpath("//div[@class='co_content8']//ul//table"))
        movie_name_num = len(response.xpath("//div[@class='co_content8']//ul//table//tr[2]//td[2]//b//a/text()").extract())
        movie_date_num = len(response.xpath("//div[@class='co_content8']//ul//table//tr[3]//td[2]//font/text()").extract())

        if ul_num == movie_name_num == movie_date_num:
            page_list = response.xpath("//div[@class='co_content8']//ul//table")
            # print(page_list)
            for i in page_list:
                item = DyttItem()
                item["movie"] = [i.xpath(".//tr[2]//td[2]//b//a/text()").extract()[0],
                                i.xpath(".//tr[3]//td[2]//font/text()").extract()[0].replace('\r\n', '')]

                # yield item
                print(str(url)[1:-1] + ':ok')
                
        else:
            print(str(url)+':error')
 
        # print(next_url)
        if len(url) != 0: # 如果这里运行错误，那就取消掉第14行的注释，然后将本行的len(url)替换为len(url_if)
            next_url = "https://www.dytt8.net/html/gndy/dyzz/" + url[0]
            yield scrapy.Request(
                next_url,
                callback=self.parse
            )
        else:
           print('END')
         