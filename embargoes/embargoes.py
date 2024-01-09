import os

from json import dumps
from pyquery import PyQuery
from requests import Session, Response
from functools import partial
from time import time

from concurrent.futures import ThreadPoolExecutor

from embargoes.helpers import Parser, counter_time

class Embargoes:
    def __init__(self) -> None:
        self.__parser: Parser = Parser()
        self.__requests: Session = Session()

    def __filter_page(self, body: PyQuery, url: str):
        response: Response = self.__requests.get("https://www.sipri.org/databases/embargoes")
        
        with open('test_data.json', 'w') as file:
            file.write(dumps({
                "link": "",
                "domain": "",
                "tag": "",
                "category": "",
                "sub_category": "",
                "description_sub_category": "",
                "topic": "",
                "description_topic": "",
                "year": "",
                "title": "",
                "content": "",
                "crawling_at": "",
                "crawling_time_epoch": int(time())
            }, indent=2))

    @counter_time
    def execute(self) -> None:
        response: Response = self.__requests.get("https://www.sipri.org/databases/embargoes")

        body: PyQuery = self.__parser.execute(response.text, 'body')
        urls: list = [PyQuery(a).attr('href') for a in self.__parser.execute(body, '.views-view-grid.horizontal.cols-6.clearfix a')]

        # for url in urls:
        #     self.__filter_page(body, url)
        
        with ThreadPoolExecutor() as executor:
            executor.map(self.__filter_page, urls)
        

if(__name__ == '__main__'):
    embargoes: Embargoes = Embargoes()
    embargoes.execute()