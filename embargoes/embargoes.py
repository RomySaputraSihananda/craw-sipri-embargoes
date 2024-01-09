import os

from json import dumps
from pyquery import PyQuery
from requests import Session, Response
from functools import partial
from time import time

from concurrent.futures import ThreadPoolExecutor

from embargoes.helpers import Parser, Datetime, counter_time

class Embargoes:
    def __init__(self) -> None:
        self.__parser: Parser = Parser()
        self.__datetime: Datetime = Datetime()
        self.__requests: Session = Session()
        self.__url: str = "https://www.sipri.org/databases/embargoes"

    def __filter_page(self, body: PyQuery, url: dict) -> None:
        year: str = list(url.keys())[0]

        response: Response = self.__requests.get(url[year])

        if(response.status_code != 200): return

        content: PyQuery = self.__parser.execute(response.text, 'body') 
        title: str = self.__parser.execute(content, 'h1').text()

        if(not os.path.exists('data')):
            os.makedirs('data')

        
        with open(f'data/{title.lower().replace(" ", "_")}.json', 'w') as file:
            file.write(dumps({
                "link": url[year],
                "domain": self.__url.split('/')[2],
                "tag": self.__url.split('/')[2:],
                "category": "",
                "sub_category": "",
                "description_sub_category": "",
                "topic": "",
                "description_topic": "",
                "year": year,
                "title": title,
                "content": self.__parser.execute(content, '.content .field-item').text(),
                "crawling_at": self.__datetime.now(),
                "crawling_time_epoch": int(time())
            }, indent=2, ensure_ascii=False))

    @counter_time
    def execute(self) -> None:
        response: Response = self.__requests.get(self.__url)

        body: PyQuery = self.__parser.execute(response.text, 'body')
        urls: list = [{PyQuery(a).text(): PyQuery(a).attr('href')} for a in self.__parser.execute(body, '.views-view-grid.horizontal.cols-6.clearfix a')]

        # for url in urls:
        #     self.__filter_page(body, url)
        
        with ThreadPoolExecutor() as executor:
            executor.map(self.__filter_page, body, urls)

        executor.shutdown(wait=True)

if(__name__ == '__main__'):
    embargoes: Embargoes = Embargoes()
    embargoes.execute()