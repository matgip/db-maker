from selenium import webdriver

import os
from dotenv import load_dotenv

from crawler import Crawler, CrawlerXpathDAO
from db_manager import DatabaseManager
from db_manager import RedisController
from db_manager import GeoFinder

load_dotenv()

driver = webdriver.Chrome(os.environ['CHROME_DRIVER_PATH'])
# Crawling url path
real_estate_agency_search_url = os.environ['REAL_ESTATE_AGENCY_SEARCH_URL']

# 시/도 선택 search box xpath
sido_search_box_xpath = '//*[@id="shSido"]'
# 시/군/구 선택 search box xpath
sigungu_search_box_xpath = '//*[@id="shSigungu"]'
# 등록번호
registration_number_input_xpath = '//*[@id="shWord1"]'
# 검색 버튼
search_button_xpath = '//*[@id="icon_btn_write"]'
# 검색 결과 하이퍼 링크 버튼
search_result_href_xpath = '//*[@id="searchVO"]/div[2]/table/tbody/tr/td[3]/a'


def main():
    # web crawler
    crawler_xpath_dao = CrawlerXpathDAO(sido_search_box_xpath,
                                        sigungu_search_box_xpath,
                                        registration_number_input_xpath,
                                        search_button_xpath,
                                        search_result_href_xpath)
    crawler = Crawler(driver, real_estate_agency_search_url, crawler_xpath_dao)

    # geo location finder
    geo_finder = GeoFinder()

    redis_controller = RedisController()

    db_manager = DatabaseManager(crawler, geo_finder, redis_controller)
    db_manager.process("AL_00_D171_20220625.csv")


if __name__ == "__main__":
    main()
