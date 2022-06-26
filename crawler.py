from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

from dotenv import load_dotenv

load_dotenv()


class CrawlerXpathDAO:

    def __init__(self, sido, sigungu, reg_num, search_button,
                 search_result_href):
        self.sido = sido
        self.sigungu = sigungu
        self.reg_num = reg_num
        self.search_button = search_button
        self.search_result_href = search_result_href

    def get_sido_xpath(self):
        return self.sido

    def get_sigungu_xpath(self):
        return self.sigungu

    def get_registration_number_xpath(self):
        return self.reg_num

    def get_search_button_xpath(self):
        return self.search_button

    def get_search_result_href_xpath(self):
        return self.search_result_href


class Crawler:

    def __init__(self, driver, url, xpath_dao):
        self.driver = driver
        self.url = url
        self.xpath_dao = xpath_dao

    def crawling(self, sido, sigungu, reg_num):
        "Crawling real estate agency infos"
        try:
            self.driver.get(self.url)
            self._select_sido(sido)
            self._select_sigungu(sigungu)
            self._put_registration_number(reg_num)
            self._click_search_button()
            self._click_search_result_href()
            self._parse_agency_infos()

        except Exception as error:
            print(error)

    def _select_sido(self, sido):
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, self.xpath_dao.get_sido_xpath()))).send_keys(sido)

    def _select_sigungu(self, sigungu):
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 self.xpath_dao.get_sigungu_xpath()))).send_keys(sigungu)

    def _put_registration_number(self, reg_num):
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, self.xpath_dao.get_registration_number_xpath()
                 ))).send_keys(reg_num)

    def _click_search_button(self):
        # BUG: Sometimes url respond with emtpy data... try again until
        # get real estate agency infos
        while True:
            WebDriverWait(self.driver, 600).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     self.xpath_dao.get_search_button_xpath()))).click()

            element = WebDriverWait(self.driver, 1).until(
                EC.presence_of_element_located(
                    (By.XPATH, self.xpath_dao.get_search_result_href_xpath())))
            if element is not None:
                break

    def _click_search_result_href(self):
        WebDriverWait(self.driver, 600).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 self.xpath_dao.get_search_result_href_xpath()))).click()

    def _parse_agency_infos(self):
        # HTML 구문 분석 & 데이터 추출
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        contents = soup.findAll("td")
        data = [data.text.strip() for data in contents]
        print(data)