from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

import os
from dotenv import load_dotenv

load_dotenv()

driver = webdriver.Chrome(os.environ['CHROME_DRIVER_PATH'])
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
    driver.get(real_estate_agency_search_url)
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, sido_search_box_xpath))).send_keys("서울특별시")

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH, sigungu_search_box_xpath))).send_keys("강동구")

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             registration_number_input_xpath))).send_keys("11740-2016-00185")

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, search_button_xpath))).click()

    WebDriverWait(driver, 120).until(
        EC.element_to_be_clickable(
            (By.XPATH, search_result_href_xpath))).click()

    # HTML 구문 분석 & 데이터 추출
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    contents = soup.findAll("td")
    data = [data.text.strip() for data in contents]
    print(data)


if __name__ == "__main__":
    main()
