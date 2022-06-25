import redis
import csv
import json
import codecs
import requests

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

import os
from dotenv import load_dotenv

load_dotenv()

url = os.environ['KAKAO_SEARCH_URL']
headers = {"Authorization": "KakaoAK " + os.environ['KAKAO_REST_API_KEY']}

# Connect to redis server
pool = redis.ConnectionPool(host=os.environ['REDIS_DB_HOST'],
                            port=os.environ['REDIS_DB_PORT'],
                            db=os.environ['REDIS_DB_INDEX'])
r = redis.Redis(connection_pool=pool)

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


def search_by_keyword(keyword):
    params = {'query': keyword}
    return requests.get(url, params=params,
                        headers=headers).json()['documents']


def crawling_real_estate_agency_infos(sido, sigungu, reg_num):
    driver.get(real_estate_agency_search_url)
    WebDriverWait(driver, 180).until(
        EC.element_to_be_clickable(
            (By.XPATH, sido_search_box_xpath))).send_keys(sido)

    WebDriverWait(driver, 180).until(
        EC.element_to_be_clickable(
            (By.XPATH, sigungu_search_box_xpath))).send_keys(sigungu)

    WebDriverWait(driver, 180).until(
        EC.element_to_be_clickable(
            (By.XPATH, registration_number_input_xpath))).send_keys(reg_num)

    # BUG: Sometimes url respond with emtpy data... try again until
    # get real estate agency infos
    while True:
        WebDriverWait(driver, 600).until(
            EC.element_to_be_clickable(
                (By.XPATH, search_button_xpath))).click()

        element = WebDriverWait(driver, 1).until(
            EC.presence_of_element_located(
                (By.XPATH, search_result_href_xpath)))
        # If empty reply, try again
        if element is not None:
            break

    WebDriverWait(driver, 600).until(
        EC.element_to_be_clickable(
            (By.XPATH, search_result_href_xpath))).click()

    # HTML 구문 분석 & 데이터 추출
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    contents = soup.findAll("td")
    data = [data.text.strip() for data in contents]
    print(data)


def make_database(file_name):
    real_estate_db = open(file_name, "r", encoding="cp949")
    # results_file = codecs.open("results.txt", "a", encoding="cp949")
    # no_results_file = codecs.open("no-results.txt", "a", encoding="cp949")
    reader = csv.reader(real_estate_db)

    for line in reader:
        sidogungu = line[1].split(" ")
        if len(sidogungu) != 2:
            print("Invalid CSV file format...")
            continue

        reg_num = line[2]
        sido = sidogungu[0]
        sigungu = sidogungu[1]
        crawling_real_estate_agency_infos(sido, sigungu, reg_num)
        # keyword = line[1] + " " + line[3]
        # places = search_by_keyword(keyword)

        # if len(places) == 0:
        #     no_results_file.write(keyword + "검색 결과 없음!\n")
        #     continue

        # results_file.write(keyword + " RESULTS: ")
        # json.dump(places[0], results_file, ensure_ascii=False)
        # results_file.write("\n")

    real_estate_db.close()
    # results_file.close()
    # no_results_file.close()


def save_to_redis(file_name):
    results_file = open(file_name, "r", encoding="cp949")
    lines = results_file.readlines()

    for l in lines:
        infos = eval(l.split(' RESULTS: ')[1])
        r.hmset(
            "agency:" + infos['id'], {
                "id": infos['id'],
                "y": infos['y'],
                "x": infos['x'],
                "phone": infos['phone'],
                "place_name": infos['place_name'],
                "address_name": infos['address_name'],
                "road_address_name": infos['road_address_name']
            })
        values = (infos['x'], infos['y'], infos['id'])
        r.geoadd("agency", values)


def main():
    make_database("AL_00_D171_20220625.csv")
    # save_to_redis("results.txt")


if __name__ == "__main__":
    main()
