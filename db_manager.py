import csv
import redis
import requests

import os
from dotenv import load_dotenv

load_dotenv()

# Connect to redis server
pool = redis.ConnectionPool(host=os.environ['REDIS_DB_HOST'],
                            port=os.environ['REDIS_DB_PORT'],
                            db=os.environ['REDIS_DB_INDEX'])


class RedisController:

    def __init__(self):
        self.r = redis.Redis(connection_pool=pool)

    def save_real_estate_agency(self, dataset):
        self.r.hmset(
            "agency:" + str(dataset["id"]), {
                "id": dataset["id"],
                "y": dataset["y"],
                "x": dataset["x"],
                "phone": dataset['전화번호'],
                "place_name": dataset['상호'],
                "address_name": dataset['소재지'],
                "owner": dataset['대표자'],
                "varified": dataset['보증보험유무']
            })


# 카카오 REST API를 이용하여 (위/경도) 값을 얻어옴
class GeoFinder:

    def __init__(self):
        self.url = os.environ['KAKAO_GEO_SEARCH_URL']
        self.headers = {
            "Authorization": "KakaoAK " + os.environ['KAKAO_REST_API_KEY']
        }

    def get_latlng(self, address):
        while True:
            params = {'query': address}
            response = requests.get(self.url,
                                    params=params,
                                    headers=self.headers).json()['documents']
            # Search lat,lng until get find nearest
            if len(response) == 0:
                address = str(address.rpartition(" ")[0])
            else:
                return (response[0]['y'], response[0]['x'])


class DatabaseManager:

    def __init__(self, crawler, geo_finder, redis_controller):
        self.crawler = crawler
        self.geo_finder = geo_finder
        self.redis_controller = redis_controller

    # '국가공간포털'에서 제공하는 db(csv 파일)를 읽고
    # '국가공간포털'에서 해당 부동산 정보를 crawling 한다.
    # '국가공간포털'에서는 (위/경도) 값을을 제공하지 않기 때문에
    # 카카오 REST API를 이용하여 (위/경도)를 받아온 후,
    # redis server에 hashmap으로 저장한다
    def process(self, file_name):
        database = open(file_name, "r", encoding="cp949")
        reader = csv.reader(database)

        agency_id = 1
        for line in reader:
            reg_num = line[2]

            sido_sigungu = line[1].split(" ")
            if len(sido_sigungu) != 2:
                print("Invalid CSV file format...")
                continue
            sido = sido_sigungu[0]
            sigungu = sido_sigungu[1]

            dataset = self.crawler.crawling(sido, sigungu, reg_num)

            # 국가공간포털은 위/경도를 반환하지 않기 때문에
            # 카카오 REST API를 이용하여 위/경도 값을 받아옴
            latlng = self.geo_finder.get_latlng(dataset['소재지'])
            dataset["y"] = latlng[0]
            dataset["x"] = latlng[1]
            dataset["id"] = agency_id

            self.redis_controller.save_real_estate_agency(dataset)

            agency_id += 1

        database.close()