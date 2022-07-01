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
        values = (dataset['x'], dataset['y'], dataset['id'])
        self.r.geoadd("agency", values)

    def get_agency_id_and_line_to_process(self):
        with self.r.pipeline() as pipe:
            while True:
                try:
                    # put a WATCH on the key that holds our sequence value
                    pipe.watch('agency_id')
                    pipe.watch('processed_line')
                    # after WATCHing, the pipeline is put into immediate execution
                    # mode until we tell it to start buffering commands again.
                    # this allows us to get the current value of our sequence
                    current_agency_id = pipe.get('agency_id')
                    if current_agency_id is None:
                        current_agency_id = 0
                    next_agency_id = int(current_agency_id) + 1

                    processed_line = pipe.get('processed_line')
                    if processed_line is None:
                        processed_line = 0
                    line_to_process = int(processed_line) + 1
                    # now we can put the pipeline back into buffered mode with MULTI
                    pipe.multi()
                    pipe.set('agency_id', next_agency_id)
                    pipe.set('processed_line', line_to_process)
                    # and finally, execute the pipeline (the set command)
                    pipe.execute()
                    # if a WatchError wasn't raised during execution, everything
                    # we just did happened automically.
                    break

                except redis.WatchError:
                    # another client must have changed 'agency_id' between
                    # the time we started WATCHing it and the pipeline's exception.
                    # our best bet is to just retry.
                    continue
                finally:
                    pipe.reset()
            return (next_agency_id, line_to_process)


# 카카오 REST API를 이용하여 (위/경도) 값을 얻어옴
class GeoFinder:

    def __init__(self):
        self.url = os.environ['KAKAO_GEO_SEARCH_URL']
        self.cached_latlng = {}
        self.headers = {
            "Authorization": "KakaoAK " + os.environ['KAKAO_REST_API_KEY']
        }

    def get_latlng(self, address):
        while True:
            params = {'query': address}
            response = requests.get(self.url,
                                    params=params,
                                    headers=self.headers).json()
            if response['documents'] is None:
                return (None, None)
            doc = response['documents']

            # Search lat,lng until get find nearest
            if len(doc) == 0:
                address = str(address.rpartition(" ")[0])
            else:
                lat = doc[0]['y']
                lng = doc[0]['x']
                if self.is_cached(lat, lng) == True:
                    print("Exact same (lat/lng), move a little bit to top...")
                    lat = str(float(lat) - 0.00002)

                self.cache_latlng(lat, lng)
                return (doc[0]['y'], doc[0]['x'])

    def is_cached(self, lat, lng):
        try:
            is_duplicated = self.cached_latlng[lat] == lng
            return is_duplicated
        except KeyError:
            return False

    def cache_latlng(self, lat, lng):
        self.cached_latlng[lat] = lng


class DatabaseManager:

    def __init__(self, file_name, crawler, geo_finder, redis_controller):
        self.file_name = file_name
        self.crawler = crawler
        self.geo_finder = geo_finder
        self.redis_controller = redis_controller

    # '국가공간포털'에서 제공하는 db(csv 파일)를 읽고
    # '국가공간포털'에서 해당 부동산 정보를 crawling 한다.
    # '국가공간포털'에서는 (위/경도) 값을 제공하지 않기 때문에
    # 카카오 REST API를 이용하여 (위/경도)를 받아온 후,
    # redis server에 hashmap으로 저장한다
    def process(self):
        while True:
            (agency_id, line_to_process
             ) = self.redis_controller.get_agency_id_and_line_to_process()
            print(agency_id, line_to_process)
            if line_to_process >= 119347:
                break

            database = open(self.file_name, "r", encoding="cp949")
            reader = csv.reader(database)

            for i, row in enumerate(reader):
                if i == line_to_process:
                    line = row
            reg_num = line[2]
            print(line)

            sido_sigungu = line[1].split(" ")
            if len(sido_sigungu) == 1:
                sido = sido_sigungu[0]
                sigungu = None
            elif len(sido_sigungu) == 2:
                sido = sido_sigungu[0]
                sigungu = sido_sigungu[1]
            elif len(sido_sigungu) == 3:
                sido = sido_sigungu[0]
                sigungu = sido_sigungu[1] + sido_sigungu[2]
            else:
                print("Invalid CSV...")
                continue

            dataset = self.crawler.crawling(sido, sigungu, reg_num)
            if dataset == "not_in_service":
                # 휴업중
                print("현재 부동산이 휴업중...")
                continue

            # 국가공간포털은 위/경도를 반환하지 않기 때문에
            # 카카오 REST API를 이용하여 위/경도 값을 받아옴
            (lat, lng) = self.geo_finder.get_latlng(dataset['소재지'])
            if lat is None or lng is None:
                print("The api max request counts exceed...")
                break
            dataset["y"] = lat
            dataset["x"] = lng
            dataset["id"] = agency_id

            self.redis_controller.save_real_estate_agency(dataset)

            database.close()
