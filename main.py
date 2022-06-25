import redis
import csv
import json
import codecs
import requests

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


def search_by_keyword(keyword):
    params = {'query': keyword}
    return requests.get(url, params=params,
                        headers=headers).json()['documents']


def fetch(file_name):
    real_estate_db = open(file_name, "r", encoding="cp949")
    results_file = codecs.open("results.txt", "a", encoding="cp949")
    no_results_file = codecs.open("no-results.txt", "a", encoding="cp949")
    reader = csv.reader(real_estate_db)

    for line in reader:
        keyword = line[1] + " " + line[3]
        places = search_by_keyword(keyword)

        if len(places) == 0:
            no_results_file.write(keyword + "검색 결과 없음!\n")
            continue

        results_file.write(keyword + " RESULTS: ")
        json.dump(places[0], results_file, ensure_ascii=False)
        results_file.write("\n")

    real_estate_db.close()
    results_file.close()
    no_results_file.close()


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
    fetch("real_estate_agencies.csv")
    save_to_redis("results.txt")


if __name__ == "__main__":
    main()
