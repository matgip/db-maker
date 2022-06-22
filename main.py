
import csv
import json
import codecs
import requests

url = 'https://dapi.kakao.com/v2/local/search/keyword.json'
headers = {"Authorization": "KakaoAK b912a737eb064082f1aff5d3600bc2be"}


def search_by_keyword(keyword):
    params = {'query': keyword}
    return requests.get(url, params=params, headers=headers).json()['documents']


def fetch(file_name):
    file = open(file_name, "r", encoding="cp949")
    output = codecs.open("results.txt", "w", encoding="cp949")
    no_result = codecs.open("no-results.txt", "w", encoding="cp949")
    reader = csv.reader(file)

    for line in reader:
        keyword = line[1] + " " + line[3]
        places = search_by_keyword(keyword)

        if len(places) == 0:
            no_result.write(keyword + "검색 결과 없음!\n")
            continue

        output.write(keyword + " RESULTS: ")
        json.dump(places[0], output, ensure_ascii=False)
        output.write("\n")

    file.close()
    output.close()
    no_result.close()


def save_to_redis(file_name):
    file = open(file_name, "r", encoding="cp949")
    lines = file.readlines()

    for l in lines:
        place = eval(l)
        print(place["address_name"])


def main():
    fetch("real_estate_agencies.csv")
    # save_to_redis("test.csv")


if __name__ == "__main__":
    main()
