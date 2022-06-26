import csv


class DatabaseManager:

    def __init__(self, crawler, geo_finder, redis_controller):
        self.crawler = crawler
        self.geo_finder = geo_finder
        self.redis_controller = redis_controller

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