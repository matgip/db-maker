import csv


class DatabaseManager:

    def __init__(self, crawler, geo_finder):
        self.crawler = crawler
        self.geo_finder = geo_finder

    def process(self, file_name):
        database = open(file_name, "r", encoding="cp949")
        reader = csv.reader(database)

        for line in reader:
            reg_num = line[2]

            sido_sigungu = line[1].split(" ")
            if len(sido_sigungu) != 2:
                print("Invalid CSV file format...")
                continue
            sido = sido_sigungu[0]
            sigungu = sido_sigungu[1]

            dataset = self.crawler.crawling(sido, sigungu, reg_num)
            latlng = self.geo_finder.get_latlng(dataset['소재지'])
            print(latlng)

        database.close()