from crawler import Crawler
from db_manager import DatabaseManager
from db_manager import RedisController
from db_manager import GeoFinder

import os
from multiprocessing import Process


def main(process_num):
    print(process_num)

    crawler = Crawler()

    # geo location finder
    geo_finder = GeoFinder()

    redis_controller = RedisController()

    db_manager = DatabaseManager("AL_00_D171_20220625.csv", crawler,
                                 geo_finder, redis_controller)
    db_manager.process()


if __name__ == "__main__":
    for num in range(os.cpu_count()):
        Process(target=main, args=(str(num))).start()
