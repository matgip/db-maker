from crawler import Crawler
from db_manager import DatabaseManager
from db_manager import RedisController
from db_manager import GeoFinder


def main():
    crawler = Crawler()

    # geo location finder
    geo_finder = GeoFinder()

    redis_controller = RedisController()

    db_manager = DatabaseManager(crawler, geo_finder, redis_controller)
    db_manager.process("AL_00_D171_20220625.csv")


if __name__ == "__main__":
    main()
