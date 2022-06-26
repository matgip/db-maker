import redis

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

    def save(self, dataset):
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
