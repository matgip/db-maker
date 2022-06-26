import requests

import os
from dotenv import load_dotenv

load_dotenv()


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
