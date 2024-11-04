from typing import Any

import requests

from src.consts import locationindex_type
from src.process_methods.abstract_method import IterationMethod
from src.status import MonthDatasetStatus

from src.util import get_post_text, find_url

class StatsCollectionMethod(IterationMethod):

    @staticmethod
    def name() -> str:
        pass

    def follow_url(self, url):
        try:
            response = requests.get(url, allow_redirects=True, timeout=10)
            return response.url  # This is the final URL after all redirects

            # If you want full redirect history:
            # return response.history  # List of all redirects
            # return [h.url for h in response.history] + [response.url]  # All URLs in chain
        except requests.RequestException as e:
            return f"Error: {e}"

    def _process_data(self, post_data: dict, location_index: locationindex_type) -> Any:
        urls = find_url(get_post_text(post_data))
        endpoints = [self.follow_url(url) for url in urls]
        return endpoints

    # async def get_final_url_async(url):
    #     async with aiohttp.ClientSession() as session:
    #         async with session.get(url, allow_redirects=True) as response:
    #             return str(response.url)

    def finalize(self):
        pass

    def set_ds_status_field(self, status: MonthDatasetStatus) -> None:
        pass

    def print_outputs(self):
        pass