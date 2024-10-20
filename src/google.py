import requests
import os
from http_client import HttpClient


class GeocodingClient:
    def __init__(self, http_client: HttpClient, url: str, key: str):
        self.http_client = http_client.get_client()
        self.__url = url
        self.__apikey = key

    def geocode(self, country: str, area: str, location: str):
        filtered_address = [x for x in [location, area, country] if x is not None]
        address = ", ".join(filtered_address)

        response = self.http_client.get(
            self.__url, params={"key": self.__apikey, "address": address}
        )
        if not response.ok:
            raise "Error in geocoding service"

        body = response.json()
        coords = body["results"][0]["geometry"]["location"]
        return {"latitude": coords["lat"], "longitude": coords["lng"]}
