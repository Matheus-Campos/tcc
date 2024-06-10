import requests
import os


class GeocodingClient:
    __service_url: str = os.getenv("GEOCODING_URL")

    def __init__(self, key: str):
        self.__apikey = key

    def geocode(self, country: str, area: str, location: str):
        filtered_address = [x for x in [location, area, country] if x is not None]
        address = ", ".join(filtered_address)

        response = requests.get(
            self.__service_url, params={"key": self.__apikey, "address": address}
        )
        if not response.ok:
            raise "Error in geocoding service"

        body = response.json()
        coords = body["results"][0]["geometry"]["location"]
        return {"latitude": coords["lat"], "longitude": coords["lng"]}
