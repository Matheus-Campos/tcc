import requests
import os
from datetime import datetime, timedelta


class StormglassClient:
    __service_url = os.getenv("STORMGLASS_URL")
    __default_weather_params = [
        "airTemperature",
        "precipitation",
        "seaLevel",
        "pressure",
        "waterTemperature",
        "waveHeight",
        "wavePeriod",
        "windSpeed",
    ]

    def __init__(self, key: str):
        self.__apikey = key

    def get_tide_extremes(self, date: datetime, latitude: float, longitude: float):
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date + timedelta(hours=23, minutes=59, seconds=59)

        response = requests.get(
            f"{self.__service_url}/tide/extremes/point",
            params={
                "lat": latitude,
                "lng": longitude,
                "start": start_of_day.timestamp(),
                "end": end_of_day.timestamp(),
            },
            headers={"Authorization": self.__apikey},
        )

        if not response.ok:
            print("Error while requesting tide level: ", response.json())
            return

        body = response.json()
        return body["data"]

    def get_weather(self, date: datetime, latitude: float, longitude: float):
        start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = date + timedelta(hours=23, minutes=59, seconds=59)

        response = requests.get(
            f"{self.__service_url}/weather/point",
            params={
                "lat": latitude,
                "lng": longitude,
                "params": ",".join(self.__default_weather_params),
                "start": start_of_day.timestamp(),
                "end": end_of_day.timestamp(),
            },
            headers={"Authorization": self.__apikey},
        )

        if not response.ok:
            print("Error while requesting weather from stormglass: ", response.json())
            return

        body = response.json()
        return body["hours"]
