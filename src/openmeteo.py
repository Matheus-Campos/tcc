import requests
import os
from datetime import datetime


class OpenMeteoClient:
    __service_url = os.getenv("OPEN_METEO_URL")
    __default_hourly_params = [
        "temperature_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
    ]

    def get_hourly_weather(self, date: datetime, latitude: float, longitude: float):
        response = requests.get(
            self.__service_url,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "start_date": date.strftime("%Y-%m-%d"),
                "end_date": date.strftime("%Y-%m-%d"),
                "hourly": ",".join(self.__default_hourly_params),
            },
        )

        if not response.ok:
            print("Error while requesting from OpenMeteo", response.json())
            return

        body = response.json()
        return self.__zip_hourly_data(body["hourly"])

    def __zip_hourly_data(self, data):
        return [
            {
                "time": time,
                "temperature_2m": temperature_2m,
                "precipitation": precipitation,
                "rain": rain,
            }
            for time, temperature_2m, precipitation, rain in zip(
                data["time"],
                data["temperature_2m"],
                data["precipitation"],
                data["rain"],
            )
        ]
