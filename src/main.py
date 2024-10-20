import pandas as pd
from dotenv import load_dotenv
import json
import re
from datetime import datetime
import numpy as np
import os
from google import GeocodingClient
from openmeteo import OpenMeteoClient
from http_client import HttpClient


class TCC:
    date_threshold = datetime(1941, 1, 1)
    __data_path = "./data/GSAF5.xls - Sheet1-GSAF.csv"
    __date_regex = r"\d{4}\.\d{2}\.\d{2}"
    __time_regex = r"\d{2}h\d{2}"

    def __init__(self):
        http_client = HttpClient()

        geocoding_service_url = os.getenv("GEOCODING_URL")
        geocoding_apikey = os.getenv("GEOCODING_API_KEY")
        self.__geocoding_client = GeocodingClient(
            http_client, geocoding_service_url, geocoding_apikey
        )

        openmeteo_url = os.getenv("OPEN_METEO_URL")
        self.__openmeteo_client = OpenMeteoClient(http_client, openmeteo_url)

    def main(self):
        df = pd.read_csv(self.__data_path).replace({np.nan: None})

        processed_data = [
            self.__process_row(case_number, country, area, location, time)
            for case_number, country, area, location, time in zip(
                df["Case Number"], df["Country"], df["Area"], df["Location"], df["Time"]
            )
        ]
        filtered_data = [data for data in processed_data if data is not None]

        print("%d total events" % len(df))
        print("%d valid events" % len(filtered_data))
        print("%d invalid events" % (len(processed_data) - len(filtered_data)))

        with open("shark_incidents.json", "w") as file:
            json.dump(
                filtered_data,
                file,
                ensure_ascii=False,
                indent=True,
                allow_nan=False,
            )

    def __process_row(self, case_number, country, area, location, time):
        if not case_number or not country or not time or not location:
            print("Case number %s is missing important data." % case_number)
            return

        date_result = re.search(self.__date_regex, str(case_number))
        time_result = re.search(self.__time_regex, str(time))
        if not date_result or not time_result:
            print("Case number %s is missing time data." % case_number)
            return

        date = None
        try:
            date = datetime.strptime(
                f"{date_result[0]} {time_result[0]}", "%Y.%m.%d %Hh%M"
            )
        except ValueError as e:
            print("parse error:", e)
            print("Case number %s has invalid time data." % case_number)
            return

        if date < self.date_threshold:
            print("Case number %s is too old for OpenMeteo." % case_number)
            return

        print("Fetching coordinates for case number %s." % case_number)
        coordinates = self.__geocoding_client.geocode(country, area, location)
        print(
            "Got coordinates for case number %s. Lat: %f, Long: %f"
            % (case_number, coordinates["latitude"], coordinates["longitude"])
        )

        print("Fetching weather data for case number %s." % case_number)
        weather = self.__openmeteo_client.get_hourly_weather(
            date, coordinates["latitude"], coordinates["longitude"]
        )

        if weather is None:
            print("Could not get weather data for case number %s." % case_number)
        print("Got weather data for case number %s." % case_number)

        return {
            "case_number": case_number,
            "country": country,
            "area": area,
            "location": location,
            "datetime": date.isoformat(timespec="milliseconds"),
            "coordinates": coordinates,
            "weather": weather,
        }


if __name__ == "__main__":
    load_dotenv(".env")
    TCC().main()
