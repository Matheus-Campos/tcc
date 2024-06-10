import pandas as pd
from dotenv import load_dotenv
import json
import re
from datetime import datetime as dt
import numpy as np
import os

load_dotenv(".env")

from geocoding import GeocodingService


class TCC:
    __date_regex = r"\d{4}\.\d{2}\.\d{2}"
    __time_regex = r"\d{2}h\d{2}"

    def __init__(self):
        geocoding_apikey = os.getenv("GEOCODING_API_KEY")
        self.__geocoding_service = GeocodingService(geocoding_apikey)

    def main(self):
        df = (
            pd.read_csv("./data/GSAF5.xls - Sheet1-GSAF.csv")
            .replace({np.nan: None})
            .head()
        )

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
            return

        date_result = re.search(self.__date_regex, str(case_number))
        time_result = re.search(self.__time_regex, str(time))
        if not date_result or not time_result:
            return

        try:
            datetime = dt.strptime(
                f"{date_result[0]} {time_result[0]}", "%Y.%m.%d %Hh%M"
            )
        except ValueError as e:
            return print("parse error:", e)

        coordinates = self.__geocoding_service.geocode(country, area, location)

        return {
            "case_number": case_number,
            "country": country,
            "area": area,
            "location": location,
            "datetime": datetime.isoformat(timespec="milliseconds"),
            "coordinates": coordinates,
        }


if __name__ == "__main__":
    TCC().main()
