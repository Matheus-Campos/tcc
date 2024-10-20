import threading
import concurrent.futures
from dotenv import load_dotenv
import requests
import pandas as pd
import numpy as np
import re
from datetime import datetime
import os
import json
from time import sleep

thread_local = threading.local()

data_path = "./data/GSAF5.xls - Sheet1-GSAF.csv"

date_threshold = datetime(1941, 1, 1)

date_regex = r"\d{4}\.\d{2}\.\d{2}"
time_regex = r"\d{2}h\d{2}"

default_hourly_params = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
]

num_threads = 16


def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.sessions.Session()
    return thread_local.session


def get_lat_long(country: str, area: str, location: str):
    filtered_address = [x for x in [location, area, country] if x is not None]
    address = ", ".join(filtered_address)

    url = os.getenv("GEOCODING_URL")
    apikey = os.getenv("GEOCODING_API_KEY")
    response = get_session().get(url, params={"key": apikey, "address": address})
    if not response.ok:
        raise "Error in geocoding service"

    body = response.json()
    coords = body["results"][0]["geometry"]["location"]
    return {"latitude": coords["lat"], "longitude": coords["lng"]}


def get_hourly_weather(date: datetime, latitude: float, longitude: float):
    url = os.getenv("OPEN_METEO_URL")
    response = get_session().get(
        url,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "start_date": date.strftime("%Y-%m-%d"),
            "end_date": date.strftime("%Y-%m-%d"),
            "hourly": ",".join(default_hourly_params),
        },
    )

    if not response.ok:
        print("Error while requesting from OpenMeteo", response.json())
        return

    body = response.json()
    return [
        {
            "time": time,
            "temperature_2m": temperature_2m,
            "precipitation": precipitation,
            "rain": rain,
        }
        for time, temperature_2m, precipitation, rain in zip(
            body["hourly"]["time"],
            body["hourly"]["temperature_2m"],
            body["hourly"]["precipitation"],
            body["hourly"]["rain"],
        )
    ]


def process_row(case: tuple[str, str, str, str, str]):
    case_number, country, area, location, time = (
        case[0],
        case[1],
        case[2],
        case[3],
        case[4],
    )

    if not case_number or not country or not time or not location:
        print("Case number %s is missing important data." % case_number)
        return

    date_result = re.search(date_regex, str(case_number))
    time_result = re.search(time_regex, str(time))
    if not date_result or not time_result:
        print("Case number %s is missing time data." % case_number)
        return

    date = None
    try:
        date = datetime.strptime(f"{date_result[0]} {time_result[0]}", "%Y.%m.%d %Hh%M")
    except ValueError as e:
        print("parse error:", e)
        print("Case number %s has invalid time data." % case_number)
        return

    if date < date_threshold:
        print("Case number %s is too old for OpenMeteo." % case_number)
        return

    sleep(1.6)  # OpenMeteo supports only 10 req/s in free tier

    print("Fetching coordinates for case number %s." % case_number)
    coordinates = get_lat_long(country, area, location)
    print(
        "Got coordinates for case number %s. Lat: %f, Long: %f"
        % (case_number, coordinates["latitude"], coordinates["longitude"])
    )

    print("Fetching weather data for case number %s." % case_number)
    weather = get_hourly_weather(
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


def main():
    df = pd.read_csv(data_path).replace({np.nan: None})
    cases = zip(
        df["Case Number"], df["Country"], df["Area"], df["Location"], df["Time"]
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        processed_data = list(executor.map(process_row, cases))

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


if __name__ == "__main__":
    load_dotenv(".env")
    main()
