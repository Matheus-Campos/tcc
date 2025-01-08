import threading
import concurrent.futures
from dotenv import load_dotenv
import requests
import pandas as pd
import numpy as np
import re
from datetime import datetime
import os
import pytz
import json
import time

thread_local = threading.local()

data_path = "./data/GSAF5.xls - Sheet1-GSAF.csv"

date_threshold = datetime(1970, 1, 1)

date_regex = r"\d{4}\.\d{2}\.\d{2}"
time_regex = r"\d{2}h\d{2}"

default_hourly_params = [
    "temperature_2m",
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

    url = os.getenv("GEOCODING_API_URL")
    apikey = os.getenv("GEOCODING_API_KEY")
    response = get_session().get(url, params={"key": apikey, "address": address})
    if not response.ok:
        print("Response:", response)
        raise "Error in geocoding service"

    body = response.json()
    coords = body["results"][0]["geometry"]["location"]
    return {"latitude": coords["lat"], "longitude": coords["lng"]}


def get_timezone(latitude: float, longitude: float, timestamp: int) -> str:
    url = os.getenv("TIMEZONE_API_URL")
    apikey = os.getenv("TIMEZONE_API_KEY")

    response = get_session().get(
        url,
        params={
            "key": apikey,
            "timestamp": timestamp,
            "location": f"{latitude},{longitude}",
        },
    )
    if not response.ok:
        print("Response:", response)
        raise "Error in timezone service"

    body: dict = response.json()
    return body.get("timeZoneId", "UTC")


def get_hourly_weather(date: datetime, latitude: float, longitude: float):
    time.sleep(1.6)  # sleep to avoid rate limiting
    url = os.getenv("OPEN_METEO_API_URL")
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
        return get_hourly_weather(date, latitude, longitude)

    body = response.json()
    return [
        {
            "time": pytz.UTC.localize(datetime.fromisoformat(time)).isoformat(),
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


def sanitize_data(case: tuple[str, str, str, str, str]):
    case_number, country, area, location, time_str = (
        case[0],
        case[1],
        case[2],
        case[3],
        case[4],
    )

    if not case_number or not country or not time_str or not location:
        print("Case number %s is missing important data." % case_number)
        return

    date_result = re.search(date_regex, str(case_number))
    time_result = re.search(time_regex, str(time_str))
    if not date_result or not time_result:
        print("Case number %s is missing time data." % case_number)
        return

    naive_date = None
    try:
        naive_date = datetime.strptime(
            f"{date_result[0]} {time_result[0]}", "%Y.%m.%d %Hh%M"
        )
    except ValueError as e:
        print("parse error:", e)
        print("Case number %s has invalid time data." % case_number)
        return

    if naive_date < date_threshold:
        print("Case number %s is too old for OpenMeteo." % case_number)
        return

    print("Fetching coordinates for case number %s." % case_number)
    coordinates = get_lat_long(country, area, location)
    print(
        "Got coordinates for case number %s. Lat: %f, Long: %f"
        % (case_number, coordinates["latitude"], coordinates["longitude"])
    )

    timezone = get_timezone(
        coordinates["latitude"], coordinates["longitude"], int(naive_date.timestamp())
    )
    print("Got timezone for case number %s. Timezone: %s" % (case_number, timezone))

    return {
        "case_number": case_number,
        "country": country,
        "area": area,
        "location": location,
        "date": naive_date.strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": coordinates["latitude"],
        "longitude": coordinates["longitude"],
        "timezone": timezone,
    }


def process_data(data: dict[str, any]) -> dict:
    coordinates = {"latitude": data["latitude"], "longitude": data["longitude"]}
    naive_date = datetime.strptime(data["date"], "%Y-%m-%d %H:%M:%S")
    case_number = data["case_number"]

    # Get the timezone for the incident location
    local_timezone = pytz.timezone(data["timezone"])

    # Localize the naive datetime to the incident location's timezone
    local_date = local_timezone.localize(naive_date)

    # Convert to UTC for weather API request
    utc_date = local_date.astimezone(pytz.UTC)

    print("Fetching weather data for case number %s." % case_number)
    weather = get_hourly_weather(
        utc_date, coordinates["latitude"], coordinates["longitude"]
    )

    if weather is None:
        print("Could not get weather data for case number %s." % case_number)
    else:
        print("Got weather data for case number %s." % case_number)

        # Get weather near the incident time
        weather = get_weather_near_time(weather, utc_date)

    return {
        "case_number": case_number,
        "local_datetime": local_date.isoformat(),
        "utc_datetime": utc_date.isoformat(),
        "weather": weather,
    }


def get_weather_near_time(weather_data: list[dict], incident_time: datetime) -> dict:
    return min(
        weather_data,
        key=lambda x: abs(datetime.fromisoformat(x["time"]) - incident_time),
    )


def main():
    start_time = time.time()
    df = pd.read_csv(data_path).replace({np.nan: None})
    cases = zip(
        df["Case Number"], df["Country"], df["Area"], df["Location"], df["Time"]
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        sanitized_data = list(executor.map(sanitize_data, cases))
        filtered_data = [data for data in sanitized_data if data is not None]
        processed_data = list(executor.map(process_data, filtered_data))
        final_data = list(
            executor.map(lambda d: {**d[0], **d[1]}, zip(filtered_data, processed_data))
        )

    print("%d total events" % len(df))
    print("%d valid events" % len(filtered_data))
    print("%d invalid events" % (len(sanitized_data) - len(filtered_data)))

    # Write the data to a Excel file
    df = pd.DataFrame(final_data)
    df.to_excel("shark_incidents.xlsx", index=False)

    json.dump(final_data, open("shark_incidents.json", "w"), indent=2)
    print("Done!")
    print("Duration: %is" % (time.time() - start_time))


if __name__ == "__main__":
    load_dotenv(".env")
    main()
