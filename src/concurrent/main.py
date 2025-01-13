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
import uuid
import dateutil.parser as date_parser

thread_local = threading.local()
lock = threading.Lock()

incident_log_path = "./data/GSAF.xls"

date_threshold = datetime(1970, 1, 1)

time_regex = r"(\d{2}).*(\d{2})"

default_hourly_params = [
    "temperature_2m",
    "precipitation",
    "rain",
]

num_threads = 16

gsaf_incident_log_url = "https://www.sharkattackfile.net/spreadsheets/GSAF5.xls"

def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.sessions.Session()
    return thread_local.session

def get_lat_long(country: str, state: str, location: str):
    filtered_address = [x for x in [location, state, country] if x is not None]
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

def process_data(case: tuple[str, str, str, str, str, str]):
    case_number = str(uuid.uuid4())
    print("Processing case:", case_number)

    date_str, time_str, country, state, location, species = case
    if not date_str or not time_str or not country or not location:
        print("Missing important data, not proceeding with case %s." % case_number)
        return

    try:
        date = date_parser.parse(date_str)
        hours, minutes = time_str.split(":")
        local_datetime = date.replace(hour=int(hours), minute=int(minutes))
    except:
        print("Date could not be parsed, not proceeding with case %s." % case_number)
        return

    if local_datetime < date_threshold:
        print("Case is older than epoch, not proceeding with case %s." % case_number)
        return

    print("Fetching coordinates for case %s ..." % case_number)
    coordinates = get_lat_long(country, state, location)
    print(
        "Got coordinates for case number %s. Lat: %f, Long: %f"
        % (case_number, coordinates["latitude"], coordinates["longitude"])
    )

    print("Fetching timezone for case %s..." % case_number)
    timezone = get_timezone(
        coordinates["latitude"], coordinates["longitude"], int(local_datetime.timestamp())
    )
    print("Got timezone for case number %s. Timezone: %s" % (case_number, timezone))

    # Get the timezone for the incident location
    local_timezone = pytz.timezone(timezone)

    # Localize the naive datetime to the incident location's timezone
    local_datetime = local_timezone.localize(local_datetime)

    # Convert to UTC for weather API request
    utc_datetime = local_datetime.astimezone(pytz.UTC)

    print("Fetching weather data for case number %s." % case_number)
    weather = get_hourly_weather(
        utc_datetime, coordinates["latitude"], coordinates["longitude"]
    )

    if weather is None:
        print("Could not get weather data for case number %s." % case_number)
        return

    print("Got weather data for case number %s." % case_number)

    # Get weather near the incident time
    weather = get_weather_near_time(weather, utc_datetime)

    return {
        "case_number": case_number,
        "country": country,
        "state": state,
        "location": location,
        "local_datetime": local_datetime.isoformat(),
        "utc_datetime": utc_datetime.isoformat(),
        "latitude": coordinates["latitude"],
        "longitude": coordinates["longitude"],
        "timezone": timezone,
        "shark_species": species,
        "weather": weather,
    }

def get_weather_near_time(weather_data: list[dict], incident_time: datetime) -> dict:
    return min(
        weather_data,
        key=lambda x: abs(datetime.fromisoformat(x["time"]) - incident_time),
    )

def download_incident_log():
    print("Downloading incident log...")
    response = requests.get(gsaf_incident_log_url)

    if not response.ok:
        print("Failed to download incident log.")
        return pd.DataFrame()

    # Save incident log to file
    with open(incident_log_path, "wb") as f:
        f.write(response.content)
        print("Download complete.")

def get_incident_log():
    # Check if incident log file exists
    if not os.path.exists(incident_log_path):
        download_incident_log()

    # Read incident log file
    df = pd.read_excel(incident_log_path).replace({np.nan: None})
    return df

def main():
    start_time = time.time()
    df = get_incident_log()

    df.columns = [column.strip() for column in df.columns]
    df = df[df["Type"] == "Unprovoked"]
    df = df[df["Time"].notnull()]
    df = df[df["Time"].apply(lambda t: bool(re.search(time_regex, str(t))))]

    def normalize_time(t):
        match = re.search(time_regex, str(t))
        return f"{match.group(1)}:{match.group(2)}"

    df["Time"] = df["Time"].map(normalize_time)
    df["Date"] = df["Date"].map(lambda d: str(d).replace("Reported ", ""))
    df = df[df["Location"].notnull()]
    df = df[df["State"].notnull()]
    df = df[df["Country"].notnull()]

    cases = zip(
        df["Date"], df["Time"], df["Country"], df["State"], df["Location"], df["Species"]
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        processed_data = list(executor.map(process_data, cases))
        # Filter out invalid data
        filtered_data = [d for d in processed_data if d is not None]

    print("%d total events" % len(df))
    print("%d valid events" % len(filtered_data))
    print("%d invalid events" % (len(processed_data) - len(filtered_data)))

    # Write the data to a Excel file
    df = pd.DataFrame(filtered_data)
    df.to_excel("shark_incidents.xlsx", index=False)

    json.dump(filtered_data, open("shark_incidents.json", "w"), indent=2)
    print("Done!")
    print("Duration: %is" % (time.time() - start_time))

if __name__ == "__main__":
    load_dotenv(".env")
    main()
