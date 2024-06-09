import pandas as pd
import json
import re
from datetime import datetime as dt
import numpy as np

date_regex = r"\d{4}\.\d{2}\.\d{2}"
time_regex = r"\d{2}h\d{2}"


def main():
    df = pd.read_csv("./data/GSAF5.xls - Sheet1-GSAF.csv").replace({np.nan: None})

    processed_data = [
        process_row(case_number, country, area, location, time)
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


def process_row(case_number, country, area, location, time):
    if not case_number or not country or not time or not location:
        return

    date_result = re.search(date_regex, str(case_number))
    time_result = re.search(time_regex, str(time))
    if not date_result or not time_result:
        return

    try:
        datetime = dt.strptime(f"{date_result[0]} {time_result[0]}", "%Y.%m.%d %Hh%M")
    except ValueError as e:
        return print("parse error:", e)

    return {
        "case_number": case_number,
        "country": country,
        "area": area,
        "location": location,
        "datetime": datetime.isoformat(timespec="milliseconds"),
    }


def format_iso(datetime):
    return datetime.isoformat(timespec="milliseconds") + "Z"


if __name__ == "__main__":
    main()
