# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "astral",
#     "duckdb",
# ]
# ///


import csv
import datetime
import zoneinfo

import astral
from astral.sun import sun
import duckdb

MIN_YEAR = 2000
SUNNY = set(["CLR:00", "FEW:01", "FEW:02"])
NEW_YORK = zoneinfo.ZoneInfo("America/New_York")

city = astral.LocationInfo("Chattanooga", "USA", "America/New_York", 35.0456, -85.3097)


def avg(lst):
    if len(lst) == 0:
        return None
    return sum(lst) / len(lst)


def c2f(c):
    if c is None:
        return None
    return (c * (9 / 5)) + 32


def rnd(n):
    if n is None:
        return None
    return int(round(n, 0))


# connect
url = "https://raw.githubusercontent.com/openchattanooga/GHCNh/refs/heads/main/GHCNh_USW00013882_por.parquet"
duckdb.sql(
    f'CREATE VIEW data AS SELECT * FROM read_parquet("{url}") WHERE Year >= {MIN_YEAR};'
)

# create info for each day
days = {}

dawn_by_day = {}
sunrise_by_day = {}
dusk_by_day = {}
sunset_by_day = {}

header = [
    "Year",
    "Month",
    "Day",
    "Hour",
    "precipitation",
    "temperature",
    "sky_cover_1",
    "sky_cover_2",
    "sky_cover_3",
]
sql = f"SELECT {','.join(header)} FROM data ORDER BY Year, Month, Day ASC;"
rows = [dict(zip(header, row)) for row in duckdb.sql(sql).fetchall()]

for row in rows:
    year = row["Year"]
    month = row["Month"]
    day = row["Day"]
    hour = row["Hour"]
    ref = datetime.datetime(year, month, day, hour, 30, 0, 0, tzinfo=NEW_YORK)
    weekday = ref.weekday()

    skey = (year, month, day)
    if skey not in sunrise_by_day:
        noon = datetime.datetime(year, month, day, 12, 0, 0, 0, tzinfo=NEW_YORK)
        s = sun(city.observer, date=noon, tzinfo=city.timezone)
        dawn_by_day[skey] = s["dawn"]
        sunrise_by_day[skey] = s["sunrise"]
        dusk_by_day[skey] = s["dusk"]
        sunset_by_day[skey] = s["sunset"]

    dawn = dawn_by_day[skey]
    sunrise = sunrise_by_day[skey]
    dusk = dusk_by_day[skey]
    sunset = sunset_by_day[skey]

    key = (year, month, day)

    if key not in days:
        days[key] = {
            "year": year,
            "month": month,
            "day": day,
            "rainy": False,
            "weekday": weekday < 5,
            "saturday": weekday == 5,
            "sunday": weekday == 6,
            "sunny_hours": 0,
            "cloudy_hours": 0,
            "min_temp": None,
            "max_temp": None,
            "day_temps": [],
            "night_temps": [],
        }

    temp = c2f(row["temperature"])

    if temp is not None:
        if sunrise < ref < sunset:
            days[key]["day_temps"].append((hour, temp))
        if ref < sunrise or ref > sunset:
            days[key]["night_temps"].append((hour, temp))

        if days[key]["min_temp"] is None or temp < days[key]["min_temp"]:
            days[key]["min_temp"] = temp
        if days[key]["max_temp"] is None or temp > days[key]["max_temp"]:
            days[key]["max_temp"] = temp

    if row["precipitation"] is not None and row["precipitation"] > 0:
        days[key]["rainy"] = True

    sky_cover_set = set([row["sky_cover_1"], row["sky_cover_2"], row["sky_cover_3"]])

    # filter out "X", which is not knowing
    if "X" in sky_cover_set:
        sky_cover_set.remove("X")
    if None in sky_cover_set:
        sky_cover_set.remove(None)

    if len(sky_cover_set) > 0 and sky_cover_set.issubset(SUNNY):
        days[key]["sunny_hours"] += 1

    # at least one cloud layer in the set says its overcast
    if "OVC:08" in sky_cover_set:
        days[key]["cloudy_hours"] += 1

months = {}
for row in days.values():
    year = row["year"]
    month = row["month"]
    key = (year, month)
    if key not in months:
        months[key] = {
            "year": year,
            "month": month,
            "cloudy_days": 0,
            "rainy_days": 0,
            "sunny_days": 0,
            "weekdays": 0,
            "saturdays": 0,
            "sundays": 0,
            "max_temps": [],
            "min_temps": [],
            "day_temps": [],
            "night_temps": [],
        }

    if row["weekday"]:
        months[key]["weekdays"] += 1
    if row["saturday"]:
        months[key]["saturdays"] += 1
    if row["sunday"]:
        months[key]["sundays"] += 1
    months[key]["day_temps"] += row["day_temps"]
    months[key]["night_temps"] += row["night_temps"]

    if row["cloudy_hours"] >= 12:
        months[key]["cloudy_days"] += 1

    if row["rainy"]:
        months[key]["rainy_days"] += 1

    if row["sunny_hours"] >= 12:
        months[key]["sunny_days"] += 1

    months[key]["min_temps"].append(row["min_temp"])
    months[key]["max_temps"].append(row["max_temp"])

results = []
for row in months.values():
    results.append(
        {
            "year": row["year"],
            "month": row["month"],
            "weekdays": row["weekdays"],
            "saturdays": row["saturdays"],
            "sundays": row["sundays"],
            "cloudy_days": row["cloudy_days"],
            "rainy_days": row["rainy_days"],
            "sunny_days": row["sunny_days"],
            "avg_low": rnd(avg(row["min_temps"])),
            "avg_high": rnd(avg(row["max_temps"])),
            # "avg_nighttime_temp": rnd(avg([t for h, t in row["night_temps"]])),
            # "avg_daytime_temp": rnd(avg([t for h, t in row["day_temps"]]))
        }
    )
    # if results[-1]["avg_nighttime_temp"] > results[-1]["avg_daytime_temp"]:
    #     print(row)

with open("stats.csv", "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "year",
            "month",
            "weekdays",
            "saturdays",
            "sundays",
            "cloudy_days",
            "rainy_days",
            "sunny_days",
            "avg_low",
            "avg_high",
        ],
    )
    writer.writeheader()
    writer.writerows(results)
