import pandas as pd
from itertools import combinations

# largest day range to search within
DAY_RANGE = 1

# incoming cols
NAME_COL = "name"
DATE_COL = "date"
CITY_COL = "city"
COUNTRY_COL = "country"
STATE_COL = "state"

# output additional cols
START_DATE_COL = "start_date"
END_DATE_COL = "end_date"
DATE_RANGE_COL = "date_range"
BANDS_COL = "bands"
BAND_COUNT_COL = "band_count"
TIMEDELTA_COL = "timedelta"
TIMEDELTA_DAYS_COL = "timedelta_days"


# Define a custom function to aggregate data for each city
def aggregate_city(city_df):
    # Get all combinations of dates within a 5-day (DAY_RANGE) window
    date_combinations = list(combinations(city_df[DATE_COL], 2))
    date_combinations = [
        sorted((d1, d2))
        for (d1, d2) in date_combinations
        if abs((d2 - d1).days) <= (DAY_RANGE - 1)
    ]

    # Aggregate data for each combination of dates
    rows = []
    for (start_date, end_date) in date_combinations:
        bands = city_df[
            (city_df[DATE_COL] >= start_date) & (city_df[DATE_COL] <= end_date)
        ][NAME_COL].tolist()

        band_set = set(bands)
        if len(band_set) <= 1:
            continue

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        if start_date != end_date:
            date_range = f"{start_date} to {end_date}"
        else:
            date_range = f"{start_date}"

        city = city_df.iloc[0][CITY_COL]
        country = city_df.iloc[0][COUNTRY_COL]
        state = city_df.iloc[0][STATE_COL]

        rows.append(
            {
                START_DATE_COL: pd.Timestamp(start_date),
                END_DATE_COL: pd.Timestamp(end_date),
                DATE_RANGE_COL: date_range,
                BANDS_COL: tuple(band_set),  # needs to be hashable
                BAND_COUNT_COL: len(band_set),
                CITY_COL: city,
                COUNTRY_COL: country,
                STATE_COL: state,
            }
        )
    return pd.DataFrame(rows)


def process_concert_ranges(concerts: pd.DataFrame):
    concerts[DATE_COL] = pd.to_datetime(concerts[DATE_COL])

    # create groupings of bands that appear within DAY_RANGE days of each other in the same city
    groupings = (
        concerts.groupby([CITY_COL, COUNTRY_COL])
        .apply(aggregate_city)
        .reset_index(drop=True)
        .drop_duplicates()
    )

    # choosing the smallest amount of days it would take to see all the bands in the list.
    groupings[TIMEDELTA_COL] = (
        groupings[END_DATE_COL] - groupings[START_DATE_COL]
    ).dt.days + 1
    with_min_timedelta = (
        groupings.groupby([CITY_COL, COUNTRY_COL, BANDS_COL])
        .agg(
            {
                TIMEDELTA_COL: "min",
            }
        )
        .reset_index()
    )

    output = pd.merge(
        groupings,
        with_min_timedelta,
    )
    output[BAND_COUNT_COL] = output[BAND_COUNT_COL].astype(int)
    output[TIMEDELTA_DAYS_COL] = output[TIMEDELTA_COL]
    return output[
        [
            START_DATE_COL,
            END_DATE_COL,
            DATE_RANGE_COL,
            TIMEDELTA_DAYS_COL,
            BANDS_COL,
            BAND_COUNT_COL,
            CITY_COL,
            COUNTRY_COL,
            STATE_COL,
        ]
    ].sort_values(by=BAND_COUNT_COL)


concerts = pd.read_csv("/Users/michaelhackman/Github/ConcertScraper/concerts.csv")
grouped = process_concert_ranges(concerts)
print(grouped)
grouped.to_csv("/Users/michaelhackman/Github/ConcertScraper/groupings.csv", index=False)
