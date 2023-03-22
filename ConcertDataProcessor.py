import pandas as pd
from itertools import combinations
from typing import Dict, NamedTuple, Optional

# largest day range to search within
DAY_RANGE = 7

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
RAW_SCORE_COL = "raw_score"
AVG_SCORE_COL = "avg_score"


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

        # score = 0
        # for band_name in band_set:
        #     try:
        #         score += ratings[band_name]
        #     except:
        #         pass
        # score = round(score / len(band_set), 2)
        raw_score = sum([ratings[band_name] for band_name in band_set])
        avg_score = round(raw_score / len(band_set), 2)

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
                BANDS_COL: tuple(sorted(band_set)),  # needs to be hashable
                BAND_COUNT_COL: len(band_set),
                CITY_COL: city,
                COUNTRY_COL: country,
                STATE_COL: state,
                RAW_SCORE_COL: raw_score,
                AVG_SCORE_COL: avg_score,
            }
        )
    return pd.DataFrame(rows)


def process_concert_ranges(concerts: pd.DataFrame) -> Optional[pd.DataFrame]:
    if concerts.empty:
        return None

    concerts[DATE_COL] = pd.to_datetime(concerts[DATE_COL])

    # create groupings of bands that appear within DAY_RANGE days of each other in the same city
    groupings = (
        concerts.groupby([CITY_COL, COUNTRY_COL])
        .apply(aggregate_city)
        .reset_index(drop=True)
        .drop_duplicates()
    )
    if groupings.empty:
        return None

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
            RAW_SCORE_COL,
            AVG_SCORE_COL,
        ]
    ].sort_values(by=[RAW_SCORE_COL, AVG_SCORE_COL, START_DATE_COL, END_DATE_COL, CITY_COL, COUNTRY_COL])


class RatingData(NamedTuple):
    rating_dict: Dict[str, str]
    rating_df: pd.DataFrame


def read_ratings() -> RatingData:
    michael_ratings = pd.read_csv(
        "/Users/michaelhackman/Github/ConcertScraper/michael_rankings.tsv", sep="\t"
    )
    taylor_ratings = pd.read_csv(
        "/Users/michaelhackman/Github/ConcertScraper/taylor_rankings.tsv", sep="\t"
    )
    all_ratings = pd.concat([michael_ratings, taylor_ratings])

    all_ratings["Tier"] = (1 - all_ratings["Tier"] + all_ratings["Tier"].max() + 1) * 10
    all_ratings = all_ratings.groupby("Band").agg({"Tier": "sum"}).reset_index()
    all_ratings_dict = dict(zip(all_ratings["Band"], all_ratings["Tier"]))
    return RatingData(all_ratings_dict, all_ratings)


def filter_by_date(df, date_col_name):
    return df[
        ((df[date_col_name] >= "2023-05-01") & (df[COUNTRY_COL] != "Canada"))
        | (df[COUNTRY_COL] == "Canada")
    ]

# really wish I could pass this through the functions, but the function I would use it in is aggregate_city, which doesn't allow extra params
rating_data = read_ratings()
# print(rating_data)
ratings = rating_data.rating_dict
concerts = pd.read_csv("/Users/michaelhackman/Github/ConcertScraper/concerts.csv")
grouped = process_concert_ranges(concerts)
if grouped is None:
    print("No groupings of concerts found!")
    exit(-1)
# filtered = grouped[
#     ((grouped[START_DATE_COL] >= "2023-05-01") & (grouped[COUNTRY_COL] != "Canada"))
#     | (grouped[COUNTRY_COL] == "Canada")
# ]
filtered = filter_by_date(grouped, START_DATE_COL)
# print(filtered)
# filtered = filtered[
#     (filtered[COUNTRY_COL] == "Canada")
# ]
# print(filtered)
filtered.to_csv(
    "/Users/michaelhackman/Github/ConcertScraper/groupings.csv", index=False
)


print(rating_data.rating_df)
scored_concerts = pd.merge(
    concerts, rating_data.rating_df, left_on="name", right_on="Band"
).drop("Band", axis=1)
scored_concerts[RAW_SCORE_COL] = scored_concerts["Tier"]
scored_concerts = filter_by_date(scored_concerts, DATE_COL)
scored_concerts = scored_concerts.drop("Tier", axis=1).sort_values(
    by=[RAW_SCORE_COL, NAME_COL]
)
scored_concerts.to_csv(
    "/Users/michaelhackman/Github/ConcertScraper/scored_concerts.csv", index=False
)
