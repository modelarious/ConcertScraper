import requests
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag
from dataclasses import dataclass
from typing import List, Dict, Optional
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed


BASE_DIRECTORY = "./"
CONCERT_CSS_SELECTOR = "a[data-analytics-label='upcoming_concerts_list']"
PAGINATION_CSS_SELECTOR = ".pagination"
ARTIST_LINK_CSS_SELECTOR = ".event-listings > ul > .artist:first-of-type > a"

NAME_COL = "name"
DATE_COL = "date"
CITY_COL = "city"
COUNTRY_COL = "country"
STATE_COL = "state"
UNIQUE_CONCERT_COUNT_COL = "unique_concert_count"
HYPERLINK_COL = "hyperlink"


@dataclass
class Concert:
    date: str
    country: str
    city: str
    state: Optional[str] = None


def read_artist_list() -> List[str]:
    with open(f"{BASE_DIRECTORY}artist_names.txt") as f:
        artist_names = [name.strip() for name in f.readlines()]
    return sorted(list(set(artist_names)))


def fetch_link(link: str) -> BeautifulSoup:
    page = requests.get(link)
    return BeautifulSoup(page.content, "html.parser")


def fetch_artist_link(name: str) -> Optional[str]:
    name_for_query = name.lower().replace(" ", "+")
    search_url = f"https://www.songkick.com/search?page=1&per_page=10&type=artists&query={name_for_query}"

    soup = fetch_link(search_url)
    results = soup.select(ARTIST_LINK_CSS_SELECTOR)

    try:
        # get the href from the first "a" element
        return results[0].get("href")
    except:
        return None


def fetch_artist_links(artist_names: List[str]) -> Dict[str, str]:
    unable_to_locate = []
    artist_links = {}
    with ThreadPoolExecutor() as executor:
        futures = {}
        for artist_name in artist_names:
            futures[executor.submit(fetch_artist_link, artist_name)] = artist_name
        for future in as_completed(futures):
            artist_name = futures[future]
            print(artist_name)
            link = future.result()
            if not link:
                unable_to_locate.append(artist_name)
            else:
                artist_links[artist_name] = link

    return artist_links, unable_to_locate


def create_artist_concert_links(artist_links: Dict[str, str]) -> Dict[str, str]:
    artist_search_urls = {}
    for artist_name, artist_link in artist_links.items():
        artist_url = f"https://www.songkick.com{artist_link}/calendar"
        artist_search_urls[artist_name] = artist_url
    return artist_search_urls


def make_concert(date_time: str, location: str) -> Concert:
    date = date_time.split("T")[0]
    location_split_out = [loc.strip() for loc in location.split(",")]
    city = location_split_out[0]
    country = location_split_out[-1]
    if len(location_split_out) == 2:
        return Concert(date=date, country=country, city=city)
    if len(location_split_out) == 3:
        state = location_split_out[1]
        return Concert(date=date, country=country, state=state, city=city)
    raise Exception(
        f"Found a concert location with invalid information: {location} -> {location_split_out}. the location split out should have either length 2 or 3"
    )


def parse_concert_results(concert_results: ResultSet) -> List[Concert]:
    concerts = []
    for link in concert_results:
        date_time = link.find("time")["datetime"]
        location = link.find("strong", {"class": "primary-detail"}).text.strip()
        concerts.append(make_concert(date_time, location))
    return concerts


def is_canceled(concert: Tag) -> bool:
    return concert.find("strong", class_="canceled")


def fetch_all_concerts_for_artist(url: str) -> List[Concert]:
    all_concerts = []
    link = url
    current_page_num = 1
    while True:
        soup = fetch_link(link)
        concert_results = [c for c in soup.select(CONCERT_CSS_SELECTOR) if not is_canceled(c)]
        all_concerts.extend(parse_concert_results(concert_results))

        # some artists have multiple pages of concerts and require pagination
        if not can_paginate(soup):
            break

        current_page_num += 1
        link = f"{url}?page={current_page_num}"
    return all_concerts


def fetch_artist_concerts(
    artist_concert_links: Dict[str, str]
) -> Dict[str, List[Concert]]:
    artist_concerts = {}
    with ThreadPoolExecutor() as executor:
        futures = {}
        for artist_name, artist_concert_link in artist_concert_links.items():
            futures[
                executor.submit(fetch_all_concerts_for_artist, artist_concert_link)
            ] = artist_name
        for future in as_completed(futures):
            artist_name = futures[future]
            print(artist_name)
            all_concerts = future.result()
            artist_concerts[artist_name] = all_concerts
    return artist_concerts


def can_paginate(soup) -> bool:
    next_page_element = soup.find("a", class_="next_page")
    return next_page_element and next_page_element.has_attr("href")


def create_concerts_dataframe(
    artist_concerts: Dict[str, List[Concert]]
) -> pd.DataFrame:
    rows = []
    for name, concerts in artist_concerts.items():
        for concert in concerts:
            rows.append(
                (name, concert.date, concert.city, concert.state, concert.country)
            )

    return (
        pd.DataFrame(
            rows, columns=[NAME_COL, DATE_COL, CITY_COL, STATE_COL, COUNTRY_COL]
        )
        .sort_values(by=NAME_COL)
        .drop_duplicates()
    )


def create_artist_info_dataframe(
    artist_concert_links: Dict[str, str], concerts: pd.DataFrame
) -> pd.DataFrame:
    rows = [(key, value) for key, value in artist_concert_links.items()]
    links = pd.DataFrame(rows, columns=[NAME_COL, HYPERLINK_COL])
    concert_counts = concerts[[NAME_COL, DATE_COL]].groupby(NAME_COL).count()
    merged = pd.merge(concert_counts, links, how="right", on=NAME_COL).rename(
        columns={DATE_COL: UNIQUE_CONCERT_COUNT_COL}
    )
    merged[UNIQUE_CONCERT_COUNT_COL] = (
        merged[UNIQUE_CONCERT_COUNT_COL].fillna(0).astype(int)
    )
    return merged



def scrape_concert_info():
    print("reading artist list")
    artist_names = read_artist_list()
    print("fetching artist links")
    artist_links, unable_to_locate = fetch_artist_links(artist_names)
    artist_concert_links = create_artist_concert_links(artist_links)
    artist_concerts = fetch_artist_concerts(artist_concert_links)

    concerts = create_concerts_dataframe(artist_concerts)
    artist_info = create_artist_info_dataframe(artist_concert_links, concerts)

    print(artist_info)
    concerts.to_csv(f"{BASE_DIRECTORY}concerts.csv", index=False)
    artist_info.to_csv(f"{BASE_DIRECTORY}artist_info.csv", index=False)

    print("WAS UNABLE TO LOCATE THE FOLLOWING BANDS:")
    from pprint import pprint

    pprint(unable_to_locate)

import argh
def main(duration_days: int = 7, filter_start_date: str = "2023-05-01"):
    print(f"received these args: {filter_start_date}, {duration_days}")
    scrape_concert_info()



argh.dispatch_command(main)
