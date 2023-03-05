import requests
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIRECTORY = "/Users/michaelhackman/Github/ConcertScraper/"
CONCERT_CSS_SELECTOR = "a[data-analytics-label='upcoming_concerts_list']"
PAGINATION_CSS_SELECTOR = ".pagination"
ARTIST_LINK_CSS_SELECTOR = ".event-listings > ul > .artist:first-of-type > a"


@dataclass
class Concert:
    location: str
    date_time: str


def read_artist_list() -> List[str]:
    with open(f"{BASE_DIRECTORY}artist_names.txt") as f:
        artist_names = [name.strip() for name in f.readlines()]
    return artist_names


def fetch_link(link: str) -> BeautifulSoup:
    page = requests.get(link)
    return BeautifulSoup(page.content, "html.parser")


def fetch_artist_links(artist_names: List[str]) -> Dict[str, str]:
    artist_links = {}
    for name in artist_names:
        name_for_query = name.lower().replace(" ", "+")
        search_url = f"https://www.songkick.com/search?page=1&per_page=10&type=artists&query={name_for_query}"

        soup = fetch_link(search_url)
        results = soup.select(ARTIST_LINK_CSS_SELECTOR)

        # get the href from the first "a" element
        artist_link = results[0].get("href")
        artist_links[name] = artist_link
    return artist_links


def create_artist_concert_links(artist_links: Dict[str, str]) -> Dict[str, str]:
    artist_search_urls = {}
    for artist_name, artist_link in artist_links.items():
        artist_url = f"https://www.songkick.com{artist_link}/calendar"
        artist_search_urls[artist_name] = artist_url
    return artist_search_urls


def parse_concert_results(concert_results: ResultSet) -> List[Concert]:
    concerts = []
    for link in concert_results:
        date_time = link.find("time")["datetime"]
        location = link.find("strong", {"class": "primary-detail"}).text.strip()
        concerts.append(Concert(location=location, date_time=date_time))
    return concerts


def fetch_all_concerts_for_artist(url: str) -> List[Concert]:
    all_concerts = []
    link = url
    current_page_num = 1
    while True:
        soup = fetch_link(link)
        concert_results = soup.select(CONCERT_CSS_SELECTOR)
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
            rows.append((name, concert.location, concert.date_time))

    return pd.DataFrame(rows, columns=["name", "concert_location", "concert_time"]).sort_values(by='name')


def create_artist_info_dataframe(
    artist_concert_links: Dict[str, str], artist_concerts: Dict[str, List[Concert]]
) -> pd.DataFrame:
    rows = []
    for name, concerts in artist_concerts.items():
        hyperlink = artist_concert_links[name]
        rows.append((name, len(concerts), hyperlink))

    return pd.DataFrame(rows, columns=["name", "concert_count", "hyperlink"]).sort_values(by='name')


# artist_names = read_artist_list()
# artist_links = fetch_artist_links(artist_names)
# artist_concert_links = create_artist_concert_links(artist_links)
# print(artist_concert_links)
artist_concert_links = {
    "A Perfect Circle": "https://www.songkick.com/artists/549892-a-perfect-circle/calendar",
    "Abhi The Nomad": "https://www.songkick.com/artists/9350584-abhi-the-nomad/calendar",
    "AJR": "https://www.songkick.com/artists/1880688-ajr/calendar",
    "Animals As Leaders": "https://www.songkick.com/artists/2426656-animals-as-leaders/calendar",
    "Anomalie": "https://www.songkick.com/artists/1679379-anomalie/calendar",
    "Ariana Grande": "https://www.songkick.com/artists/4971683-ariana-grande/calendar",
    "Avenged Sevenfold": "https://www.songkick.com/artists/425252-avenged-sevenfold/calendar",
    "Billie Eilish": "https://www.songkick.com/artists/8913479-billie-eilish/calendar",
    "Bo Burnham": "https://www.songkick.com/artists/1090574-bo-burnham/calendar",
    "Breaking Benjamin": "https://www.songkick.com/artists/99074-breaking-benjamin/calendar",
    "Bring Me The Horizon": "https://www.songkick.com/artists/347077-bring-me-the-horizon/calendar",
    "Charlie Curtis-Beard": "https://www.songkick.com/artists/9286299-charlie-curtisbeard/calendar",
    "Chicks": "https://www.songkick.com/artists/58171-chicks/calendar",
    "Corpse": "https://www.songkick.com/artists/138044-corpse/calendar",
    "Cory Wong": "https://www.songkick.com/artists/2908766-cory-wong/calendar",
    "Demi Lovato": "https://www.songkick.com/artists/976211-demi-lovato/calendar",
    "Disclosure": "https://www.songkick.com/artists/771160-disclosure/calendar",
    "Dream Theater": "https://www.songkick.com/artists/8765-dream-theater/calendar",
    "Dua Lipa": "https://www.songkick.com/artists/8310783-dua-lipa/calendar",
    "Echosmith": "https://www.songkick.com/artists/6088614-echosmith/calendar",
    "Ed Sheeran": "https://www.songkick.com/artists/2083334-ed-sheeran/calendar",
    "Evans Blue": "https://www.songkick.com/artists/25520-evans-blue/calendar",
    "Fall Out Boy": "https://www.songkick.com/artists/315398-fall-out-boy/calendar",
    "Fergie": "https://www.songkick.com/artists/315579-fergie/calendar",
    "Fighting Kind": "https://www.songkick.com/artists/4836663-fighting-kind/calendar",
    "Flyleaf": "https://www.songkick.com/artists/346190-flyleaf/calendar",
    "Foo Fighters": "https://www.songkick.com/artists/29315-foo-fighters/calendar",
    "Fort Minor": "https://www.songkick.com/artists/455951-fort-minor/calendar",
    "Good Charlotte": "https://www.songkick.com/artists/520231-good-charlotte/calendar",
    "Haken": "https://www.songkick.com/artists/625989-haken/calendar",
    "Hiatus Kaiyote": "https://www.songkick.com/artists/4737378-hiatus-kaiyote/calendar",
    "Hollywood Undead": "https://www.songkick.com/artists/818463-hollywood-undead/calendar",
    "Infected Mushroom": "https://www.songkick.com/artists/153795-infected-mushroom/calendar",
    "Jacob Collier": "https://www.songkick.com/artists/6775909-jacob-collier/calendar",
    "Jay-Z": "https://www.songkick.com/artists/173376-jayz/calendar",
    "Justin Timberlake": "https://www.songkick.com/artists/209003-justin-timberlake/calendar",
    "Korn": "https://www.songkick.com/artists/202975-korn/calendar",
    "Lady Gaga": "https://www.songkick.com/artists/974908-lady-gaga/calendar",
    "Lil Nas X": "https://www.songkick.com/artists/10001194-lil-nas-x/calendar",
    "Lindsey Stirling": "https://www.songkick.com/artists/5429218-lindsey-stirling/calendar",
    "Louis Cole": "https://www.songkick.com/artists/1453171-louis-cole/calendar",
    "Magnolia Park": "https://www.songkick.com/artists/10080833-magnolia-park/calendar",
    "Marianas Trench": "https://www.songkick.com/artists/96444-marianas-trench/calendar",
    "Marshmello": "https://www.songkick.com/artists/8613384-marshmello/calendar",
    "Martin Garrix": "https://www.songkick.com/artists/5003643-martin-garrix/calendar",
    "Matchbox Twenty": "https://www.songkick.com/artists/30543-matchbox-twenty/calendar",
    "Mike Shinoda": "https://www.songkick.com/artists/2015258-mike-shinoda/calendar",
    "MIKÉL": "https://www.songkick.com/artists/989049-mikel/calendar",
    "Moonchild": "https://www.songkick.com/artists/133602-moonchild/calendar",
    "Muse": "https://www.songkick.com/artists/219230-muse/calendar",
    "My Darkest Days": "https://www.songkick.com/artists/1169452-my-darkest-days/calendar",
    "OneRepublic": "https://www.songkick.com/artists/568431-onerepublic/calendar",
    "Panic! At the Disco": "https://www.songkick.com/artists/139037-panic-at-the-disco/calendar",
    "Paramore": "https://www.songkick.com/artists/127596-paramore/calendar",
    "Parov Stelar": "https://www.songkick.com/artists/8954549-parov-stelar/calendar",
    "Periphery": "https://www.songkick.com/artists/973012-periphery/calendar",
    "Polyphia": "https://www.songkick.com/artists/6308759-polyphia/calendar",
    "Powfu": "https://www.songkick.com/artists/10041489-powfu/calendar",
    "Queens of the Stone Age": "https://www.songkick.com/artists/479466-queens-of-the-stone-age/calendar",
    "Quietdrive": "https://www.songkick.com/artists/491917-quietdrive/calendar",
    "Radiohead": "https://www.songkick.com/artists/253846-radiohead/calendar",
    "Red": "https://www.songkick.com/artists/2480126-red/calendar",
    "The Red Jumpsuit Apparatus": "https://www.songkick.com/artists/489864-red-jumpsuit-apparatus/calendar",
    "Renée Elise Goldsberry": "https://www.songkick.com/artists/10178884-renee-elise-goldsberry/calendar",
    "Sam Smith": "https://www.songkick.com/artists/807990-sam-smith/calendar",
    "Shag": "https://www.songkick.com/artists/139287-shag/calendar",
    "Shawn Mendes": "https://www.songkick.com/artists/8008073-shawn-mendes/calendar",
    "Sick Puppies": "https://www.songkick.com/artists/359193-sick-puppies/calendar",
    "Snarky Puppy": "https://www.songkick.com/artists/29793-snarky-puppy/calendar",
    "Sungazer": "https://www.songkick.com/artists/8469503-sungazer/calendar",
    "Supertramp": "https://www.songkick.com/artists/420142-supertramp/calendar",
    "System of a Down": "https://www.songkick.com/artists/74172-system-of-a-down/calendar",
    "Taylor McFerrin": "https://www.songkick.com/artists/670649-taylor-mcferrin/calendar",
    "Tennyson": "https://www.songkick.com/artists/8375803-tennyson/calendar",
    "TesseracT": "https://www.songkick.com/artists/495638-tesseract/calendar",
    "Theory of a Deadman": "https://www.songkick.com/artists/209134-theory-of-a-deadman/calendar",
    "Thirty Seconds to Mars": "https://www.songkick.com/artists/4550768-thirty-seconds-to-mars/calendar",
    "Three Days Grace": "https://www.songkick.com/artists/424396-three-days-grace/calendar",
    "Tigran Hamasyan": "https://www.songkick.com/artists/164878-tigran-hamasyan/calendar",
    "Tool": "https://www.songkick.com/artists/521019-tool/calendar",
    "Twenty One Pilots": "https://www.songkick.com/artists/3123851-twenty-one-pilots/calendar",
    "Unprocessed": "https://www.songkick.com/artists/9424684-unprocessed/calendar",
    "Vampire Weekend": "https://www.songkick.com/artists/288696-vampire-weekend/calendar",
    "Veil of Maya": "https://www.songkick.com/artists/584600-veil-of-maya/calendar",
    "Vulfpeck": "https://www.songkick.com/artists/6634379-vulfpeck/calendar",
    "weird inside": "https://www.songkick.com/artists/8600409-weird-inside/calendar",
    "Woodkid": "https://www.songkick.com/artists/4461618-woodkid/calendar",
    "The xx": "https://www.songkick.com/artists/515434-xx/calendar",
    "You Me At Six": "https://www.songkick.com/artists/246701-you-me-at-six/calendar",
    "Zac Brown Band": "https://www.songkick.com/artists/514278-zac-brown-band/calendar",
    "Zedd": "https://www.songkick.com/artists/992104-zedd/calendar",
    "ABBA": "https://www.songkick.com/artists/124886-abba/calendar",
    "The Academy Of Ancient Music": "https://www.songkick.com/artists/121215-academy-of-ancient-music/calendar",
    "Anika Nilles": "https://www.songkick.com/artists/10106780-anika-nilles/calendar",
    "Arch Echo": "https://www.songkick.com/artists/3700631-arch-echo/calendar",
    "Ark Patrol": "https://www.songkick.com/artists/8531119-ark-patrol/calendar",
    "Austin Wintory": "https://www.songkick.com/artists/7400014-austin-wintory/calendar",
    "Bill Laurance": "https://www.songkick.com/artists/8266768-bill-laurance/calendar",
    "Billy Cobham": "https://www.songkick.com/artists/446916-billy-cobham/calendar",
    "Billy Preston": "https://www.songkick.com/artists/150072-billy-preston/calendar",
    "Bruno Mars": "https://www.songkick.com/artists/941964-bruno-mars/calendar",
    "Choir Of King's College, Cambridge": "https://www.songkick.com/artists/20981-choir-of-kings-college-cambridge/calendar",
    "Craig Xen": "https://www.songkick.com/artists/8993909-craig-xen/calendar",
    "Damon Albarn": "https://www.songkick.com/artists/205139-damon-albarn/calendar",
    "Dan Mayo": "https://www.songkick.com/artists/8960304-dan-mayo/calendar",
    "Danae Greenfield": "https://www.songkick.com/artists/10149786-danae-greenfield/calendar",
    "Darren Korb": "https://www.songkick.com/artists/7116259-darren-korb/calendar",
    "Dave Koz": "https://www.songkick.com/artists/428987-dave-koz/calendar",
    "Dave Mackay": "https://www.songkick.com/artists/620036-dave-mackay/calendar",
    "David Maxim Micic": "https://www.songkick.com/artists/8967329-david-maxim-micic/calendar",
    "Deadmau5": "https://www.songkick.com/artists/244669-deadmau5/calendar",
    "Eagles of Death Metal": "https://www.songkick.com/artists/527957-eagles-of-death-metal/calendar",
    "Electric Mantis": "https://www.songkick.com/artists/8684779-electric-mantis/calendar",
    "Hans Zimmer": "https://www.songkick.com/artists/251898-hans-zimmer/calendar",
    "Haywyre": "https://www.songkick.com/artists/589119-haywyre/calendar",
    "Headspace": "https://www.songkick.com/artists/494193-headspace/calendar",
    "James Ehnes": "https://www.songkick.com/artists/393950-james-ehnes/calendar",
    "Joey Alexander": "https://www.songkick.com/artists/7259094-joey-alexander/calendar",
    "Julio Bashmore": "https://www.songkick.com/artists/2990466-julio-bashmore/calendar",
    "Justice": "https://www.songkick.com/artists/301499-justice/calendar",
    "Laura Mvula": "https://www.songkick.com/artists/6126864-laura-mvula/calendar",
    "LCD Soundsystem": "https://www.songkick.com/artists/241554-lcd-soundsystem/calendar",
    "Mac Ayres": "https://www.songkick.com/artists/9044599-mac-ayres/calendar",
    "Major Parkinson": "https://www.songkick.com/artists/486030-major-parkinson/calendar",
    "Maurizio Pollini": "https://www.songkick.com/artists/327242-maurizio-pollini/calendar",
    "Maxo": "https://www.songkick.com/artists/1274503-maxo/calendar",
    "Medasin": "https://www.songkick.com/artists/8954984-medasin/calendar",
    "Meshuggah": "https://www.songkick.com/artists/217917-meshuggah/calendar",
    "Miguel Migs": "https://www.songkick.com/artists/532238-miguel-migs/calendar",
    "Monuments": "https://www.songkick.com/artists/354711-monuments/calendar",
    "Mosca": "https://www.songkick.com/artists/982647-mosca/calendar",
    "Mother Mother": "https://www.songkick.com/artists/547378-mother-mother/calendar",
    "MXXWLL": "https://www.songkick.com/artists/9258799-mxxwll/calendar",
    "Nine Inch Nails": "https://www.songkick.com/artists/241755-nine-inch-nails/calendar",
    "OLIVER": "https://www.songkick.com/artists/2560021-oliver/calendar",
    "Paris Monster": "https://www.songkick.com/artists/7850694-paris-monster/calendar",
    "Plini": "https://www.songkick.com/artists/8507178-plini/calendar",
    "Primus": "https://www.songkick.com/artists/385368-primus/calendar",
    "Pryapisme": "https://www.songkick.com/artists/1061012-pryapisme/calendar",
    "The Reign of Kindo": "https://www.songkick.com/artists/580301-reign-of-kindo/calendar",
    "Ruslan Sirota": "https://www.songkick.com/artists/3705496-ruslan-sirota/calendar",
    "The Safety Fire": "https://www.songkick.com/artists/1997655-safety-fire/calendar",
    "Sam Wills": "https://www.songkick.com/artists/4503663-sam-wills/calendar",
    "Sergei Rachmaninoff": "https://www.songkick.com/artists/10091313-sergei-rachmaninoff/calendar",
    "Serj Tankian": "https://www.songkick.com/artists/582183-serj-tankian/calendar",
    "Shai Maestro": "https://www.songkick.com/artists/1001072-shai-maestro/calendar",
    "Solstice Coil": "https://www.songkick.com/artists/31787-solstice-coil/calendar",
    "Sparkee": "https://www.songkick.com/artists/10077485-sparkee/calendar",
    "Thrown": "https://www.songkick.com/artists/582161-thrown/calendar",
    "Todd Rundgren": "https://www.songkick.com/artists/591959-todd-rundgren/calendar",
    "Tom Misch": "https://www.songkick.com/artists/8341698-tom-misch/calendar",
    "Wallace": "https://www.songkick.com/artists/64619-wallace/calendar",
    "Wayne Shorter": "https://www.songkick.com/artists/57889-wayne-shorter/calendar",
    "Woody Goss": "https://www.songkick.com/artists/10119192-woody-goss/calendar",
    "Yakul": "https://www.songkick.com/artists/9200914-yakul/calendar",
    "Yaron Herman": "https://www.songkick.com/artists/424676-yaron-herman/calendar",
    "Yoni Rechter": "https://www.songkick.com/artists/24017-yoni-rechter/calendar",
    "Yoshihisa Hirano": "https://www.songkick.com/artists/10138152-yoshihisa-hirano/calendar",
}
artist_concerts = fetch_artist_concerts(artist_concert_links)

concerts = create_concerts_dataframe(artist_concerts)
artist_info = create_artist_info_dataframe(artist_concert_links, artist_concerts)


print(artist_info)
concerts.to_csv(f"{BASE_DIRECTORY}concerts.csv", index=False)
artist_info.to_csv(f"{BASE_DIRECTORY}artist_info.csv", index=False)
