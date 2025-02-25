import json
import logging
import time
import uuid
import aiohttp
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup
from google.cloud import storage
from .interfaces import IDataTransformer, IDataStorage
from .retry import make_request


class GCSDataStorage:
    def __init__(self, bucket_name: str, project_id: str):
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def store(self, data: Dict[str, Any], file_path: str) -> None:
        json_data = json.dumps(data)
        blob = self.bucket.blob(file_path)
        blob.upload_from_string(data=json_data, content_type="application/json")
        logging.info(f"Data stored to {file_path} in bucket {self.bucket.name}")

    def store_all(self, data_list: List[Dict[str, Any]], base_path: str) -> None:
        for idx, data in enumerate(data_list):
            file_path = f"{base_path}/item_{idx}.json"
            self.store(data, file_path)
        logging.info(
            f"Stored {len(data_list)} items in bucket {self.bucket.name} under {base_path}"
        )


class VADataTransformer(IDataTransformer):
    def __init__(self):
        self.people_id = None

    def transform(self, raw_data: Dict[str, Any], people_id: int) -> Dict[str, Any]:
        self.mal_id = people_id
        soup = BeautifulSoup(raw_data["html"], "html.parser")
        transformed_data = {
            "_airbyte_ab_id": str(uuid.uuid4()),
            "_airbyte_emitted_at": int(time.time() * 1000),
            "_airbyte_data": {
                "people_id": people_id,
                "url": raw_data["url"],
                "name": self._extract_name(soup),
                "given_name": self._extract_given_name(soup),
                "family_name": self._extract_family_name(soup),
                "birthday": self._extract_birthday(soup),
                "member_favorites": self._extract_member_favorites(soup),
                "more": self._extract_more(soup),
                "voice_acting_roles": self._extract_voice_acting_roles(soup),
                "anime_staff_positions": self._extract_anime_staff_positions(soup),
                "published_manga": self._extract_published_manga(soup),
            },
        }
        return transformed_data

    def _extract_name(self, soup: BeautifulSoup) -> Optional[str]:
        name_tag = soup.find("h1", class_="title-name")
        if name_tag:
            return name_tag.text.strip()
        return None

    def _extract_given_name(self, soup: BeautifulSoup) -> str:
        given_name_tag = soup.find("span", class_="dark_text", string="Given name:")
        if given_name_tag and given_name_tag.next_sibling:
            return given_name_tag.next_sibling.strip()
        return ""

    def _extract_family_name(self, soup: BeautifulSoup) -> str:
        family_name_tag = soup.find("span", class_="dark_text", string="Family name:")
        if family_name_tag and family_name_tag.next_sibling:
            return family_name_tag.next_sibling.strip()
        return ""

    def _extract_birthday(self, soup: BeautifulSoup) -> Optional[str]:
        birthday_tag = soup.find("span", class_="dark_text", string="Birthday:")
        if birthday_tag and birthday_tag.next_sibling:
            birthday_str = birthday_tag.next_sibling.strip()
            try:
                return datetime.strptime(birthday_str, "%b %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                try:
                    return datetime.strptime(birthday_str, "%b  %d, %Y").strftime(
                        "%Y-%m-%d"
                    )
                except ValueError:
                    return None
        return None

    def _extract_member_favorites(self, soup: BeautifulSoup) -> Optional[int]:
        favorites_tag = soup.find(
            "span", class_="dark_text", string="Member Favorites:"
        )
        if favorites_tag and favorites_tag.next_sibling:
            try:
                favorites_str = favorites_tag.next_sibling.strip().replace(",", "")
                return int(favorites_str)
            except ValueError:
                return None
        return None

    def _extract_more(self, soup: BeautifulSoup) -> str:
        more_div = soup.find("div", class_="people-informantion-more")
        if more_div:
            return more_div.get_text(separator="\n").strip()
        return ""

    def _extract_voice_acting_roles(self, soup: BeautifulSoup) -> List[Dict]:
        roles = []
        table = soup.find("table", class_="js-table-people-character")
        if table:
            for row in table.find_all("tr"):
                columns = row.find_all("td")
                if len(columns) >= 3:
                    anime_anchor = columns[1].find("a", class_="js-people-title")
                    anime_title = anime_anchor.text.strip() if anime_anchor else None
                    anime_url = (
                        anime_anchor["href"].strip()
                        if anime_anchor and "href" in anime_anchor.attrs
                        else None
                    )
                    character_divs = columns[2].find_all("div", class_="spaceit_pad")
                    if character_divs:
                        char_anchor = character_divs[0].find("a")
                        character_name = (
                            char_anchor.text.strip() if char_anchor else None
                        )
                        character_url = (
                            char_anchor["href"].strip()
                            if char_anchor and "href" in char_anchor.attrs
                            else None
                        )
                        role_type = (
                            character_divs[1].text.strip()
                            if len(character_divs) > 1
                            else None
                        )
                    else:
                        character_name = None
                        character_url = None
                        role_type = None

                    role_info = {
                        "anime_title": anime_title,
                        "anime_url": anime_url,
                        "character_name": character_name,
                        "character_url": character_url,
                        "role_type": role_type,
                    }
                    roles.append(role_info)
        return roles

    def _extract_anime_staff_positions(self, soup: BeautifulSoup) -> List[Dict]:
        positions = []
        table = soup.find("table", class_="js-table-people-staff")
        if table:
            for row in table.find_all("tr"):
                columns = row.find_all("td")
                if len(columns) > 1:
                    anime_link = columns[1].find("a", class_="js-people-title")
                    if anime_link:
                        position_info = {
                            "anime_title": anime_link.text.strip(),
                            "anime_url": anime_link["href"].strip(),
                            "position": (
                                columns[1].find("small").text.strip()
                                if columns[1].find("small")
                                else None
                            ),
                        }
                        positions.append(position_info)
        return positions

    def _extract_published_manga(self, soup: BeautifulSoup) -> List[Dict]:
        works = []
        table = soup.find("table", class_="js-table-people-manga")
        if table:
            for row in table.find_all("tr"):
                columns = row.find_all("td")
                if len(columns) > 1:
                    manga_link = columns[1].find("a", class_="js-people-title")
                    if manga_link:
                        role_tag = columns[1].find("small")
                        role = role_tag.text.strip() if role_tag else None
                        work_info = {
                            "manga_title": manga_link.text.strip(),
                            "manga_url": manga_link["href"].strip(),
                            "role": role,
                        }
                        works.append(work_info)
        return works


class MALPeopleScraper:
    def __init__(
        self,
        data_transformer: IDataTransformer,
        data_storage: IDataStorage,
    ):
        self.data_transformer = data_transformer
        self.data_storage = data_storage
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

    def scrape(self, url: str) -> Dict[str, Any]:
        try:
            response = make_request("GET", url, headers=self.headers)
            return {"html": response.text, "url": url}
        except requests.RequestException as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None

    async def scrape_all_people(self, output_prefix: str = "data") -> None:
        """
        Asynchronously paginate through the people entry pages (using utils.paginate_people)
        and scrape each individual voice actor page.

        Args:
            output_prefix (str): The prefix for the GCS storage path
        """
        from .utils import paginate_people

        async with aiohttp.ClientSession() as client:
            async for person_url in paginate_people(client):
                try:
                    people_id = int(person_url.rstrip("/").split("/")[-1])
                except ValueError:
                    logging.error(f"Could not extract people_id from URL: {person_url}")
                    continue

                try:
                    raw_data = self.scrape(person_url)
                    if raw_data is None:
                        continue

                    voice_actor_data = self.data_transformer.transform(
                        raw_data, people_id
                    )

                    if voice_actor_data:
                        # Define a unique output path for each voice actor using their people_id.
                        # Format: output_prefix/people_id.json
                        file_path = f"{output_prefix}/{people_id}.json"
                        self.data_storage.store(voice_actor_data, file_path)
                        logging.info(
                            f"Scraped and stored voice actor {people_id} to GCS"
                        )
                except Exception as e:
                    logging.error(f"Error scraping voice actor {people_id}: {e}")
                    continue
