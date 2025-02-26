import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import uuid
from typing import Dict, List, Any, Optional
from .interfaces import IUrlGenerator, IDataScraper, IDataTransformer, IDataStorage
from pathlib import Path
import logging
import time
import re
from .anime_checkpoint import AnimeCheckpointHandler
from .utils import paginate_anime
import aiohttp
import asyncio
import os


class MALUrlGenerator(IUrlGenerator):
    def generate(self, mal_id: int) -> str:
        """Generate MAL anime URL from ID"""
        return f"https://myanimelist.net/anime/{mal_id}"


class MALScraper(IDataScraper):
    @staticmethod
    def scrape(url: str) -> Dict[str, Any]:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return {"html": response.text, "url": url}
        except requests.RequestException as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None


class MALDataTransformer(IDataTransformer):
    def __init__(self):
        self.mal_id = None

    def transform(self, raw_data: Dict[str, Any], mal_id: int) -> Dict[str, Any]:
        self.mal_id = mal_id
        soup = BeautifulSoup(raw_data["html"], "html.parser")

        transformed_data = {
            "_airbyte_ab_id": str(uuid.uuid4()),
            "_airbyte_emitted_at": int(time.time() * 1000),
            "_airbyte_data": {
                "id": self.mal_id,
                "title": self._extract_title(soup),
                "url": f"https://myanimelist.net/anime/{self.mal_id}",
                "microdata": self._extract_microdata(soup),
                "leftSide": self._extract_left_side(soup),
                "relatedEntries": self._extract_related_entries(soup),
                "themeSongs": self._extract_theme_songs(soup),
                "streamingPlatforms": self._extract_streaming_platforms(soup),
                "voiceActors": self._extract_characters_voice_actors_list(soup),
            },
        }

        return transformed_data

    def _extract_microdata(self, soup: BeautifulSoup) -> List[Dict]:
        return [
            {
                "_type": "http://schema.org/TVSeries",
                "name": self._get_text(soup, "h1.title-name strong"),
                "image": self._get_image_url(soup),
                "genre": self._extract_genres(soup),
                "aggregateRating": self._extract_rating(soup),
                "itemListElement": self._extract_breadcrumbs(soup),
                "description": self._get_text(soup, 'p[itemprop="description"]'),
            },
            {
                "_type": "http://schema.org/BreadcrumbList",
                "itemListElement": self._extract_breadcrumbs(soup),
            },
        ]

    def _get_text(self, soup: BeautifulSoup, selector: str) -> str:
        element = soup.select_one(selector)
        return element.text.strip() if element else ""

    def _get_image_url(self, soup: BeautifulSoup) -> str:
        img = soup.select_one('img[itemprop="image"]')
        return img.get("src", "") if img else ""

    def _extract_title(self, soup: BeautifulSoup) -> str:
        # Try multiple title selectors
        selectors = ["h1.title-name strong", "h1.title-name", "span[itemprop='name']"]
        for selector in selectors:
            title_tag = soup.select_one(selector)
            if title_tag:
                return title_tag.text.strip()
        return "Unknown"

    def _extract_left_side(self, soup: BeautifulSoup) -> Dict:
        left_side = {
            "Alternative Titles": self._extract_alternative_titles(soup),
            "Information": self._extract_information(soup),
            "Statistics": self._extract_statistics(soup),
            "Available At": self._extract_available_at(soup),
            "Resources": self._extract_resources(soup),
        }
        return left_side

    def _clean_ranked_value(self, value: str) -> Optional[str]:
        if "N/A" in value:
            return "N/A"

        # Extract #NUMBER pattern
        import re

        match = re.search(r"#\d+", value)
        if match:
            return match.group(0)
        return None

    def _extract_statistics(self, soup: BeautifulSoup) -> Dict:
        statistics = {}
        stats = soup.find("h2", string="Statistics")
        if stats:
            for div in stats.find_next_siblings("div", class_="spaceit_pad"):
                key = div.find("span", class_="dark_text")
                if key:
                    key_text = key.text.strip().rstrip(":")
                    value = div.text.replace(key.text, "").strip()

                    # Special handling for Ranked field
                    if key_text == "Ranked":
                        clean_rank = self._clean_ranked_value(value)
                        if clean_rank:
                            statistics[key_text] = clean_rank
                        continue

                    # Remove extra spaces for other fields
                    value = " ".join(value.split())
                    statistics[key_text] = value
        return statistics

    def _extract_available_at(self, soup: BeautifulSoup) -> List[Dict]:
        available = []
        available_section = soup.find("h2", string="Available At")
        if available_section:
            links_div = available_section.find_next_sibling("div")
            if links_div:
                for link in links_div.find_all("a"):
                    available.append(
                        {"url": link.get("href", ""), "title": link.text.strip()}
                    )
        return available

    def _extract_resources(self, soup: BeautifulSoup) -> List[Dict]:
        resources = []
        resources_header = soup.find("h2", string="Resources")
        if resources_header:
            external_links = resources_header.find_next_sibling(
                "div", class_="external_links"
            )
            if external_links:
                # Get direct visible links (AniDB, ANN)
                direct_links = external_links.find_all(
                    "a", class_="link", recursive=False
                )
                for link in direct_links:
                    caption = link.find("div", class_="caption")
                    if caption:
                        resources.append(
                            {"url": link.get("href", ""), "title": caption.text.strip()}
                        )

                # Get all hidden links from js-links div
                js_links = external_links.find(
                    "div", {"class": "js-links", "data-rel": "resource"}
                )
                if js_links:
                    hidden_links = js_links.find_all("a", class_="link")
                    for link in hidden_links:
                        caption = link.find("div", class_="caption")
                        if caption:
                            resources.append(
                                {
                                    "url": link.get("href", ""),
                                    "title": caption.text.strip(),
                                }
                            )
        return resources

    def _extract_theme_songs(self, soup: BeautifulSoup) -> Dict:
        songs = {"opening": [], "ending": []}

        # Opening themes
        opening_div = soup.find("div", {"class": "theme-songs js-theme-songs opnening"})
        if opening_div:
            for row in opening_div.find_all("tr"):
                title = row.find("span", class_="theme-song-title")
                artist = row.find("span", class_="theme-song-artist")
                episode = row.find("span", class_="theme-song-episode")

                if title:
                    songs["opening"].append(
                        {
                            "title": title.text.strip('"'),
                            "artist": (
                                artist.text.replace(" by", "").strip() if artist else ""
                            ),
                            "episode": episode.text.strip("()") if episode else "",
                        }
                    )

        # Ending themes
        ending_div = soup.find("div", {"class": "theme-songs js-theme-songs ending"})
        if ending_div:
            for row in ending_div.find_all("tr"):
                title = row.find("span", class_="theme-song-title")
                artist = row.find("span", class_="theme-song-artist")
                episode = row.find("span", class_="theme-song-episode")

                if title:
                    songs["ending"].append(
                        {
                            "title": title.text.strip('"'),
                            "artist": (
                                artist.text.replace(" by", "").strip() if artist else ""
                            ),
                            "episode": episode.text.strip("()") if episode else "",
                        }
                    )

        return songs

    def _extract_related_entries(self, soup: BeautifulSoup) -> Dict:
        related = {"tile": [], "table": {}}

        entries_tile = soup.find("div", class_="entries-tile")
        if entries_tile:
            for entry in entries_tile.find_all("div", class_="entry"):
                # Get relation and format
                relation_div = entry.find("div", class_="relation")
                if relation_div:
                    # Clean and combine relation text
                    relation_text = " ".join(relation_div.get_text(strip=True).split())
                    relation_type = relation_text.split()[0]
                    relation_format = relation_text.split()[1].strip("()")
                    relation = f"{relation_type} ({relation_format})"

                    # Get title and URL
                    title_div = entry.find("div", class_="title")
                    if title_div and title_div.find("a"):
                        link = title_div.find("a")
                        related["tile"].append(
                            {
                                "relation": relation,
                                "title": link.get_text(strip=True),
                                "url": link.get("href", ""),
                            }
                        )

        return related

    def _extract_streaming_platforms(self, soup: BeautifulSoup) -> List[Dict]:
        platforms = []
        broadcasts = soup.find("div", class_="broadcasts")

        if broadcasts:
            for item in broadcasts.find_all("a", class_="broadcast-item"):
                platforms.append(
                    {"url": item.get("href", ""), "title": item.get("title", "")}
                )

        return platforms

    def _extract_genres(self, soup: BeautifulSoup) -> List[str]:
        genres = []
        genre_spans = soup.find_all("span", {"itemprop": "genre"})

        if genre_spans:
            genres = [span.text.strip() for span in genre_spans]

        return genres

    def _extract_rating(self, soup: BeautifulSoup) -> Optional[Dict]:
        rating_value = self._get_text(soup, 'span[itemprop="ratingValue"]')
        rating_count = self._get_text(soup, 'span[itemprop="ratingCount"]')

        if not rating_value or not rating_count:
            return None

        return {
            "_type": "http://schema.org/AggregateRating",
            "ratingValue": rating_value,
            "ratingCount": rating_count,
            "bestRating": "10",
            "worstRating": "1",
        }

    def _extract_breadcrumbs(self, soup: BeautifulSoup) -> List[Dict]:
        return [
            {
                "_type": "http://schema.org/ListItem",
                "item": "https://myanimelist.net/",
                "position": "1",
            },
            {
                "_type": "http://schema.org/ListItem",
                "item": "https://myanimelist.net/anime.php",
                "position": "2",
            },
            {
                "_type": "http://schema.org/ListItem",
                "item": f"https://myanimelist.net/anime/{self.mal_id}",
                "position": "3",
            },
        ]

    def _get_song_elements(self, soup: BeautifulSoup, song_type: str) -> List[tuple]:
        elements = []
        songs_div = soup.find("div", class_=f"theme-songs js-theme-songs {song_type}")
        if songs_div:
            for row in songs_div.select("tr"):
                title = row.select_one(".theme-song-title")
                artist = row.select_one(".theme-song-artist")
                episode = row.select_one(".theme-song-episode")
                if all([title, artist, episode]):
                    elements.append((title, artist, episode))
        return elements

    def _extract_alternative_titles(self, soup: BeautifulSoup) -> Dict:
        titles = {
            "Synonyms": "",
            "Japanese": "",
            "English": "",
            "German": "",
            "Spanish": "",
            "French": "",
        }

        # Get Synonyms and Japanese from main section
        main_titles = soup.find("h2", string="Alternative Titles")
        if main_titles:
            for div in main_titles.find_next_siblings("div", class_="spaceit_pad"):
                label = div.find("span", class_="dark_text")
                if label:
                    key = label.text.strip().rstrip(":")
                    if key in ["Synonyms", "Japanese"]:
                        titles[key] = div.text.replace(label.text, "").strip()

        # Get other language titles from hidden section
        alt_titles = soup.find("div", class_="js-alternative-titles")
        if alt_titles:
            for div in alt_titles.find_all("div", class_="spaceit_pad"):
                label = div.find("span", class_="dark_text")
                if label:
                    key = label.text.strip().rstrip(":")
                    if key in titles:
                        titles[key] = div.text.replace(label.text, "").strip()

        return titles

    def _extract_information(self, soup: BeautifulSoup) -> Dict:
        info = {
            "Type": "",
            "Episodes": "",
            "Status": "",
            "Aired": "",
            "Premiered": "",
            "Broadcast": "",
            "Producers": "",
            "Licensors": "",
            "Studios": "",
            "Source": "",
            "Genres": "",
            "Theme": "",
            "Demographic": "",
            "Duration": "",
            "Rating": "",
        }

        info_section = soup.find("h2", string="Information")
        if info_section:
            for div in info_section.find_next_siblings("div", class_="spaceit_pad"):
                label = div.find("span", class_="dark_text")
                if label:
                    key = label.text.strip().rstrip(":")
                    if key == "Genres":
                        genres = self._extract_genres(soup)
                        info[key] = ", ".join(genres)
                    elif key in info:
                        value = div.text.replace(label.text, "").strip()
                        info[key] = value

        return info

    # This method searches the soup object (which represents the parsed HTML)
    def _extract_characters_voice_actors_list(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts voice actors for each character from the Characters & Staff page, including character and voice actor IDs."""
        voice_actors_data = []

        # Find the link to the Characters & Staff page.
        characters_staff_link = soup.find("a", href=re.compile(r"/characters$"))
        if not characters_staff_link:
            logging.warning(
                f"No Characters & Staff link found for anime ID {self.mal_id}"
            )
            return []  # Return empty list if no link is found.
        characters_staff_url = characters_staff_link["href"]

        try:
            # Fetch the Characters & Staff page.
            char_staff = MALScraper.scrape(characters_staff_url)
            if char_staff is None:
                logging.error("Failed to fetch the Characters & Staff page")
                return []
            char_staff_soup = BeautifulSoup(char_staff["html"], "html.parser")
            # Find each table representing a character.
            character_tables = char_staff_soup.find_all(
                "table", class_="js-anime-character-table"
            )

            for table in character_tables:
                character_obj = {}
                # Extract the character URL from the <a> tag with /character/ in href.
                char_link = table.find("a", href=re.compile(r"/character/"))
                if char_link:
                    char_url = char_link["href"]
                    # Extract the numeric character ID using regex.
                    match = re.search(r"/character/(\d+)", char_url)
                    character_obj["characterId"] = match.group(1) if match else ""
                else:
                    character_obj["characterId"] = ""

                # Initialize the list for this character's voice actors.
                voice_actor_list = []
                # Each voice actor row has the class 'js-anime-character-va-lang'.
                va_rows = table.find_all("tr", class_="js-anime-character-va-lang")
                for row in va_rows:
                    name_td = row.find("td", align="right")
                    if not name_td:
                        continue
                    va_link = name_td.find("a", href=re.compile(r"/people/"))
                    if va_link:
                        va_url = va_link["href"]
                        # Extract the numeric voice actor ID from the URL.
                        match_va = re.search(r"/people/(\d+)", va_url)
                        va_id = match_va.group(1) if match_va else ""
                    else:
                        va_id = ""
                    lang_div = name_td.find("div", class_="js-anime-character-language")
                    va_language = lang_div.text.strip() if lang_div else ""

                    # Append the voice actor's details: ID and language.
                    voice_actor_list.append(
                        {
                            "voiceActorId": va_id,
                            "language": va_language,
                        }
                    )
                character_obj["voiceActors"] = voice_actor_list
                voice_actors_data.append(character_obj)
            return voice_actors_data

        except requests.RequestException as e:
            logging.error(
                f"Failed to fetch Characters & Staff page {characters_staff_url}: {e}"
            )
            return []


class JSONDataStorage(IDataStorage):
    def store(self, data: Dict[str, Any], output_path: str) -> None:
        try:
            # Ensure directory exists
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

            # Write data atomically
            temp_path = f"{output_path}.tmp"

            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            Path(temp_path).replace(output_path)

            logging.debug(f"Successfully wrote data to {output_path}")
        except Exception as e:
            logging.error(f"Failed to write JSON file: {e}")
            raise IOError(f"Failed to write JSON file: {e}")

    def store_all(self, records: List[Dict], output_path: str) -> None:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            for record in records:
                # Write each record as single line without spaces between fields
                f.write(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                )


class MALAnimeScraper:
    def __init__(
        self,
        url_generator: IUrlGenerator,
        data_scraper: IDataScraper,
        data_transformer: IDataTransformer,
        data_storage: IDataStorage,
    ):
        self.url_generator = url_generator
        self.data_scraper = data_scraper
        self.data_transformer = data_transformer
        self.data_storage = data_storage

    def scrape_and_store(self, mal_id: int, output_path: str) -> None:
        try:
            # Generate URL
            url = self.url_generator.generate(mal_id)
            logging.debug(f"Generated URL: {url}")

            # Scrape raw data
            raw_data = self.data_scraper.scrape(url)
            if not raw_data:
                raise ValueError(f"Failed to scrape data for ID {mal_id}")
            logging.debug("Raw data scraped successfully")

            # Transform data
            transformed_data = self.data_transformer.transform(raw_data, mal_id)
            self.data_storage.store(transformed_data, output_path)
            logging.debug(f"Data stored to {output_path}")

        except Exception as e:
            logging.error(f"Error in scrape_and_store: {e}")
            raise

    def scrape(self, mal_id: int) -> Dict[str, Any]:
        url = self.url_generator.generate(mal_id)
        raw_data = self.data_scraper.scrape(url)
        return self.data_transformer.transform(raw_data, mal_id)

    async def scrape_all_anime(
        self,
        output_prefix: str = "anime_data",
        checkpoint_path: str = "anime_checkpoint.json",
        save_checkpoint_interval: int = 5,  # Save checkpoint after every N successful scrapes
    ) -> None:
        """
        Asynchronously paginate through all anime and scrape each one.
        Supports checkpointing to resume from where it left off.

        Args:
            output_prefix (str): The prefix for the output files
            checkpoint_path (str): Path to the checkpoint file
            save_checkpoint_interval (int): How often to save the checkpoint
        """

        # Initialize checkpoint handler
        checkpoint = AnimeCheckpointHandler(checkpoint_path)
        successful_scrapes = 0
        Path(os.path.dirname(output_prefix)).mkdir(parents=True, exist_ok=True)

        async with aiohttp.ClientSession() as client:
            async for anime in paginate_anime(client, checkpoint):
                try:
                    anime_id = anime["id"]

                    # Skip if already completed
                    if checkpoint.is_completed(anime_id):
                        logging.debug(
                            f"Skipping already processed anime ID: {anime_id}"
                        )
                        continue

                    # Scrape the anime
                    logging.info(f"Scraping anime ID: {anime_id} - {anime['title']}")
                    record = self.scrape(anime_id)

                    if record:
                        # Define a unique output path for each anime
                        file_path = f"{output_prefix}/{anime_id}.json"
                        self.data_storage.store(record, file_path)

                        # Mark as completed and update checkpoint
                        checkpoint.mark_completed(anime_id)
                        successful_scrapes += 1

                        # Periodically save the checkpoint
                        if successful_scrapes % save_checkpoint_interval == 0:
                            checkpoint.save_checkpoint()

                        logging.info(
                            f"Scraped and stored anime {anime_id} (Total: {checkpoint.get_completed_count()})"
                        )

                except Exception as e:
                    logging.error(f"Error scraping anime {anime_id}: {e}")
                    # Pause a bit longer on error
                    await asyncio.sleep(5)
                    continue

            # Final checkpoint save after complete
            checkpoint.save_checkpoint()
            logging.info(
                f"Anime scraping completed. Total processed: {checkpoint.get_completed_count()}"
            )

    def get_anime_checkpoint_status(
        self, checkpoint_path: str = "anime_checkpoint.json"
    ) -> Dict:
        """Get information about the current anime checkpoint status."""
        from .anime_checkpoint import AnimeCheckpointHandler

        checkpoint = AnimeCheckpointHandler(checkpoint_path)
        letter, page = checkpoint.get_pagination_state()

        return {
            "checkpoint_file": checkpoint_path,
            "completed_ids_count": checkpoint.get_completed_count(),
            "current_letter": letter,
            "current_page": page,
        }
