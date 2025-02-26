import re
import aiohttp
import asyncio
import logging
from typing import AsyncGenerator, Dict, Any

BASE_URL = "https://myanimelist.net"
PAGE_SIZE = 50
# Using 1-based indexing letters (excluding ".")
LETTERS = [l for l in ".ABCDEFGHIJKLMNOPQRSTUVWXYZ" if l != "."]

# This regex will find IDs from URLs like '/people/12345/'
STAFF_ID_PATTERN = re.compile(r"/people/(\d+)/")

# New regex pattern to find anime entries
ANIME_PATTERN = re.compile(
    r'<a class="hoverinfo_trigger[^"]+" href="https://myanimelist\.net/anime/(\d+)[^"]+"[^>]+><strong>([^<]+)</strong>'
)


async def paginate_anime(
    client: aiohttp.ClientSession,
    checkpoint_handler=None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Asynchronously paginate through anime listings.
    Yields a dictionary with id, title, and url for each anime.

    Args:
        client: aiohttp client session to use for requests
        checkpoint_handler: Optional checkpoint handler to track progress

    Yields:
        Dictionary containing id, title, and url of each anime found
    """
    # Get starting position from checkpoint if available
    start_letter_idx = 0
    start_page = 0

    if checkpoint_handler:
        letter, page = checkpoint_handler.get_pagination_state()
        if letter and letter in LETTERS:
            start_letter_idx = LETTERS.index(letter)
            start_page = page
            logging.info(
                f"Resuming anime pagination from letter={letter} (index={start_letter_idx}), page={page}"
            )

    # Start from where we left off in the alphabet
    for idx, letter in enumerate(LETTERS[start_letter_idx:], start_letter_idx):
        page = start_page if idx == start_letter_idx else 0

        while True:
            url = f"{BASE_URL}/anime.php"
            params = {"letter": letter, "show": page * PAGE_SIZE}
            logging.info(f"Fetching anime: letter={letter}, page={page}")

            try:
                async with client.get(url, params=params) as response:
                    response.raise_for_status()
                    text = await response.text()
            except Exception as e:
                logging.error(f"Error fetching anime page {letter}-{page}: {e}")
                # Wait a bit and retry this page
                await asyncio.sleep(5)
                continue

            # Update checkpoint before processing this page
            if checkpoint_handler:
                checkpoint_handler.update_pagination(letter, page)

            # Use the new pattern to find anime entries
            anime_entries = ANIME_PATTERN.findall(text)

            if not anime_entries:
                logging.info(f"No more anime found for letter {letter}")
                break

            for anime_id, anime_title in anime_entries:
                anime_id = int(anime_id)
                anime_url = f"{BASE_URL}/anime/{anime_id}"

                # Skip already processed IDs if checkpoint is available
                if checkpoint_handler and checkpoint_handler.is_completed(anime_id):
                    logging.debug(f"Skipping already processed anime ID: {anime_id}")
                    continue

                yield {
                    "id": anime_id,
                    "title": anime_title,
                    "url": anime_url,
                }

            if len(anime_entries) < PAGE_SIZE:
                logging.info(f"Reached end of letter {letter} at page {page}")
                break

            page += 1
            # Small delay to avoid hitting rate limits
            await asyncio.sleep(1)

    logging.info("Finished paginating through all anime")


async def paginate_people(
    client: aiohttp.ClientSession,
    checkpoint_handler=None,
) -> AsyncGenerator[str, None]:
    """
    Asynchronously paginate through people listings.
    Yields the URL of each person.

    Args:
        client: aiohttp client session to use for requests
        checkpoint_handler: Optional checkpoint handler to track progress

    Yields:
        URL of each person found in the pagination
    """
    # Get starting position from checkpoint if available
    start_letter_idx = 0
    start_page = 0

    if checkpoint_handler:
        letter, page = checkpoint_handler.get_pagination_state()
        if letter and letter in LETTERS:
            start_letter_idx = LETTERS.index(letter)
            start_page = page
            logging.info(
                f"Resuming pagination from letter={letter} (index={start_letter_idx}), page={page}"
            )

    # Start from where we left off in the alphabet
    for idx, letter in enumerate(LETTERS[start_letter_idx:], start_letter_idx):
        page = start_page if idx == start_letter_idx else 0

        while True:
            url = f"{BASE_URL}/people.php"
            params = {"letter": letter, "show": page * PAGE_SIZE}
            logging.info(f"Fetching people: letter={letter}, page={page}")

            try:
                async with client.get(url, params=params) as response:
                    response.raise_for_status()
                    text = await response.text()
            except Exception as e:
                logging.error(f"Error fetching people page {letter}-{page}: {e}")
                # Wait a bit and retry this page
                await asyncio.sleep(5)
                continue

            # Update checkpoint before processing this page
            if checkpoint_handler:
                checkpoint_handler.update_pagination(letter, page)

            person_ids = STAFF_ID_PATTERN.findall(text)
            if not person_ids:
                logging.info(f"No more people found for letter {letter}")
                break

            for person_id in person_ids:
                person_url = f"{BASE_URL}/people/{person_id}/"

                # Skip already processed IDs if checkpoint is available
                if checkpoint_handler and checkpoint_handler.is_completed(
                    int(person_id)
                ):
                    logging.debug(f"Skipping already processed people ID: {person_id}")
                    continue

                yield person_url

            if len(person_ids) < PAGE_SIZE:
                logging.info(f"Reached end of letter {letter} at page {page}")
                break

            page += 1
            # Small delay to avoid hitting rate limits
            await asyncio.sleep(1)

    logging.info("Finished paginating through all people")
