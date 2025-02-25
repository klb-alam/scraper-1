import re
import aiohttp
import asyncio

BASE_URL = "https://myanimelist.net"
PAGE_SIZE = 50
# Using 1-based indexing letters (excluding ".")
LETTERS = [l for l in ".ABCDEFGHIJKLMNOPQRSTUVWXYZ" if l != "."]

# This regex will find IDs from URLs like '/people/12345/'
STAFF_ID_PATTERN = re.compile(r"/people/(\d+)/")


async def paginate_people(client: aiohttp.ClientSession):
    """
    Asynchronously paginate through people listings.
    Yields the URL of each person.
    if the total people ids is less than 50 mean last page.
    """
    for letter in LETTERS:
        page = 0
        while True:
            url = f"{BASE_URL}/people.php"
            params = {"letter": letter, "show": page * PAGE_SIZE}
            async with client.get(url, params=params) as response:
                response.raise_for_status()
                text = await response.text()

            person_ids = STAFF_ID_PATTERN.findall(text)
            if not person_ids:
                break

            for person_id in person_ids:
                yield f"{BASE_URL}/people/{person_id}/"

            if len(person_ids) < PAGE_SIZE:
                break
            page += 1
