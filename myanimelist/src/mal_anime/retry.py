import time
import logging
import requests
from tenacity import retry, wait_exponential, stop_never


# Custom session that delays requests if called too soon
class InterceptedSession(requests.Session):
    def __init__(self, sleep_in_ms):
        super().__init__()
        self.sleep_in_ms = sleep_in_ms  # Minimum delay between requests in milliseconds
        self.last_requested_at = 0

    def request(self, method, url, **kwargs):
        now = time.time() * 1000  # current time in milliseconds
        delta = now - self.last_requested_at
        if delta < self.sleep_in_ms:
            delay = (self.sleep_in_ms - delta) / 1000  # convert to seconds
            time.sleep(delay)
        self.last_requested_at = time.time() * 1000
        logging.info("Loading %s", url)
        return super().request(method, url, **kwargs)


# Create an instance with a delay (e.g., 1000ms)
session = InterceptedSession(sleep_in_ms=1000)


# Define a function that uses tenacity to retry infinitely with exponential backoff
@retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_never)
def make_request(method, url, **kwargs):
    response = session.request(method, url, **kwargs)
    response.raise_for_status()  # raise an error for bad responses
    return response
