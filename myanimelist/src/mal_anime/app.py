# src/mal_anime/app.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
import logging
from pathlib import Path
from .scraper import (
    MALAnimeScraper,
    MALUrlGenerator,
    MALScraper,
    MALDataTransformer,
    JSONDataStorage,
)
from .people_scraper import VADataTransformer, MALPeopleScraper, GCSDataStorage

app = FastAPI()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log")],
)


# --- Pydantic Models (Data Validation) ---
class ScrapeAnimeRequest(BaseModel):
    mal_ids: List[int]
    output_path: str = "output/mal_titles.jsonl"

    class Config:
        min_items_mal_ids = 1


# --- Helper Functions ---
def setup_anime_scraper():
    return MALAnimeScraper(
        url_generator=MALUrlGenerator(),
        data_scraper=MALScraper(),
        data_transformer=MALDataTransformer(),
        data_storage=JSONDataStorage(),
    )


## redeployed!
def setup_people_scraper():
    return MALPeopleScraper(
        data_transformer=VADataTransformer(),
        data_storage=GCSDataStorage("mal_people", "fep-staging"),
    )


# --- Routes ---


@app.get("/health", response_class=JSONResponse)
async def healthcheck():
    """
    Simple healthcheck endpoint.
    """
    return {"status": "healthy"}


@app.post("/scrape-anime")
async def scrape_anime(request_data: ScrapeAnimeRequest):
    """
    Starts the anime scraping process. Uses Pydantic for request validation.
    """
    mal_ids = request_data.mal_ids
    output_path = request_data.output_path
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    scraper = setup_anime_scraper()
    records = []
    for mal_id in mal_ids:
        logging.info(f"Scraping MAL ID: {mal_id}")
        try:
            record = scraper.scrape(mal_id)
            records.append(record)
        except Exception as e:
            logging.error(f"Error scraping MAL ID {mal_id}: {e}")
            raise HTTPException(
                status_code=500, detail=f"Error scraping MAL ID {mal_id}: {e}"
            )

    scraper.data_storage.store_all(records, output_path)
    logging.info(f"Scraping completed. Data saved to {output_path}")

    return {
        "message": "Anime scraping started successfully",
        "output_path": output_path,
    }


@app.post("/scrape-people")
async def scrape_people(background_tasks: BackgroundTasks):
    """
    Starts the voice actor scraping process (runs in the background).
    Stores data in Google Cloud Storage bucket.
    """

    try:
        scraper = setup_people_scraper()
        background_tasks.add_task(scraper.scrape_all_people)
        return {"message": "Voice actor scraping initiated."}
    except Exception as e:
        logging.error(f"Error setting up voice actor scraper: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error setting up voice actor scraper: {e}"
        )
