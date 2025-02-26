# src/mal_anime/app.py
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
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


class ScrapePeopleRequest(BaseModel):
    output_prefix: str = "data"
    checkpoint_path: str = "people_checkpoint.json"
    resume: bool = True
    save_interval: int = 5


# --- Helper Functions ---
def setup_anime_scraper():
    return MALAnimeScraper(
        url_generator=MALUrlGenerator(),
        data_scraper=MALScraper(),
        data_transformer=MALDataTransformer(),
        data_storage=JSONDataStorage(),
    )


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
async def scrape_people(
    background_tasks: BackgroundTasks,
    request_data: ScrapePeopleRequest = ScrapePeopleRequest(),
):
    """
    Starts the voice actor scraping process with checkpoint support.

    This runs as a background task and stores data in Google Cloud Storage.
    Uses checkpointing to allow resuming the scraping process if interrupted.

    Args:
        output_prefix: The prefix for the GCS storage path
        checkpoint_path: Path to the checkpoint file
        resume: Whether to resume from checkpoint or start fresh
        save_interval: How often to save checkpoint (after every N successful scrapes)
    """
    try:
        scraper = setup_people_scraper()

        # If not resuming, delete the checkpoint file if it exists
        if not request_data.resume:
            import os

            if os.path.exists(request_data.checkpoint_path):
                os.remove(request_data.checkpoint_path)
                logging.info(
                    f"Deleted checkpoint file {request_data.checkpoint_path} to start fresh"
                )

        # Start the background task
        background_tasks.add_task(
            scraper.scrape_all_people,
            output_prefix=request_data.output_prefix,
            checkpoint_path=request_data.checkpoint_path,
            save_checkpoint_interval=request_data.save_interval,
        )

        return {
            "message": "Voice actor scraping initiated with checkpointing",
            "checkpoint_path": request_data.checkpoint_path,
            "output_prefix": request_data.output_prefix,
            "resuming": request_data.resume,
        }
    except Exception as e:
        logging.error(f"Error setting up voice actor scraper: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error setting up voice actor scraper: {e}"
        )


@app.get("/people-checkpoint-status")
async def people_checkpoint_status(
    checkpoint_path: str = Query(
        "people_checkpoint.json", description="Path to the checkpoint file"
    )
):
    """
    Get information about the current checkpoint status for people scraper.
    """
    try:
        scraper = setup_people_scraper()
        status = scraper.get_checkpoint_status(checkpoint_path)
        return status
    except Exception as e:
        logging.error(f"Error reading checkpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading checkpoint: {e}")
