# src/mal_anime/anime_scraper_cli.py
import asyncio
import argparse
import logging
import json
from pathlib import Path
from typing import Set, Dict, Any
import aiohttp

from .scraper import (
    MALUrlGenerator,
    MALScraper,
    MALDataTransformer,
    JSONDataStorage,
    MALAnimeScraper,
)
from .utils import paginate_anime


class AnimeCheckpointHandler:
    """Simple checkpoint handler for tracking anime scraping progress"""

    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.completed_ids: Set[int] = set()
        self.current_letter = None
        self.current_page = 0
        self.load_checkpoint()

    def load_checkpoint(self):
        try:
            if Path(self.checkpoint_file).exists():
                with open(self.checkpoint_file, "r") as f:
                    data = json.load(f)
                    self.completed_ids = set(data.get("completed_ids", []))
                    self.current_letter = data.get("current_letter")
                    self.current_page = data.get("current_page", 0)
                logging.info(
                    f"Loaded checkpoint with {len(self.completed_ids)} completed IDs"
                )
            else:
                logging.info("No checkpoint file found, starting fresh")
        except Exception as e:
            logging.error(f"Error loading checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, "w") as f:
                json.dump(
                    {
                        "completed_ids": list(self.completed_ids),
                        "current_letter": self.current_letter,
                        "current_page": self.current_page,
                    },
                    f,
                )
            logging.debug("Checkpoint saved")
        except Exception as e:
            logging.error(f"Error saving checkpoint: {e}")

    def mark_completed(self, anime_id: int):
        self.completed_ids.add(anime_id)

    def update_pagination(self, letter: str, page: int):
        self.current_letter = letter
        self.current_page = page
        self.save_checkpoint()

    def is_completed(self, anime_id: int) -> bool:
        return anime_id in self.completed_ids

    def get_pagination_state(self):
        return self.current_letter, self.current_page

    def get_completed_count(self):
        return len(self.completed_ids)


async def scrape_all_anime(
    output_dir: str,
    checkpoint_file: str = "anime_checkpoint.json",
    save_interval: int = 10,
    resume: bool = True,
):
    """
    Scrape all anime from A-Z using pagination

    Args:
        output_dir: Directory to save scraped files
        checkpoint_file: File to track progress
        save_interval: How often to save checkpoint
        resume: Whether to resume from checkpoint
    """
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Initialize checkpoint handler
    if not resume and Path(checkpoint_file).exists():
        Path(checkpoint_file).unlink()
        logging.info(f"Deleted old checkpoint file to start fresh")

    checkpoint = AnimeCheckpointHandler(checkpoint_file)

    # Initialize scraper
    scraper = MALAnimeScraper(
        url_generator=MALUrlGenerator(),
        data_scraper=MALScraper(),
        data_transformer=MALDataTransformer(),
        data_storage=JSONDataStorage(),
    )

    successful_scrapes = 0

    async with aiohttp.ClientSession() as client:
        async for anime in paginate_anime(client, checkpoint):
            anime_id = anime["id"]
            anime_title = anime["title"]

            try:
                logging.info(f"Scraping anime ID: {anime_id} - {anime_title}")

                # Scrape the anime
                record = scraper.scrape(anime_id)

                # Save to a file
                file_path = output_path / f"{anime_id}.json"
                scraper.data_storage.store(record, str(file_path))

                # Update checkpoint
                checkpoint.mark_completed(anime_id)
                successful_scrapes += 1

                if successful_scrapes % save_interval == 0:
                    logging.info(f"Saved checkpoint after {successful_scrapes} scrapes")
                    checkpoint.save_checkpoint()

                # Small delay to avoid rate limiting
                await asyncio.sleep(1)

            except Exception as e:
                logging.error(f"Error scraping anime {anime_id}: {str(e)}")
                await asyncio.sleep(5)  # Longer delay on error

    # Final checkpoint save
    checkpoint.save_checkpoint()
    logging.info(f"Finished scraping {checkpoint.get_completed_count()} anime titles")


def main():
    parser = argparse.ArgumentParser(description="Scrape all anime from MyAnimeList")
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="output/anime",
        help="Output directory for scraped anime data",
    )
    parser.add_argument(
        "-c",
        "--checkpoint",
        type=str,
        default="anime_checkpoint.json",
        help="Checkpoint file to track progress",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=10,
        help="Save checkpoint interval (number of successful scrapes)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume from checkpoint, start fresh",
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("anime_scraper.log")],
    )

    # Run the scraper
    asyncio.run(
        scrape_all_anime(
            output_dir=args.output,
            checkpoint_file=args.checkpoint,
            save_interval=args.interval,
            resume=not args.no_resume,
        )
    )


if __name__ == "__main__":
    main()
