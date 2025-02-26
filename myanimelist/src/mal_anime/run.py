import asyncio
import argparse
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import os
from .scraper import (
    MALAnimeScraper,
    MALUrlGenerator,
    MALScraper,
    MALDataTransformer,
    GCSDataStorage,
)
from .people_scraper import VADataTransformer, MALPeopleScraper, GCSDataStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log")],
)


class ScraperManager:
    """
    Manages concurrent execution of anime and people scrapers.
    Handles configuration, execution, and status tracking.
    """

    def __init__(
        self,
        anime_bucket: str = "mal_anime",
        people_bucket: str = "mal_people",
        project_id: str = "fep-staging",
        anime_output_prefix: str = "anime_data",
        people_output_prefix: str = "people_data",
        anime_checkpoint: str = "anime_checkpoint.json",
        people_checkpoint: str = "people_checkpoint.json",
        save_interval: int = 1,
    ):
        self.anime_bucket = anime_bucket
        self.people_bucket = people_bucket
        self.project_id = project_id
        self.anime_output_prefix = anime_output_prefix
        self.people_output_prefix = people_output_prefix
        self.anime_checkpoint = anime_checkpoint
        self.people_checkpoint = people_checkpoint
        self.save_interval = save_interval
        self.anime_scraper = None
        self.people_scraper = None

    def setup_anime_scraper(self):
        """Initialize and return the anime scraper with GCS storage."""
        self.anime_scraper = MALAnimeScraper(
            url_generator=MALUrlGenerator(),
            data_scraper=MALScraper(),
            data_transformer=MALDataTransformer(),
            data_storage=GCSDataStorage(self.anime_bucket, self.project_id),
        )
        return self.anime_scraper

    def setup_people_scraper(self):
        """Initialize and return the people/voice actor scraper with GCS storage."""
        self.people_scraper = MALPeopleScraper(
            data_transformer=VADataTransformer(),
            data_storage=GCSDataStorage(self.people_bucket, self.project_id),
        )
        return self.people_scraper

    async def run_scrapers(self, run_anime=True, run_people=True, resume=True):
        """
        Run the specified scrapers concurrently.

        Args:
            run_anime: Whether to run the anime scraper
            run_people: Whether to run the people scraper
            resume: Whether to resume from checkpoint or start fresh
        """
        tasks = []

        # Setup output directories
        Path(self.anime_output_prefix).parent.mkdir(parents=True, exist_ok=True)
        Path(self.people_output_prefix).parent.mkdir(parents=True, exist_ok=True)

        # Delete checkpoint files if not resuming
        if not resume:
            if run_anime and Path(self.anime_checkpoint).exists():
                Path(self.anime_checkpoint).unlink()
                logging.info(f"Deleted anime checkpoint file to start fresh")

            if run_people and Path(self.people_checkpoint).exists():
                Path(self.people_checkpoint).unlink()
                logging.info(f"Deleted people checkpoint file to start fresh")

        # Setup and start anime scraper if requested
        if run_anime:
            if not self.anime_scraper:
                self.setup_anime_scraper()

            logging.info("Starting anime scraper")
            anime_task = asyncio.create_task(
                self.anime_scraper.scrape_all_anime(
                    output_prefix=self.anime_output_prefix,
                    checkpoint_path=self.anime_checkpoint,
                    save_checkpoint_interval=self.save_interval,
                )
            )
            tasks.append(anime_task)

        # Setup and start people scraper if requested
        if run_people:
            if not self.people_scraper:
                self.setup_people_scraper()

            logging.info("Starting people scraper")
            people_task = asyncio.create_task(
                self.people_scraper.scrape_all_people(
                    output_prefix=self.people_output_prefix,
                    checkpoint_path=self.people_checkpoint,
                    save_checkpoint_interval=self.save_interval,
                )
            )
            tasks.append(people_task)

        # Wait for all scrapers to complete
        if tasks:
            await asyncio.gather(*tasks)
            logging.info("All scrapers completed")
        else:
            logging.warning("No scrapers were started")

    def get_status(self) -> Dict[str, Any]:
        """Get the current status of anime and people scrapers."""
        status = {
            "anime": None,
            "people": None,
        }

        # Get anime scraper status if checkpoint exists
        if Path(self.anime_checkpoint).exists():
            from .anime_checkpoint import AnimeCheckpointHandler

            checkpoint = AnimeCheckpointHandler(self.anime_checkpoint)
            letter, page = checkpoint.get_pagination_state()
            status["anime"] = {
                "completed_count": checkpoint.get_completed_count(),
                "current_letter": letter,
                "current_page": page,
            }

        # Get people scraper status if checkpoint exists
        if Path(self.people_checkpoint).exists():
            from .people_checkpoint import PeopleCheckpointHandler

            checkpoint = PeopleCheckpointHandler(self.people_checkpoint)
            letter, page = checkpoint.get_pagination_state()
            status["people"] = {
                "completed_count": checkpoint.get_completed_count(),
                "current_letter": letter,
                "current_page": page,
            }

        return status


async def main_async(args):
    """Asynchronous main function that runs the scrapers based on arguments."""
    manager = ScraperManager(
        anime_bucket=args.anime_bucket,
        people_bucket=args.people_bucket,
        project_id=args.project_id,
        anime_output_prefix=args.anime_output,
        people_output_prefix=args.people_output,
        anime_checkpoint=args.anime_checkpoint,
        people_checkpoint=args.people_checkpoint,
        save_interval=args.interval,
    )

    if args.status:
        # Just print status and exit
        status = manager.get_status()
        print("\nScraper Status:")
        if status["anime"]:
            print(
                f"Anime: {status['anime']['completed_count']} entries scraped, "
                f"current position: {status['anime']['current_letter']}-{status['anime']['current_page']}"
            )
        else:
            print("Anime: No checkpoint found")

        if status["people"]:
            print(
                f"People: {status['people']['completed_count']} entries scraped, "
                f"current position: {status['people']['current_letter']}-{status['people']['current_page']}"
            )
        else:
            print("People: No checkpoint found")
    else:
        # Run the scrapers
        await manager.run_scrapers(
            run_anime=args.anime, run_people=args.people, resume=not args.no_resume
        )


def main():
    """Parse command line arguments and run the scraper manager."""
    parser = argparse.ArgumentParser(description="MyAnimeList Scraper Manager")

    # Simple core options
    parser.add_argument("--anime", action="store_true", help="Run the anime scraper")
    parser.add_argument(
        "--people", action="store_true", help="Run the people/voice actor scraper"
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Don't resume from checkpoint, start fresh",
    )

    args = parser.parse_args()

    # Set default values for the parameters we removed from CLI
    args.project_id = "fep-staging"
    args.interval = 10
    args.anime_bucket = "mal_anime"
    args.people_bucket = "mal_people"
    args.anime_output = "anime_data"
    args.people_output = "data"
    args.anime_checkpoint = "src/mal_anime/anime_checkpoint.json"
    args.people_checkpoint = "src/mal_anime/people_checkpoint.json"
    args.status = False

    # If neither anime nor people specified, run both by default
    if not args.anime and not args.people:
        args.anime = True
        args.people = True

    # Run the async main function
    asyncio.run(main_async(args))


if __name__ == "__main__":
    print(os.getcwd())
    main()
