import logging
import argparse
import yaml
import json
from pathlib import Path
from typing import List, Dict, Any

from .scraper import (
    MALUrlGenerator,
    MALScraper,
    MALDataTransformer,
    JSONDataStorage,
    MALAnimeScraper,
)
from .config import DEFAULT_OUTPUT_FILE


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("scraper.log")],
    )


def parse_args():
    parser = argparse.ArgumentParser(description="MAL Anime Scraper")
    parser.add_argument("mal_ids", type=int, nargs="*", help="MAL IDs to scrape")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="src/mal_anime/config/config.yaml",
        help="Config file path",
    )
    parser.add_argument(
        "-o", "--output", type=str, default="output/data.jsonl", help="Output file path"
    )
    return parser.parse_args()


def load_config(config_path: str) -> Dict:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return {
        "mal_ids": config.get("mal_ids", []),
        "output_path": config.get("output_path", "output/data.jsonl"),
    }


def write_records(records: List[Dict], output_path: str):
    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            # Write each record as single line
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    setup_logging()
    args = parse_args()

    # Load config if specified
    config = (
        load_config(args.config)
        if args.config
        else {"mal_ids": [], "output_path": None}
    )

    # Get MAL IDs from config and/or CLI
    mal_ids = []
    mal_ids.extend(config["mal_ids"])
    if args.mal_ids:
        mal_ids.extend(args.mal_ids)

    if not mal_ids:
        raise ValueError("No MAL IDs provided via config or command line")

    # Use output path from: CLI arg (if provided) -> config -> default
    output_path = (
        args.output if args.output != "output/data.jsonl" else config["output_path"]
    )
    output_path = output_path or "output/data.jsonl"

    logging.debug("Starting MAL Anime Scraper")
    logging.info(f"Starting scraping for MAL IDs: {mal_ids}")

    scraper = MALAnimeScraper(
        url_generator=MALUrlGenerator(),
        data_scraper=MALScraper(),
        data_transformer=MALDataTransformer(),
        data_storage=JSONDataStorage(),
    )

    records = []
    for mal_id in mal_ids:
        logging.info(f"Scraping MAL ID: {mal_id}")
        try:
            record = scraper.scrape(mal_id)
            records.append(record)
            logging.info(f"Successfully scraped data for ID: {mal_id}")
        except Exception as e:
            logging.error(f"Error scraping MAL ID {mal_id}: {e}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    scraper.data_storage.store_all(records, output_path)
    logging.info(f"Scraping completed. Data saved to {output_path}")


if __name__ == "__main__":
    main()
