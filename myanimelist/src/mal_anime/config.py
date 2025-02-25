from pathlib import Path

BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DEFAULT_OUTPUT_FILE = OUTPUT_DIR / "mal_scraped_data.json"
