# Setup

```
cd /home/x/scraper/new_scrapper
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

# Run Scraper

```
# Default (read input from config file, output to default file in config)
python -m src.mal_anime

# Single ID
python -m src.mal_anime 52034

# Multiple IDs
python -m src.mal_anime 52034 58259 51009

# Custom output file
python -m src.mal_anime -o output/custom_output.json 52034 58259

# With config file
python -m src.mal_anime -c src/mal_anime/config/config.yaml
or
python -m src.mal_anime -c src/mal_anime/config/config.yaml -o output/data.jsonl
```

# Run Tests

```
cd new_scrapper
pip install -e .
pytest tests/
```
