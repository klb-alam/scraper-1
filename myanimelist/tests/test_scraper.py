import pytest
from pathlib import Path
from src.mal_anime.scraper import (
    MALUrlGenerator,
    MALScraper,
    MALDataTransformer,
    JSONDataStorage,
    MALAnimeScraper
)

@pytest.fixture
def url_generator():
    return MALUrlGenerator()

@pytest.fixture
def data_scraper():
    return MALScraper()

@pytest.fixture
def data_transformer():
    return MALDataTransformer()

@pytest.fixture
def data_storage():
    return JSONDataStorage()

@pytest.fixture
def scraper(url_generator, data_scraper, data_transformer, data_storage):
    return MALAnimeScraper(
        url_generator=url_generator,
        data_scraper=data_scraper,
        data_transformer=data_transformer,
        data_storage=data_storage
    )

def test_url_generation(url_generator):
    assert url_generator.generate(30123) == "https://myanimelist.net/anime/30123"

def test_scraper_initialization(scraper):
    assert isinstance(scraper.url_generator, MALUrlGenerator)
    assert isinstance(scraper.data_scraper, MALScraper)
    assert isinstance(scraper.data_transformer, MALDataTransformer)
    assert isinstance(scraper.data_storage, JSONDataStorage)

def test_data_storage(data_storage, tmp_path):
    test_data = {"test": "data"}
    output_file = tmp_path / "test.json"
    data_storage.store(test_data, str(output_file))
    assert output_file.exists()
