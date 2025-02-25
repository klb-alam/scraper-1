from abc import ABC, abstractmethod
from typing import Dict, List, Any


class IUrlGenerator(ABC):
    @abstractmethod
    def generate(self, mal_id: int) -> str:
        """Generate URL for given MAL ID"""
        pass


class IDataScraper(ABC):
    @abstractmethod
    def scrape(self, url: str) -> Dict[str, Any]:
        pass


class IDataTransformer(ABC):
    @abstractmethod
    def transform(
        self, raw_data: Dict[str, Any], mal_id: int
    ) -> Dict[str, Any]:
        pass


class IDataStorage(ABC):
    @abstractmethod
    def store(self, data: Dict[str, Any], output_path: str) -> None:
        """Store data in specified format"""
        pass
