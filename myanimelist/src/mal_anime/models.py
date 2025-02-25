from dataclasses import dataclass
from typing import Dict, List


@dataclass
class AnimeDetails:
    id: int
    title: str
    url: str
    microdata: List[Dict]
    leftSide: Dict
    relatedEntries: Dict
    themeSongs: Dict
    streamingPlatforms: List[Dict]


@dataclass
class AnimeRecord:
    _airbyte_ab_id: str
    _airbyte_emitted_at: int
    _airbyte_data: AnimeDetails

    def to_dict(self) -> Dict:
        return {
            "_airbyte_ab_id": self._airbyte_ab_id,
            "_airbyte_emitted_at": self._airbyte_emitted_at,
            "_airbyte_data": {
                "id": self._airbyte_data.id,
                "title": self._airbyte_data.title,
                "url": self._airbyte_data.url,
                "microdata": self._airbyte_data.microdata,
                "leftSide": self._airbyte_data.leftSide,
                "relatedEntries": self._airbyte_data.relatedEntries,
                "themeSongs": self._airbyte_data.themeSongs,
                "streamingPlatforms": self._airbyte_data.streamingPlatforms
            }
        }
