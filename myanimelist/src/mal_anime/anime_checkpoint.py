# src/mal_anime/anime_checkpoint.py

import json
import logging
from pathlib import Path
from typing import Set, Optional


class AnimeCheckpointHandler:
    """
    Handles saving and loading checkpoint state for the anime scraper.
    Tracks which anime IDs have been successfully processed and the pagination state.
    """

    def __init__(self, checkpoint_file: str = "anime_checkpoint.json"):
        self.checkpoint_file = checkpoint_file
        self.completed_ids: Set[int] = set()
        # Pagination state: which letter and page we're currently on
        self.current_letter: Optional[str] = None
        self.current_page: int = 0
        self.load_checkpoint()

    def load_checkpoint(self) -> None:
        """Load the checkpoint file if it exists."""
        try:
            if Path(self.checkpoint_file).exists():
                with open(self.checkpoint_file, "r") as f:
                    checkpoint_data = json.load(f)
                    self.completed_ids = set(checkpoint_data.get("completed_ids", []))
                    self.current_letter = checkpoint_data.get("current_letter")
                    self.current_page = checkpoint_data.get("current_page", 0)
                logging.info(
                    f"Loaded anime checkpoint with {len(self.completed_ids)} completed IDs, "
                    f"position: letter={self.current_letter}, page={self.current_page}"
                )
            else:
                logging.info("No anime checkpoint file found, starting fresh")
        except Exception as e:
            logging.error(f"Error loading anime checkpoint file: {e}")
            # If there's an error, start with an empty set to be safe
            self.completed_ids = set()
            self.current_letter = None
            self.current_page = 0

    def save_checkpoint(self) -> None:
        """Save the current checkpoint state."""
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
            logging.debug(
                f"Anime checkpoint saved: {len(self.completed_ids)} IDs, "
                f"position: letter={self.current_letter}, page={self.current_page}"
            )
        except Exception as e:
            logging.error(f"Error saving anime checkpoint file: {e}")

    def mark_completed(self, anime_id: int) -> None:
        """Mark an anime ID as successfully processed."""
        self.completed_ids.add(anime_id)

    def update_pagination(self, letter: str, page: int) -> None:
        """Update the current pagination position and save the checkpoint."""
        self.current_letter = letter
        self.current_page = page
        self.save_checkpoint()

    def is_completed(self, anime_id: int) -> bool:
        """Check if an anime ID has already been successfully processed."""
        return anime_id in self.completed_ids

    def get_pagination_state(self) -> tuple:
        """Get the current pagination state (letter, page)."""
        return self.current_letter, self.current_page

    def get_completed_count(self) -> int:
        """Get the count of completed IDs."""
        return len(self.completed_ids)
