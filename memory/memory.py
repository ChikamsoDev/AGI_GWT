# memory/memory.py

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from GWT.base_module import BaseModule
from GWT.messages import Message

logger = logging.getLogger(__name__)
EPISODE_GAP_SECONDS = 30
MAX_EPISODE_DURATION_SECONDS = 45 * 60  
# -----------------------------
# Helpers
# -----------------------------

def clamp(value: float,
          minimum: float = 0.0,
          maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


@dataclass
class Episode:
    source: str
    content: List[str] = field(default_factory=list) 
    
    id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    salience: float = 0.0
    prediction_error: float = 0.0

    semantic_tags: List[str] = field(default_factory=list)

    consolidation_state: str = "raw"
    retrieval_count: int = 0

    source_flag: str = "perceived"
    last_retrieved: Optional[datetime] = None

    # Placeholders for future memory systems
    embedding: Optional[list] = None
    perceptual_hashes: Optional[dict] = None
    
    t_start: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    t_end: Optional[datetime] = None
    source_history: List[str] = field(default_factory=list)


class MemoryModule(BaseModule):
    def __init__(
        self,
        gwt,
        salience_threshold: float = 0.5,
        novelty_window_seconds: int = 60,
        boundary_threshold: float = 0.6,
    ):
        super().__init__("memory", gwt)

        self.episodes: List[Episode] = []
        self.current_episode: Optional[Episode] = None
        self.salience_threshold = salience_threshold
        self.boundary_threshold = boundary_threshold
        self.novelty_window_seconds = novelty_window_seconds
        


    def _compute_boundary_score(self, message: Message) -> float:
        if self.current_episode is None:
            return 1.0  # nothing open — always starts a new episode

        now = message.timestamp
        last_activity = self.current_episode.t_end or self.current_episode.t_start
        time_score = clamp((now - last_activity).total_seconds() / EPISODE_GAP_SECONDS)

        incoming_tags = set(self._extract_tags(message))
        episode_tags = set(self.current_episode.semantic_tags)
        if incoming_tags and episode_tags:
            overlap = len(incoming_tags & episode_tags) / len(incoming_tags | episode_tags)
            tag_score = 1.0 - overlap
        else:
            tag_score = 0.5  # no tags on either side — no strong signal, stay neutral

        prediction_error = getattr(message, "prediction_error", 0.0)

        source_score = 0.0 if message.source in self.current_episode.source_history else 0.3

        score = (
            0.35 * time_score
            + 0.30 * tag_score
            + 0.25 * prediction_error
            + 0.10 * source_score
        )
        return clamp(score)
        

    def on_broadcast(self, message: Message):
        prediction_error = getattr(message, "prediction_error", 0.0)
    # TODO omitted on purpose
        salience = self._compute_salience(...)
        if salience < self.salience_threshold:
            logger.info("Memory ignored low-salience broadcast.")
            return

        force_close = False
        if self.current_episode is not None:
            duration = (message.timestamp - self.current_episode.t_start).total_seconds()
            if duration > MAX_EPISODE_DURATION_SECONDS:
                force_close = True
                logger.warning(
                    f"Episode {self.current_episode.id} force-closed by safety "
                    f"valve after {duration:.0f}s (max={MAX_EPISODE_DURATION_SECONDS}s). "
                    f"This should be rare — investigate if it fires repeatedly."
                )

        boundary_score = self._compute_boundary_score(message)

        if self.current_episode is None or force_close or boundary_score >= self.boundary_threshold:
            if self.current_episode is not None and self.current_episode.t_end is None:
                self.current_episode.t_end = self.current_episode.t_start
            # start a new episode
            episode = Episode(
                source=message.source,
                content=[str(message.content)],
                salience=salience,
                prediction_error=prediction_error,
                semantic_tags=self._extract_tags(message),
                source_history=[message.source],
            )
            self.episodes.append(episode)
            self.current_episode = episode

            logger.info(
                f"Stored new episode {episode.id} "
                f"(salience={episode.salience:.2f})"
            )
        else:
            # extend the current episode
            ep = self.current_episode
            ep.t_end = message.timestamp
            ep.content.append(str(message.content))
            ep.source_history.append(message.source)
            ep.semantic_tags = list(set(ep.semantic_tags) | set(self._extract_tags(message)))
            ep.salience = max(ep.salience, salience)
            ep.prediction_error = max(ep.prediction_error, prediction_error)

            logger.info(
                f"Extended episode {ep.id} "
                f"(salience={ep.salience:.2f})"
            )

    # -----------------------------
    # Salience
    # -----------------------------

    def _compute_salience(
        self,
        priority: float,
        prediction_error: float,
        semantic_tags: List[str],
        source: str,
    ) -> float:

        novelty = self._compute_novelty(source, semantic_tags)

        return (
            (priority * 0.5)
            + (prediction_error * 0.3)
            + (novelty * 0.2)
        )

    def _compute_novelty(
        self,
        source: str,
        semantic_tags: List[str],
    ) -> float:

        now = datetime.now(timezone.utc)

        for episode in self.episodes:
            last_activity = episode.t_end or episode.timestamp
            age = (now - last_activity).total_seconds()

            if age > self.novelty_window_seconds:
                continue

            same_source = episode.source == source
            shared_tags = set(episode.semantic_tags) & set(semantic_tags)

            if same_source and shared_tags:
                return 0.0

        return 1.0

    def _extract_tags(self, message: Message) -> List[str]:
        tags = getattr(message, "semantic_tags", [])
        return list(tags)

    # -----------------------------
    # Retrieval
    # -----------------------------
    def _mark_retrieved(self, episodes: List[Episode]) -> List[Episode]:
        now = datetime.now(timezone.utc)

        for ep in episodes:
            ep.retrieval_count += 1
            ep.last_retrieved = now

        return episodes

    def get_by_time(
        self,
        start: datetime,
        end: datetime,
    ) -> List[Episode]:

        results = [
            episode
            for episode in self.episodes
            if start <= episode.timestamp <= end
        ]

        return self._mark_retrieved(results)

    def get_by_tags(self, tags: List[str]) -> List[Episode]:

        tag_set = set(tags)

        results = [
            episode
            for episode in self.episodes
            if tag_set.intersection(episode.semantic_tags)
        ]

        return self._mark_retrieved(results)

    def get_recent(self, n: int) -> List[Episode]:

        results = sorted(
            self.episodes,
            key=lambda ep: ep.timestamp,
            reverse=True,
        )[:n]

        return self._mark_retrieved(results)

    def get_semantic(self) -> List[Episode]:

        results = [
            episode
            for episode in self.episodes
            if episode.consolidation_state == "semantic"
        ]

        return self._mark_retrieved(results)

    # -----------------------------
    # Consolidation
    # -----------------------------

    def consolidate(self, episode_id: str):

        progression = {
            "raw": "rehearsed",
            "rehearsed": "consolidated",
            "consolidated": "semantic",
            "semantic": "semantic",
        }

        for episode in self.episodes:
            if episode.id == episode_id:
                episode.consolidation_state = progression[
                    episode.consolidation_state
                ]
                return episode

        raise ValueError(f"Episode {episode_id} not found.")