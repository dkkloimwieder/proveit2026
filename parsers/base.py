"""Base classes for enterprise topic parsing."""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TopicInfo:
    """Parsed topic information - common across all enterprises."""
    topic: str
    enterprise: str | None = None
    site: str | None = None
    area: str | None = None
    line: str | None = None
    equipment: str | None = None
    category: str | None = None
    data_type: str | None = None
    # For flat tag structures (Enterprise C)
    unit: str | None = None
    tag: str | None = None


class BaseParser(ABC):
    """Abstract base class for enterprise-specific topic parsers."""

    # Topic prefixes to ignore (e.g., external vendor data)
    IGNORED_PREFIXES: tuple[str, ...] = ()

    # Enterprise prefix this parser handles
    ENTERPRISE_PREFIX: str = ""

    @abstractmethod
    def parse_topic(self, topic: str) -> TopicInfo | None:
        """Parse an MQTT topic into structured TopicInfo.

        Args:
            topic: Raw MQTT topic string

        Returns:
            TopicInfo with parsed components, or None if topic should be ignored
        """
        pass

    def should_ignore(self, topic: str) -> bool:
        """Check if topic should be ignored based on prefix."""
        # Strip enterprise prefix first
        if self.ENTERPRISE_PREFIX and topic.startswith(self.ENTERPRISE_PREFIX):
            remainder = topic[len(self.ENTERPRISE_PREFIX):]
            return any(remainder.startswith(p) for p in self.IGNORED_PREFIXES)
        return False

    @property
    def subscription_topic(self) -> str:
        """MQTT subscription topic pattern for this enterprise."""
        return f"{self.ENTERPRISE_PREFIX}#"
