"""Enterprise A topic parser - Glass Manufacturing.

Topic structures:
- Production: Enterprise A/Dallas/Line 1/{Area}/{Equipment}/{Category}/{DataType}
- Utilities: Enterprise A/opto22/Utilities/{Category}/{Equipment}/{Metric}
- Site-level: Enterprise A/Dallas/Site/{DataType}

Areas: BatchHouse, HotEnd, ColdEnd
Equipment: Silo01-04, BatchMixer, BatchCharger, Furnace, Forehearth, ISMachine, Lehr, Inspector, Palletizer
Categories: State, Status, Description, edge, OEE, Asset Info, Location Info
"""

from .base import BaseParser, TopicInfo


class EnterpriseAParser(BaseParser):
    """Parser for Enterprise A MQTT topics - glass manufacturing."""

    ENTERPRISE_PREFIX = "Enterprise A/"
    IGNORED_PREFIXES = ("maintainx/", "jpi/")

    # Known categories that indicate data type follows
    CATEGORIES = ("State", "Status", "Description", "edge", "OEE", "ISO7459")

    # Known areas in production line
    AREAS = ("BatchHouse", "HotEnd", "ColdEnd")

    def parse_topic(self, topic: str) -> TopicInfo | None:
        """Parse Enterprise A topic into components."""
        if not topic.startswith(self.ENTERPRISE_PREFIX):
            return None

        if self.should_ignore(topic):
            return None

        parts = topic.split("/")
        info = TopicInfo(topic=topic, enterprise="A")

        # Minimum: Enterprise A/something
        if len(parts) < 2:
            return info

        # Handle opto22/Utilities path (industrial controls)
        if len(parts) >= 3 and parts[1] == "opto22":
            return self._parse_utilities_topic(parts, info)

        # Handle Dallas site
        if parts[1] == "Dallas":
            info.site = "Dallas"

            # Site-level topics: Enterprise A/Dallas/Site/{DataType}
            if len(parts) >= 3 and parts[2] == "Site":
                info.category = "site"
                if len(parts) >= 4:
                    info.data_type = "/".join(parts[3:])
                return info

            # Organization info
            if len(parts) >= 3 and parts[2] == "Organization Info":
                info.category = "organization"
                return info

            # Line-level: Enterprise A/Dallas/Line 1/...
            if len(parts) >= 3 and parts[2].startswith("Line"):
                info.line = parts[2]
                return self._parse_line_topic(parts[3:], info)

        return info

    def _parse_line_topic(self, parts: list[str], info: TopicInfo) -> TopicInfo:
        """Parse topics under a production line."""
        if not parts:
            return info

        # Check if first part is an area
        if parts[0] in self.AREAS:
            info.area = parts[0]
            parts = parts[1:]

        if not parts:
            return info

        # Next should be equipment or a category
        if parts[0] in self.CATEGORIES:
            # Direct category under line (e.g., Line 1/OEE/...)
            info.category = parts[0]
            if len(parts) >= 2:
                info.data_type = "/".join(parts[1:])
            return info

        # Equipment level
        info.equipment = parts[0]
        parts = parts[1:]

        if not parts:
            return info

        # Category
        if parts[0] in self.CATEGORIES:
            info.category = parts[0]
            if len(parts) >= 2:
                info.data_type = "/".join(parts[1:])
        elif parts[0] in ("Asset Info", "Location Info"):
            info.category = parts[0].lower().replace(" ", "_")
            if len(parts) >= 2:
                info.data_type = "/".join(parts[1:])
        else:
            # Unknown structure - treat rest as data_type
            info.data_type = "/".join(parts)

        return info

    def _parse_utilities_topic(self, parts: list[str], info: TopicInfo) -> TopicInfo:
        """Parse opto22/Utilities topics."""
        # Enterprise A/opto22/Utilities/{Category}/{Equipment}/{Metric}
        info.area = "Utilities"

        if len(parts) < 4:
            return info

        # parts[2] is "Utilities", parts[3] is category
        utility_category = parts[3]  # Air Dryers, Compressors, Electrical Panels, etc.
        info.category = utility_category

        if len(parts) >= 5:
            info.equipment = parts[4]

        if len(parts) >= 6:
            info.data_type = "/".join(parts[5:])

        return info
