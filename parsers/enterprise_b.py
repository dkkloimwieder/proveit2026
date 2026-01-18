"""Enterprise B topic parser - beverage manufacturing."""

from .base import BaseParser, TopicInfo


CATEGORY_NAMES = ("metric", "node", "workorder", "lotnumber", "processdata", "state")


class EnterpriseBParser(BaseParser):
    """Parser for Enterprise B MQTT topics.

    Handles hierarchical manufacturing topics:
    - Enterprise-level: Node/category/data_type (4 parts)
    - Site-level: Site/node/category/data_type (5 parts)
    - Area-level: Site/area/category/data_type (6 parts)
    - Line-level: Site/area/line/category/data_type (7 parts)
    - Equipment-level: Site/area/line/equipment/category/data_type (8 parts)
    """

    ENTERPRISE_PREFIX = "Enterprise B/"
    IGNORED_PREFIXES = ("maintainx/", "abelara/", "roeslein/")

    def parse_topic(self, topic: str) -> TopicInfo | None:
        """Parse Enterprise B topic into components."""
        if not topic.startswith(self.ENTERPRISE_PREFIX):
            return None

        if self.should_ignore(topic):
            return None

        parts = topic.split("/")
        info = TopicInfo(topic=topic, enterprise="B")

        # Handle enterprise-level topics (Enterprise B/Node/... or Enterprise B/Metric/...)
        if len(parts) >= 2 and parts[1] in ("Node", "Metric"):
            info.site = None
            info.area = None
            info.category = parts[1].lower()  # 'node' or 'metric'
            if len(parts) >= 3:
                info.data_type = "/".join(parts[2:])
            return info

        if len(parts) >= 2:
            info.site = parts[1] if parts[1].startswith("Site") else None

        # Handle site-level topics (Enterprise B/Site/node/...)
        if len(parts) >= 3 and parts[2] in CATEGORY_NAMES:
            info.area = None
            info.category = parts[2]
            if len(parts) >= 4:
                info.data_type = "/".join(parts[3:])
            return info

        if len(parts) >= 3:
            info.area = parts[2]

        # Detect topic depth by checking where category appears
        if len(parts) >= 4:
            if parts[3] in CATEGORY_NAMES:
                # Area-level: parts[3] is category
                info.line = None
                info.equipment = None
                info.category = parts[3]
                if len(parts) >= 5:
                    info.data_type = "/".join(parts[4:])
            elif len(parts) >= 5 and parts[4] in CATEGORY_NAMES:
                # Line-level: parts[3] is line, parts[4] is category
                info.line = parts[3]
                info.equipment = None
                info.category = parts[4]
                if len(parts) >= 6:
                    info.data_type = "/".join(parts[5:])
            else:
                # Equipment-level: standard structure
                info.line = parts[3]
                if len(parts) >= 5:
                    info.equipment = parts[4]
                if len(parts) >= 6:
                    info.category = parts[5]
                if len(parts) >= 7:
                    info.data_type = "/".join(parts[6:])

        return info
