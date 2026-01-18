"""Enterprise C topic parser - Biotech Batch Processing.

Topic structure: Enterprise C/{unit}/{tag_name}

Units: chrom, sub, sum, tff
Tag naming (ISA-5.1 / ISA-88 style):
- {TAGTYPE}{UNIT}_{INSTANCE}_{SUFFIX}
- TAGTYPE: TIC, FIC, PIC, SIC, etc.
- SUFFIX: _PV, _SP, _DESC, _EU, _ACTIVE, _MODE, _STATUS, _START
"""

import re
from .base import BaseParser, TopicInfo


class EnterpriseCParser(BaseParser):
    """Parser for Enterprise C MQTT topics - biotech batch processing."""

    ENTERPRISE_PREFIX = "Enterprise C/"
    IGNORED_PREFIXES = ("maintainx/",)

    # Known process units
    UNITS = ("chrom", "sub", "sum", "tff")

    # Tag suffix patterns that indicate value types
    VALUE_SUFFIXES = {
        "_PV": "process_value",
        "_SP": "setpoint",
        "_DESC": "description",
        "_EU": "engineering_unit",
        "_ACTIVE": "active_status",
        "_MODE": "mode",
        "_STATUS": "status",
        "_START": "start_command",
        "_CMD": "command",
        "_ACK": "acknowledge",
    }

    # Tag type patterns (ISA-5.1 instrument codes)
    TAG_TYPES = {
        "TIC": "temperature_controller",
        "TI": "temperature_indicator",
        "FIC": "flow_controller",
        "FI": "flow_indicator",
        "FCV": "flow_control_valve",
        "PIC": "pressure_controller",
        "PI": "pressure_indicator",
        "SIC": "speed_controller",
        "AIC": "analyzer_controller",
        "AI": "analyzer_indicator",
        "WI": "weight_indicator",
        "HV": "hand_valve",
        "XV": "on_off_valve",
        "CI": "conductivity_indicator",
        "UV": "uv_indicator",
        "DI": "digital_input",
    }

    def parse_topic(self, topic: str) -> TopicInfo | None:
        """Parse Enterprise C topic into components."""
        if not topic.startswith(self.ENTERPRISE_PREFIX):
            return None

        if self.should_ignore(topic):
            return None

        remainder = topic[len(self.ENTERPRISE_PREFIX):]
        parts = remainder.split("/")

        info = TopicInfo(topic=topic, enterprise="C")

        if not parts:
            return info

        # First part is the unit
        info.unit = parts[0]

        if len(parts) >= 2:
            # Second part is the tag name
            tag = parts[1]
            info.tag = tag

            # Parse tag to extract category and data_type
            parsed = self._parse_tag(tag)
            if parsed:
                info.category = parsed.get("tag_type", "unknown")
                info.data_type = parsed.get("value_type", tag)
                info.equipment = parsed.get("unit_number")

        return info

    def _parse_tag(self, tag: str) -> dict | None:
        """Parse ISA-style tag name into components.

        Examples:
        - CHR01_TT001_PV -> {unit: CHR01, tag_type: TT, instance: 001, suffix: PV}
        - TIC-250-001_PV_Celsius -> {tag_type: TIC, unit: 250, instance: 001, suffix: PV, eu: Celsius}
        - UNIT-250_BATCH_ID -> {unit: UNIT-250, field: BATCH_ID}
        """
        result = {}

        # Check for known value suffixes
        for suffix, value_type in self.VALUE_SUFFIXES.items():
            if suffix in tag:
                result["value_type"] = value_type
                break

        # Try to extract tag type from beginning
        for tag_type, description in self.TAG_TYPES.items():
            if tag.startswith(tag_type) or f"_{tag_type}" in tag:
                result["tag_type"] = tag_type
                result["tag_description"] = description
                break

        # Extract unit number (digits after tag type or in format XXX-NNN)
        unit_match = re.search(r'[-_](\d{3})[-_]', tag)
        if unit_match:
            result["unit_number"] = unit_match.group(1)

        # Check for batch-related tags
        batch_keywords = ["BATCH", "RECIPE", "FORMULA", "PHASE", "STATE", "OPR", "UNIT"]
        for keyword in batch_keywords:
            if keyword in tag:
                result["tag_type"] = result.get("tag_type", "batch")
                result["batch_related"] = True
                break

        return result if result else None
