"""Enterprise B data collector - Beverage Manufacturing.

This is a wrapper around the existing DataCollector in data_collector.py.
Kept for consistency with the collector architecture.
"""

# Import the existing collector
from data_collector import DataCollector as EnterpriseBCollector

__all__ = ["EnterpriseBCollector"]
