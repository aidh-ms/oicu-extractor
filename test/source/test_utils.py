import pandas as pd
from icu_pipeline.source.utils import (
    to_timestamp,
    offset_to_timestamp,
    offset_to_period,
)


class TestUtils:
    def test_to_timestamp(self):
        timestamp = to_timestamp("12:00", 2022, 1, 1)
        assert timestamp == pd.Timestamp("2022-01-01T12:00", tz="UTC")

    def test_offset_to_timestamp(self):
        timestamp = pd.Timestamp("2022-01-01T12:00", tz="UTC")
        offset_timestamp = offset_to_timestamp(timestamp, 60)
        assert offset_timestamp == pd.Timestamp("2022-01-01T13:00", tz="UTC")

    def test_offset_to_period(self):
        period = offset_to_period(2022, "12:00", 60, 120)
        assert period["start"] == pd.Timestamp("2022-01-01T13:00", tz="UTC")
        assert period["end"] == pd.Timestamp("2022-01-01T15:00", tz="UTC")
