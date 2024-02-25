import pandas as pd

from icu_pipeline.mapper.schema.fhir import Period


def to_timestamp(
    time: str, year: int | str, month: int | str = "01", day: int | str = "01"
) -> pd.Timestamp:
    return pd.Timestamp(f"{year}-{month}-{day}T{time}", tz="UTC")


def offset_to_timestamp(timestamp: pd.Timestamp, offset: int) -> pd.Timestamp:
    return timestamp + pd.Timedelta(minutes=offset)


def offset_to_period(
    admit_year: int | str, admit_time: str, admit_offset: int, discharge_offset: int
) -> Period:
    admit_timestamp = to_timestamp(admit_time, admit_year)
    start = offset_to_timestamp(admit_timestamp, admit_offset)
    return Period(
        start=start,
        end=offset_to_timestamp(start, discharge_offset),
    )
