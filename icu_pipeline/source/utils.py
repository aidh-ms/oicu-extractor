import pandas as pd

from icu_pipeline.schema.fhir import Period


def to_timestamp(time: str, year: int | str, month: int | str = "01", day: int | str = "01") -> pd.Timestamp:
    """
    Convert a time string to a pd.Timestamp object.

    Converts a time string to a pd.Timestamp object with the specified year, month, and day.

    Parameters
    ----------
    time : str
        String representation of the time.
    year : int | str
        Year of the timestamp.
    month : int | str, optional
        Month of the timestamp, by default "01"
    day : int | str, optional
        Day of the timestamp, by default "01"

    Returns
    -------
    pd.Timestamp
        Timestamp object with the specified time.
    """
    return pd.Timestamp(f"{year}-{month}-{day}T{time}", tz="UTC")


def offset_to_timestamp(timestamp: pd.Timestamp, offset: int) -> pd.Timestamp:
    """
    Convert an offset to a timestamp.

    Takes an offset and adds it to a timestamp to return a new timestamp.

    Parameters
    ----------
    timestamp : pd.Timestamp
        The timestamp to add the offset to.
    offset : int
        The offset to add to the timestamp.

    Returns
    -------
    pd.Timestamp
        The timestamp, shifted by the offset.
    """
    return timestamp + pd.Timedelta(minutes=offset)


def offset_to_period(admit_year: int | str, admit_time: str, admit_offset: int, discharge_offset: int) -> Period:
    """
    Convert offsets to a period.

    Takes an admit year, admit time, admit offset, and discharge offset to return a period.

    Parameters
    ----------
    admit_year : int | str
        Year of the admission.
    admit_time : str
        Time of the admission.
    admit_offset : int
        Offset of the admission.
    discharge_offset : int
        Offset of the discharge.

    Returns
    -------
    Period
        Period object with the specified offsets.
    """
    admit_timestamp = to_timestamp(admit_time, admit_year)
    start = offset_to_timestamp(admit_timestamp, admit_offset)
    return Period(
        start=start,
        end=offset_to_timestamp(start, discharge_offset),
    )
