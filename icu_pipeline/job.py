from dataclasses import dataclass

from pandera.typing import DataFrame

from icu_pipeline.source import DataSource


@dataclass(frozen=True)
class Job:
    jobID: str
    database: DataSource
    subjects: DataFrame
