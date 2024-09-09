from dataclasses import dataclass
from icu_pipeline.source import DataSource


@dataclass(frozen=True)
class Job:
    jobID: str
    database: DataSource
    subjects: list[str]
