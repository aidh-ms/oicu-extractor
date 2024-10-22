from icu_pipeline.source import SourceConfig
from icu_pipeline.source.database import AbstractDatabaseSourceSampler
from icu_pipeline.source.eicu.observation import EICUObservationMapper, EICUPationObservationMapper


class EICUSampler(AbstractDatabaseSourceSampler):
    # TODO - subject IDs have an arbitrary amount of admissions..
    #   Use both, subject_id + admission_id?
    IDENTIFIER = ["patienthealthsystemstayid"]

    def __init__(self, source_config: SourceConfig) -> None:
        super().__init__(source_config)

        self.SQL_QUERY = self.build_query(
            schema="eicu_crd",
            table="patient",
        )


__all__ = [
    "EICUSampler",
    "EICUObservationMapper",
    "EICUPationObservationMapper",
]
