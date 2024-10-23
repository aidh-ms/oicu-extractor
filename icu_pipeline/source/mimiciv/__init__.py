from icu_pipeline.source import SourceConfig
from icu_pipeline.source.database import AbstractDatabaseSourceSampler
from icu_pipeline.source.mimiciv.dosage import MimicDosageMapper
from icu_pipeline.source.mimiciv.observation import MimicObservationMapper


class MimicSampler(AbstractDatabaseSourceSampler):
    # TODO - subject IDs have an arbitrary amount of admissions..
    #   Use both, subject_id + admission_id?
    IDENTIFIER = ["subject_id"]

    def __init__(self, source_config: SourceConfig) -> None:
        super().__init__(source_config)

        self.SQL_QUERY = self.build_query(
            schema="mimiciv_icu",
            table="icustays",
        )


__all__ = [
    "MimicSampler",
    "MimicObservationMapper",
    "MimicDosageMapper",
]
