from icu_pipeline.source.database import AbstractDatabaseSourceSampler
from icu_pipeline.source import SourceConfig


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


from icu_pipeline.source.mimiciv.observation import MimicObservationMapper