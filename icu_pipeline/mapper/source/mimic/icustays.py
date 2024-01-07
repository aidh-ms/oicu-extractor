import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    CodeableReference,
    CodeableConcept,
    Coding,
    Reference,
    Quantity,
    Period,
)
from icu_pipeline.mapper.schema.fhir.encounter import FHIREncounter


class ICUEncounterMapper(
    AbstractDatabaseSourceMapper[FHIREncounter, AbstractOHDSISinkSchema],
):
    SQL_QUERY = "SELECT * FROM mimiciv_icu.icustays;"

    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIREncounter]:
        encounter_df = pd.DataFrame()

        encounter_df[FHIREncounter.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="MIMIC-Patient")
        )
        encounter_df[FHIREncounter.actual_period] = df.apply(
            lambda _df: Period(
                start=pd.to_datetime(_df["intime"], utc=True),
                end=pd.to_datetime(_df["outtime"], utc=True),
            ),
            axis=1,
        )
        encounter_df[FHIREncounter.care_team] = df["first_careunit"].map(
            lambda id: Reference(reference=str(id), type="CareTeam")
        )

        return encounter_df.pipe(DataFrame[FHIREncounter])

    def _to_ohdsi(self, df: DataFrame) -> DataFrame[AbstractOHDSISinkSchema]:
        raise NotImplementedError
