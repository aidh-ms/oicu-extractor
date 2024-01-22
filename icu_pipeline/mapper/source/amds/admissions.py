import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import F, AbstractDatabaseSourceMapper
from icu_pipeline.mapper.source.utils import offset_to_period
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    Reference,
    Period,
)
from icu_pipeline.mapper.schema.fhir.encounter import FHIREncounter


class AmdsEncounterMapper(
    AbstractDatabaseSourceMapper[FHIREncounter, AbstractOHDSISinkSchema],
):
    SQL_QUERY = "SELECT * FROM amsterdamumcdb.admissions;"

    def _to_fhir(self, df: DataFrame) -> DataFrame[FHIREncounter]:
        encounter_df = pd.DataFrame()
        encounter_df[FHIREncounter.subject] = df["patientid"].map(
            lambda id: Reference(reference=str(id), type="AMDS-Patient")
        )
        encounter_df[FHIREncounter.actual_period] = df.apply(
            lambda _df: offset_to_period(
                _df["admissionyeargroup"].str.split("-", expand=True)[
                    0
                ],  # only the year group is given, extract the first year as anchor
                "00:00:00",  # no time is given, so use midnight as anchor
                _df["admittedat"],
                _df["dischargedat"],
            ),
            axis=1,
        )
