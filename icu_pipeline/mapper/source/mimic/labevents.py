from abc import ABCMeta

import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.fhir import (
    Identifier,
    Subject,
    ValueQuantity,
    FHIRObservation,
)


class AbstractMimicLabEventsMapper(AbstractDatabaseSourceMapper, metaclass=ABCMeta):
    SQL_QUERY = "SELECT * FROM mimiciv_hosp.labevents WHERE itemid = any(%(values)s);"

    def _to_fihr(self, df: DataFrame) -> DataFrame:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["subject_id"].map(
            lambda id: Subject(reference=str(id))
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["charttime"], utc=True
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: ValueQuantity(
                value=float(_df["valuenum"]), unit=_df["valueuom"]
            ),
            axis=1,
        )
        observation_df[FHIRObservation.identifier] = [
            Identifier(value=self._snomed_id)
        ] * len(df)
        return observation_df

    def _to_ohdsi(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError


class MimicSerumCreatinineMapper(AbstractMimicLabEventsMapper):
    SQL_PARAMS = {"values": [50912, 52546]}
