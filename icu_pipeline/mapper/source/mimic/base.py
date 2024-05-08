import os
from abc import ABCMeta
from typing import Union, Iterator, Generator
import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import Engine, create_engine

from icu_pipeline.mapper.source import AbstractDatabaseSourceMapper
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.schema.fhir import (
    CodeableConcept,
    Coding,
    Reference,
    Quantity,
)
from icu_pipeline.mapper.schema.fhir.observation import FHIRObservation


class AbstractMimicEventsMapper(
    AbstractDatabaseSourceMapper[FHIRObservation, AbstractOHDSISinkSchema],
    metaclass=ABCMeta,
):
    def __init__(
        self, *args: list, item_ids: str | None = None, schema: str | None, table: str | None, **kwargs: dict
    ) -> None:
        super().__init__(*args, **kwargs)

        if item_ids is None:
            raise ValueError()
        item_ids = ', '.join(map(str, item_ids))
        self.SQL_QUERY = f"SELECT * FROM {schema}.{table} WHERE itemid IN ({item_ids})"

    def create_connection(self) -> Engine:
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        MIMIC_DB = os.getenv("MIMIC_DB")
        engine = create_engine(f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{MIMIC_DB}")
        return engine.connect().execution_options(stream_results=True)

    def _to_ohdsi(self, df: DataFrame) -> DataFrame:
        # Drop this as soon as possible
        return super()._to_ohdsi(df)

    def _to_fihr(self, df: DataFrame) -> Generator[DataFrame[FHIRObservation], None, None]:
        observation_df = pd.DataFrame()

        observation_df[FHIRObservation.subject] = df["subject_id"].map(
            lambda id: Reference(reference=str(id), type="MIMIC-Patient")
        )
        observation_df[FHIRObservation.effective_date_time] = pd.to_datetime(
            df["charttime"], utc=True
        )
        observation_df[FHIRObservation.value_quantity] = df.apply(
            lambda _df: Quantity(
                value=float(_df["valuenum"]), unit=_df["valueuom"] or self.UNIT
            ),
            axis=1,
        )
        observation_df[FHIRObservation.code] = [
            CodeableConcept(
                coding=Coding(code=self._concept_id, system=self._concept_type)
            )
        ] * len(df)

        return observation_df.pipe(DataFrame[FHIRObservation])
