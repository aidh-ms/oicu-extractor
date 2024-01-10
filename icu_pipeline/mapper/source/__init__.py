from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any, Generic, TypeVar, Type, Generator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import create_engine

from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat

F = TypeVar("F", bound=AbstractFHIRSinkSchema)
O = TypeVar("O", bound=AbstractOHDSISinkSchema)


class DataSource(StrEnum):
    MIMIC = auto()
    EICU = auto()


@dataclass
class SourceMapperConfiguration:
    connection: str
    chunksize: int = 10000


class AbstractSourceMapper(ABC, Generic[F, O]):
    def __init__(
        self,
        concept_id: str,
        concept_type: str,
        fhir_schema: Type[AbstractFHIRSinkSchema],
        ohdsi_schema: Type[AbstractOHDSISinkSchema],
        source_mapper_config: SourceMapperConfiguration,
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat = MappingFormat.FHIR,
    ) -> None:
        super().__init__()

        self._concept_id = concept_id
        self._concept_type = concept_type
        self._fhir_schema = fhir_schema
        self._ohdsi_schema = ohdsi_schema
        self._source_config = source_mapper_config
        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format

    def map(self) -> None:
        match self._mapping_format:
            case MappingFormat.FHIR:
                self.to_fihr()
            case MappingFormat.OHDSI:
                self.to_ohdsi()

    def to_fihr(self):
        for df in self.get_data():
            df = self._to_fihr(df).pipe(self._fhir_schema)
            self._sink_mapper.to_output_format(df, self._fhir_schema, self._id)

    def to_ohdsi(self):
        for df in self.get_data():
            df = self._to_ohdsi(df).pipe(self._ohdsi_schema)
            self._sink_mapper.to_output_format(df, self._ohdsi_schema, self._id)

    @abstractmethod
    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        raise NotImplementedError

    @abstractmethod
    def _to_fihr(self, df: DataFrame) -> DataFrame[F]:
        raise NotImplementedError

    @abstractmethod
    def _to_ohdsi(self, df: DataFrame) -> DataFrame[O]:
        raise NotImplementedError


class AbstractDatabaseSourceMapper(
    AbstractSourceMapper, Generic[F, O], metaclass=ABCMeta
):
    SQL_QUERY: str
    SQL_PARAMS: dict[str, Any] = {}

    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        engine = create_engine(self._source_config.connection)
        with engine.connect().execution_options(
            stream_results=True
        ) as con, con.begin():
            for df in pd.read_sql_query(
                self.SQL_QUERY,
                con,
                chunksize=self._source_config.chunksize,
                params=self.SQL_PARAMS,
            ):
                yield df
