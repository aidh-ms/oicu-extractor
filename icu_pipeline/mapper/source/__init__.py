from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any, Generic, TypeVar, Type, Generator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import create_engine, Engine
from psycopg import sql

from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat

F = TypeVar("F", bound=AbstractFHIRSinkSchema)
O = TypeVar("O", bound=AbstractOHDSISinkSchema)


class DataSource(StrEnum):
    MIMIC = auto()
    AMDS = auto()
    EICU = auto()


@dataclass
class SourceMapperConfiguration:
    connection: str
    chunksize: int = 10000
    limit: int = -1


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
        # FHIR is default and later transformed if necessary
        return self.to_fihr()
        # match self._mapping_format:
        #     case MappingFormat.FHIR:
        #         return self.to_fihr()
        #     case MappingFormat.OHDSI:
        #         return self.to_ohdsi()
        #     case _:
        #         raise KeyError(f"Couldn't match MappingFormat '{self._mapping_format}'")

    def to_fihr(self):
        for df in self.get_data():
            df = self._to_fihr(df).pipe(self._fhir_schema)
            #self._sink_mapper.to_output_format(df, self._fhir_schema, self._concept_id)
            yield df

    def to_ohdsi(self):
        for df in self.get_data():
            df = self._to_ohdsi(df).pipe(self._ohdsi_schema)
            self._sink_mapper.to_output_format(df, self._ohdsi_schema, self._concept_id)

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
    SQL_PARAMS: dict[str, Any] = dict()
    SQL_FIELDS: dict[str, str] = dict()

    def create_connection(self) -> Engine:
        raise NotImplementedError

    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        with (
            self.create_connection() as con,
            con.begin(),
        ):
            limit = self._source_config.limit
            if limit > 0:
                query = sql.SQL(self.SQL_QUERY.replace(";", f" LIMIT {limit};"))
            else:
                query = sql.SQL(self.SQL_QUERY)
            query = query.format(
                    **{
                        field: sql.Identifier(name)
                        for field, name in self.SQL_FIELDS.items()
                    }
                )
            for df in pd.read_sql_query(
                query.as_string(con),  # type: ignore[arg-type]
                con,
                chunksize=self._source_config.chunksize,
                params=self.SQL_PARAMS,
            ):
                yield df
