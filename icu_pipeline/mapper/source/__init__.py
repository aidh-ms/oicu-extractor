from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any, Generic, TypeVar, Type, Generator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import Engine, create_engine
from psycopg import sql
from psycopg.sql import Composable

from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema

F = TypeVar("F", bound=AbstractFHIRSinkSchema)


class DataSource(StrEnum):
    MIMIC = auto()
    AMDS = auto()
    EICU = auto()


@dataclass
class SourceMapperConfiguration:
    connection: str
    chunksize: int = 10000
    limit: int = -1


class AbstractSourceMapper(ABC, Generic[F]):
    def __init__(
        self,
        concept_id: str,
        concept_type: str,
        fhir_schema: Type[AbstractFHIRSinkSchema],
        source_config: SourceMapperConfiguration,
    ) -> None:
        super().__init__()

        self._concept_id = concept_id
        self._concept_type = concept_type
        self._fhir_schema = fhir_schema
        self._source_config = source_config

    def map(self) -> None:
        # FHIR is default and later transformed if necessary in a separate module
        return self.to_fihr()

    def to_fihr(self):
        for df in self.get_data():
            df = self._to_fihr(df).pipe(self._fhir_schema)
            yield df

    @abstractmethod
    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        raise NotImplementedError

    @abstractmethod
    def _to_fihr(self, df: DataFrame) -> DataFrame[F]:
        raise NotImplementedError


class AbstractDatabaseSourceMapper(AbstractSourceMapper, Generic[F], metaclass=ABCMeta):
    SQL_QUERY: str | Composable

    def create_connection(self) -> Engine:
        engine = create_engine(self._source_config.connection)
        return engine.connect().execution_options(stream_results=True)

    def build_query(
        self,
        schema: str,
        table: str,
        fields: dict[str, str],
        constraints: dict[str, Any],
    ) -> Composable:
        def build_field(exp: str, org: str) -> Composable:
            return sql.Composed(
                (sql.Identifier(org), sql.SQL(" AS "), sql.Identifier(exp))
            )

        def build_constraint(key: str, value: Any) -> Composable:
            if isinstance(value, list):
                return sql.Composed(
                    (
                        sql.Identifier(key),
                        sql.SQL(" = any({})").format(sql.Literal(value)),
                    )
                )
            return sql.Composed(
                (sql.Identifier(key), sql.SQL(" = "), sql.Literal(value))
            )

        raw_query = """
            SELECT {fields}
            FROM {schema}.{table}
            WHERE {constraints}
            LIMIT {limit}
        """

        query = sql.SQL(raw_query).format(
            fields=sql.SQL(", ").join(
                [build_field(exp, org) for exp, org in fields.items()]
            ),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            constraints=sql.SQL(" AND ").join(
                [build_constraint(key, value) for key, value in constraints.items()]
            ),
            limit=(
                sql.Literal(limit)
                if (limit := self._source_config.limit) > 0
                else sql.SQL("ALL")
            ),
        )

        return query

    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        with (
            self.create_connection() as con,
            con.begin(),
        ):
            query = self.SQL_QUERY
            if isinstance(self.SQL_QUERY, str):
                query = sql.SQL(self.SQL_QUERY)

            print(query.as_string(con.connection.cursor()))

            for df in pd.read_sql_query(
                query.as_string(con.connection.cursor()),  # type: ignore[arg-type]
                con,
                chunksize=self._source_config.chunksize,
            ):
                yield df
