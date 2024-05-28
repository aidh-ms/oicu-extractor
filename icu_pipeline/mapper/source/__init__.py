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

from icu_pipeline.logger import ICULogger

# add logging
logger = ICULogger().get_logger()

F = TypeVar("F", bound=AbstractFHIRSinkSchema)


class DataSource(StrEnum):
    """
    Enum for the different data sources that can be queried.
    """

    MIMIC = auto()
    AMDS = auto()
    EICU = auto()


@dataclass
class SourceMapperConfiguration:
    """
    Configuration for the source mapper.s
    """

    connection: str
    chunksize: int = 10000
    # optional limit for the number of rows to be fetched
    limit: int = -1


class AbstractSourceMapper(ABC, Generic[F]):
    """
    Abstract class for the source mappers.

    This class is used to map data from a source to a sink. It provides a base structure for
    specific source mappers, which should implement the `map` method.

    Parameters
    ----------
    concept_id : str
        The concept ID to be used in the mapping process.
    concept_type : str
        The type of the concept to be used in the mapping process.
    fhir_schema : Type[AbstractFHIRSinkSchema]
        The FHIR schema to be used in the mapping process.
    source_config : SourceMapperConfiguration
        The configuration for the source mapper.

    Methods
    -------
    map():
        Maps the data from the source to the sink. This method should be implemented by subclasses.
    """

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
        """
        Maps the data from the source to the sink.

        For now, this method only calls the `to_fihr` method, which is the default mapping method. FHIR can be later transformed to other sink formats in a separate module.

        Returns
        -------
        None
        """
        return self.to_fihr()

    def to_fihr(self):
        """
        Converts the data to FHIR format.

        This method iterates over the data obtained from the `get_data` method, converts each piece of data to FHIR format using the `_to_fihr` method, and then applies the FHIR schema to the data.

        Yields
        ------
        DataFrame
            The data in FHIR format.
        """
        for df in self.get_data():
            df = self._to_fihr(df).pipe(self._fhir_schema)
            yield df

    @abstractmethod
    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        """
        Retrieves the data to be mapped.

        This method should be implemented by subclasses.

        Returns
        -------
        Generator
            A generator that yields dataframes with the data to be mapped.
        """
        raise NotImplementedError

    @abstractmethod
    def _to_fihr(self, df: DataFrame) -> DataFrame[AbstractFHIRSinkSchema]:
        """
        Converts a dataframe to FHIR format.

        This method should be implemented by subclasses.

        Parameters
        ----------
        df : DataFrame
            The dataframe to be converted.

        Returns
        -------
        DataFrame
            The dataframe in FHIR format.
        """
        raise NotImplementedError


class AbstractDatabaseSourceMapper(AbstractSourceMapper, Generic[F], metaclass=ABCMeta):
    """
    Abstract class for the database source mappers.

    This class is used to create a connection to a database and retrieve data from it. It inherits from the AbstractSourceMapper class and adds functionality specific to database source mappers.

    Parameters
    ----------
    concept_id : str
        The concept ID to be used in the mapping process.
    concept_type : str
        The type of the concept to be used in the mapping process.
    fhir_schema : Type[AbstractFHIRSinkSchema]
        The FHIR schema to be used in the mapping process.
    source_config : SourceMapperConfiguration
        The configuration for the source mapper.

    Methods
    -------
    create_connection():
        Establishes a connection to the database. This method should be implemented by subclasses.
    build_query(schema, table, fields, constraints):
        Builds a SQL query to retrieve data from the database.
    get_datab():
        Retrieves data from the database. This method should be implemented by subclasses.
    """

    SQL_QUERY: str | Composable  # the SQL query to be executed

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
        """
        builds a select SQL query to retrieve data from the database.

        Parameters
        ----------
        schema : str
            The schema to be queried.
        table : str
            The table to be queried.
        fields : dict
            The fields to be retrieved and their new name.
        constraints : dict
            The constraints to be applied to the query.
        """

        def _build_field(exp: str, org: str) -> Composable:
            return sql.Composed(
                (sql.Identifier(org), sql.SQL(" AS "), sql.Identifier(exp))
            )

        def _build_constraint(key: str, value: Any) -> Composable:
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
                [_build_field(exp, org) for exp, org in fields.items()]
            ),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            constraints=sql.SQL(" AND ").join(
                [_build_constraint(key, value) for key, value in constraints.items()]
            ),
            limit=(
                sql.Literal(limit)
                if (limit := self._source_config.limit) > 0
                else sql.SQL("ALL")
            ),
        )

        return query

    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        """
        Retrieves data from the database.

        This method establishes a connection to the database, constructs a SQL query based on the
        SQL_QUERY and SQL_FIELDS class attributes, and then executes the query to retrieve data.
        The data is retrieved in chunks, with the chunk size specified by the source configuration.

        Yields
        ------
        pd.DataFrame
            A DataFrame containing a chunk of the data retrieved from the database.

        Raises
        ------
        DatabaseError
            If there is a problem executing the SQL query.
        """
        with (
            self.create_connection() as con,
            con.begin(),
        ):
            query = self.SQL_QUERY
            if isinstance(self.SQL_QUERY, str):
                query = sql.SQL(self.SQL_QUERY)

            print(query.as_string(con.connection.cursor()))

            for df in pd.read_sql_query(
                # type: ignore[arg-type]
                query.as_string(con.connection.cursor()),
                con,
                chunksize=self._source_config.chunksize,
            ):
                yield df
