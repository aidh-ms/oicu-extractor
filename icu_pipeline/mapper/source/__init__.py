from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import Any, Generic, TypeVar, Type, Generator

import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy import Engine
from psycopg import sql

from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema

from icu_pipeline.logger import ICULogger

# add logging
logger = ICULogger().get_logger()

FHIRSchemaType = TypeVar("FHIRSchemaType", bound=AbstractFHIRSinkSchema)
OHDSISchemaType = TypeVar("OHDSISchemaType", bound=AbstractOHDSISinkSchema)


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
    # connection: str TODO: do we need this?
    chunksize: int = 10000
    # optional limit for the number of rows to be fetched
    limit: int = -1


class AbstractSourceMapper(ABC, Generic[FHIRSchemaType, OHDSISchemaType]):
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
    def _to_fihr(self, df: DataFrame) -> DataFrame[FHIRSchemaType]:
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


class AbstractDatabaseSourceMapper(
    AbstractSourceMapper, Generic[FHIRSchemaType,
                                  OHDSISchemaType], metaclass=ABCMeta
):
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
    get_datab():
        Retrieves data from the database. This method should be implemented by subclasses.
    """
    SQL_QUERY: str  # the SQL query to be executed
    # the parameters to be used in the SQL query
    SQL_PARAMS: dict[str, Any] = dict()
    # the fields to be retrieved from the database
    SQL_FIELDS: dict[str, str] = dict()

    def create_connection(self) -> Engine:
        raise NotImplementedError

    def get_data(self) -> Generator[pd.DataFrame, None, None]:
        with (
            self.create_connection() as con,
            con.begin(),
        ):
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
            limit = self._source_config.limit  # default is -1 so no limit at all
            if limit > 0:  # add limit to the query
                query = sql.SQL(self.SQL_QUERY.replace(
                    ";", f" LIMIT {limit};"))
            else:
                query = sql.SQL(self.SQL_QUERY)
                # format the query with the fields
            query = query.format(
                **{
                    field: sql.Identifier(name)
                    for field, name in self.SQL_FIELDS.items()
                }
            )
            logger.debug(f"Executing query: {query.as_string(con)}")
            for df in pd.read_sql_query(
                query.as_string(con),  # type: ignore[arg-type]
                con,
                chunksize=self._source_config.chunksize,
                params=self.SQL_PARAMS,
            ):
                yield df
