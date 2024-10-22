from typing import Generator

import pandas as pd
from psycopg import sql
from psycopg.sql import Composable
from sqlalchemy import Connection, create_engine

from icu_pipeline.logger import ICULogger
from icu_pipeline.source import AbstractSourceSampler, SourceConfig

logger = ICULogger.get_logger()


class AbstractDatabaseSourceSampler(AbstractSourceSampler):
    """
    Abstract class for the database source mappers.

    This class is used to create a connection to a database and retrieve data from it. It inherits from the AbstractSourceMapper class and adds functionality specific to database source mappers.

    Parameters
    ----------
    source_config : SourceConfiguration
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

    def __init__(self, source_config: SourceConfig) -> None:
        self._source_config = source_config

    def create_connection(self) -> Connection:
        engine = create_engine(self._source_config.connection)
        return engine.connect().execution_options(stream_results=True)

    def build_query(
        self,
        schema: str,
        table: str,
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

        raw_query = """
            SELECT DISTINCT {fields}
            FROM {schema}.{table}
            LIMIT {limit}
        """
        
        query = sql.SQL(raw_query).format(
            fields=sql.SQL(", ").join([sql.SQL(i) for i in self.IDENTIFIER]),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            limit=(sql.Literal(limit) if (limit := self._source_config.limit) > 0 else sql.SQL("ALL")),
        )

        return query

    def get_samples(self) -> Generator[pd.DataFrame, None, None]:
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

            logger.debug(query.as_string(con.connection.cursor()))

            for df in pd.read_sql_query(
                # type: ignore[arg-type]
                query.as_string(con.connection.cursor()),
                con,
                chunksize=self._source_config.chunksize,
            ):
                yield df  # Potentially multiple columns as ID
