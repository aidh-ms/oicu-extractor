from typing import Any, Generic, TypeVar

import pandas as pd
from pandera.typing import DataFrame
from psycopg import sql
from psycopg.sql import Composable
from sqlalchemy import Connection, create_engine

from icu_pipeline.job import Job
from icu_pipeline.logger import ICULogger
from icu_pipeline.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.source import AbstractSourceMapper, DataSource, SourceConfig

logger = ICULogger.get_logger()

F = TypeVar("F", bound=AbstractFHIRSinkSchema)


class AbstractDatabaseSourceMapper(AbstractSourceMapper, Generic[F]):
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

    def __init__(
        self,
        concept_id: str,
        concept_type: str,
        fhir_schema: type[AbstractFHIRSinkSchema] | str,
        datasource: DataSource,
        source_config: SourceConfig,
        unit: str,
        **kwargs: dict[Any, Any],
    ) -> None:
        super().__init__(concept_id, concept_type, fhir_schema, datasource, source_config, unit)
        self._id_field: str | None = None
        self._query_args: dict[Any, Any] = {}

    def create_connection(self) -> Connection:
        engine = create_engine(self._source_config.connection)
        return engine.connect().execution_options(stream_results=True)

    def build_query(
        self,
        schema: str,
        table: str,
        fields: dict[str, str],
        constraints: dict[str, Any],
        ids: DataFrame,
        joins: dict[str, dict[str, str]] | None = None,
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
        ids : pd.DataFrame
            The IDs to be used in the query.
        joins : dict
            The tables to be joined and the fields to join on.
        """

        assert self._id_field is not None, f"Attribute 'self._id_field' was not set for class {type(self)}"

        def _build_field(exp: str, org: str) -> Composable:
            return sql.Composed((sql.Identifier(org), sql.SQL(" AS "), sql.Identifier(exp)))

        def _build_constraint(key: str, value: Any) -> Composable:
            if isinstance(value, str) and value.lower() == "not null":
                return sql.Composed((sql.Identifier(key), sql.SQL(" IS NOT NULL")))

            if isinstance(value, list):
                return sql.Composed(
                    (
                        sql.Identifier(key),
                        sql.SQL(" = any({})").format(sql.Literal(value)),
                    )
                )

            return sql.Composed((sql.Identifier(key), sql.SQL(" = "), sql.Literal(value)))

        def _build_join_identifier(identifier: str) -> Composable:
            return sql.SQL(".").join(sql.Identifier(t) for t in identifier.split("."))

        # There are Tables without any constraints (except sampling) -> see mimiciv_derived.age
        params = {
            "fields": sql.SQL(", ").join([_build_field(exp, org) for exp, org in fields.items()]),
            "schema": sql.Identifier(schema),
            "table": sql.Identifier(table),
            "subsetting": sql.SQL(" AND ").join(
                [
                    sql.SQL("{next_identifier} IN ({next_list})").format(
                        next_identifier=sql.SQL(i),
                        next_list=sql.SQL(",".join(ids[i].astype(str))),
                    )
                    for i in ids.columns
                ]
            ),
        }

        if len(constraints) == 0:
            raw_query = """
                SELECT {fields}
                FROM {schema}.{table}
                {joins}
                WHERE {subsetting}
            """
        else:
            raw_query = """
                SELECT {fields}
                FROM {schema}.{table}
                {joins}
                WHERE {constraints}
                AND {subsetting}
            """

            params["constraints"] = sql.SQL(" AND ").join(
                [_build_constraint(key, value) for key, value in constraints.items()]
            )

        params["joins"] = sql.SQL("")
        if joins is not None:
            params["joins"] = sql.SQL(" ").join(
                [
                    sql.SQL("INNER JOIN {table} ON {condition}").format(
                        table=_build_join_identifier(table),
                        condition=sql.SQL(" AND ").join(
                            sql.SQL("{field_a} = {field_b}").format(
                                field_a=_build_join_identifier(field_a),
                                field_b=_build_join_identifier(field_b),
                            )
                            for field_a, field_b in value.items()
                        ),
                    )
                    for table, value in joins.items()
                ]
            )

        query = sql.SQL(raw_query).format(**params)

        return query

    def get_data(self, job: Job) -> DataFrame:
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
            query = self.build_query(ids=job.subjects, **self._query_args)

            logger.debug(query.as_string(con.connection.cursor()))  # type: ignore[arg-type]

            df = pd.read_sql_query(
                # type: ignore[arg-type]
                query.as_string(con.connection.cursor()),  # type: ignore[arg-type]
                con,
                chunksize=None,
            )
            return self._to_fihr(df.pipe(DataFrame)).pipe(DataFrame)
