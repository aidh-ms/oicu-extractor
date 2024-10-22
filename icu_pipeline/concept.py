import pandas as pd
from .job import Job
from icu_pipeline.source import DataSource, SourceConfig
from icu_pipeline.source import AbstractSourceMapper, getDataSourceMapper
from icu_pipeline.unit import BaseConverter, ConverterConfig
from icu_pipeline.graph import Node
from conceptbase.config import ConceptConfig, ConceptCoding


class Concept(Node):
    """
    A class to represent a medical concept and its mapping to data sources.

    This class is responsible for loading the appropriate schema and source mappers
    based on the provided configuration. It also provides a method to map the concept
    to data from the sources.

    Parameters
    ----------
    concept_config : ConceptConfig
        The configuration for the concept, including its name, description, identifiers,
        unit, schema, and mappers.
    source_configs : dict[DataSource, SourceConfig]
        The configurations for the data sources that the concept should be mapped to.
    concept_coding : ConceptCoding
        The coding system used for the concept's identifiers.

    Attributes
    ----------
    _concept_config : ConceptConfig
        The configuration for the concept.
    _source_configs : dict[DataSource, SourceConfig]
        The configurations for the data sources.
    _concept_coding : ConceptCoding
        The coding system used for the concept's identifiers.
    _fhir_schema : Type[AbstractFHIRSinkSchema]
        The FHIR schema class for the concept.
    """

    def __init__(
        self,
        concept_config: ConceptConfig,
        source_configs: dict[DataSource, SourceConfig],
        concept_coding: ConceptCoding,
    ) -> None:
        super().__init__(concept_id=concept_config.name)
        self._concept_config = concept_config
        self._source_configs = source_configs
        self._data_sources: dict[DataSource, AbstractSourceMapper] = {}
        # Create a Source for every config
        for config in concept_config.mapper:
            if config.source not in source_configs:
                # Only use the defined sources
                continue
            mapper_type = getDataSourceMapper(config)
            mapper = mapper_type(
                concept_id=concept_config.name,
                concept_type=concept_coding,
                source_config=source_configs[config.source],
                unit=config.unit,
                **config.params,
            )
            self._data_sources[config.source] = mapper

        self._concept_coding = concept_coding

    def __str__(self) -> str:
        return f"Concept({self._node_id},{self._concept_id})"

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Concept):
            return value._concept_config.name == self._concept_config.name
        if isinstance(value, str):
            return value == self._concept_config.name
        return super().__eq__(value)

    def fetch_sources(self, job: Job, *args, **kwargs):
        # Don't do anything
        pass

    def get_data(self, job) -> dict[str, pd.DataFrame]:
        """Map the concept to data from the sources."""
        assert (
            job.database in self._data_sources
        ), f"Data Source '{job.database}' doesn't have a mapper for Concept '{self._concept_config.name}'"
        # Query the DB and return the DF
        return self._data_sources[job.database].get_data(job)

    def getDefaultConverter(self) -> BaseConverter:
        return BaseConverter.getConverter(
            config=ConverterConfig(
                concept_id=self._concept_id,
                source_units={m.source: m.unit for m in self._concept_config.mapper},
                sink_unit=self._concept_config.unit,
            )
        )
