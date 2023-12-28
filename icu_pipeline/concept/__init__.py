from abc import ABC, ABCMeta, abstractmethod
from typing import Type

from icu_pipeline.mapper.source import AbstractSourceMapper, DataSource
from icu_pipeline.mapper.schema.fhir import AbstractFHIRSinkSchema
from icu_pipeline.mapper.schema.ohdsi import AbstractOHDSISinkSchema
from icu_pipeline.mapper.sink import AbstractSinkMapper, MappingFormat
from icu_pipeline.mapper.source import SourceMapperConfiguration, DataSource


class AbstractConcept(ABC):
    FHIR_SCHEMA: Type[AbstractFHIRSinkSchema]
    OHDSI_SCHEMA: Type[AbstractOHDSISinkSchema]
    MAPPER: dict[
        DataSource, Type[AbstractSourceMapper] | list[Type[AbstractSourceMapper]]
    ]

    def __init__(
        self,
        sink_mapper: AbstractSinkMapper,
        mapping_format: MappingFormat,
        source_mapper_configs: dict[DataSource, SourceMapperConfiguration],
    ) -> None:
        super().__init__()

        self._sink_mapper = sink_mapper
        self._mapping_format = mapping_format
        self._source_mapper_configs = source_mapper_configs

    def map(self):
        for source, source_mapper in self.MAPPER.items():
            if not isinstance(source_mapper, list):
                self._map(source_mapper, source)
                continue

            source_mappers = source_mapper
            for source_mapper in source_mappers:
                self._map(source_mapper, source)

    @abstractmethod
    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: Type[AbstractSourceMapper],
    ) -> None:
        raise NotImplementedError


class AbstractSnomedConcept(AbstractConcept, metaclass=ABCMeta):
    SNOMED_ID: str

    def _map(
        self,
        source_mapper: Type[AbstractSourceMapper],
        source: Type[AbstractSourceMapper],
    ):
        mapper = source_mapper(
            self.SNOMED_ID,
            self.FHIR_SCHEMA,
            self.OHDSI_SCHEMA,
            self._source_mapper_configs[source],
            self._sink_mapper,
            self._mapping_format,
        )

        mapper.map()
